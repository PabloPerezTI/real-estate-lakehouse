# Diseño del modelo de datos — Madrid Real Estate Lakehouse

> Decisiones de modelado, identidad, calidad de datos y arquitectura del proyecto, tomadas tras el profiling del dataset fuente.
> Fuente: CSV de Kaggle (~21k anuncios inmobiliarios de Madrid, estilo Idealista, 57 columnas).

---

## 1. Entidades y regla de identidad

### 1.1 Propiedad frente a anuncio

El dataset no distingue entre el inmueble físico y su anuncio. Se modelan como entidades separadas:

- **Anuncio**: cada fila del CSV y cada anuncio nuevo que emite el simulador. Entidad natural de la fuente.
- **Propiedad**: el inmueble físico. Entidad derivada: no existe en la fuente; se deduce agrupando
  los anuncios que describen el mismo inmueble (§1.2).

Una propiedad puede tener N anuncios (republicaciones, obra nueva con varias unidades, venta y alquiler simultáneos).

Los anuncios de una misma propiedad **no se fusionan ni se corrigen entre sí**: cada uno conserva su propio
precio e historia, como en el portal real. Las métricas eligen el grain: por anuncio, `COUNT(*)`;
por inmueble, `COUNT(DISTINCT propiedad_key)`.

### 1.2 Regla de identidad de la propiedad (clave derivada por hash)

La fuente no tiene clave natural de la propiedad: el `id` identifica la fila, no el inmueble, y el profiling
mostró que ninguna combinación de columnas físicas es única. Por eso la identidad se **define por convención**:

```
propiedad_key := sha2( concat_ws('|',
                    n(neighborhood_id),
                    n(street_name), n(street_number),
                    n(floor),
                    n(sq_mt_built), n(n_rooms), n(n_bathrooms),
                    n(house_type_id)
                 ), 256 )

n(x) := COALESCE( lower(trim(CAST(x AS STRING))), 'NA' )
```

**Dónde se calcula: solo en silver**, a partir de los atributos físicos de cada anuncio (filas de la carga
inicial y eventos `publicacion`). Es la única implementación. El simulador no la emite: es una identidad
derivada del warehouse que el sistema fuente no conoce. Los eventos sin atributos físicos (`cambio_precio`,
`retirada`…) se enlazan con su propiedad a través de `anuncio_id`.

**Por qué cada paso de la normalización:**
- **`COALESCE(..., 'NA')`**: con `concat`, un solo NULL convierte toda la cadena en NULL y
  `sha2(NULL) = NULL`. Como `street_number` está vacío en ~71% de las filas, la mayoría de anuncios acabarían
  con clave NULL: agrupados en una única "propiedad fantasma" (GROUP BY trata los NULL como un grupo) o perdidos
  en los joins (`NULL = NULL` no es verdadero).
- **Separador `'|'` + `'NA'` en cada posición**: `concat_ws` *salta* los NULL en vez de propagarlos,
  lo que desplaza los valores y provoca colisiones entre propiedades distintas.
  Ejemplo: (calle mayor, número 3, planta NULL) y (calle mayor, número NULL, planta 3) darían ambas
  `mayor|3|...`. Con `'NA'` quedan `mayor|3|na|...` y `mayor|na|3|...`.
- **`lower(trim(...))`**: "Calle Mayor " y "calle mayor" son la misma calle.
- **`'NA'` es un token interno del cálculo de la clave. Nunca se escribe como valor en una tabla**:
  en las tablas, "desconocido" es siempre NULL (ver §5).

**Atributos excluidos del hash, y por qué:**
- `sq_mt_useful` (62% NULL y ruidoso: el mismo inmueble aparece con valores distintos).
- `raw_address` (redundante con street_name + street_number, y 25% NULL).
- Atributos mutables (`energy_certificate`, equipamiento): la identidad no puede depender de lo que cambia.

**Limitaciones asumidas:**
- Unidades de obra nueva indistinguibles (misma dirección, misma tipología, sin planta) colapsan en una sola
  propiedad con N anuncios. Caso verificado: Calle Ferenc Puskas, 50 (promoción en El Cañaveral)
  → 10 anuncios, 2 propiedades (una por tipología).
- `street_number` falta en ~71% de las filas y `street_name` en ~27%: para la mayoría de anuncios la identidad
  efectiva es barrio + calle + planta + atributos físicos. Pisos idénticos en la misma calle sin número conocido
  colapsan. La fuente no tiene información para distinguirlos.
- Dos inmuebles realmente idénticos en el mismo edificio y planta colisionarían. Riesgo aceptado.

### 1.3 Identificador del anuncio

Cada fila del CSV es un anuncio, así que su `id` es el **identificador del anuncio** (`anuncio_id`).
No sirve para identificar la propiedad: el mismo inmueble aparece con varios `id`.
En bronze el CSV conserva el nombre original `id`; silver lo renombra a `anuncio_id`.

### 1.4 Identificadores del modelo

| Identificador | Identifica | Origen | Cómo |
|---|---|---|---|
| `event_id` | Un evento concreto (algo que pasó) | El simulador, al crear el evento | UUID obtenido del generador con semilla del día simulado (§6.2) |
| `anuncio_id` | Un anuncio | El CSV; para anuncios nuevos, el simulador | El `id` del CSV tal cual (entero). Los anuncios nuevos continúan la numeración |
| `propiedad_key` | Un inmueble físico | Silver | Hash de atributos físicos (§1.2) |

Ejemplo: Calle Ferenc Puskas, 50 → 10 filas = 10 `anuncio_id` → 2 `propiedad_key`.
Si uno de esos anuncios baja dos veces de precio, genera 2 `event_id`.

---

## 2. Grains declarados

| Tabla | Grain (una fila =) | Tipo |
|---|---|---|
| `fact_publicaciones` | un anuncio publicado | Transaction |
| `fact_cambios_precio` | un cambio de precio de un anuncio en una fecha | Transaction |
| `fact_contactos` | un contacto recibido por un anuncio | Transaction |
| `fact_anuncios` (derivada) | el ciclo de vida completo de un anuncio | Accumulating snapshot |
| `dim_propiedad` | una versión de una propiedad | Dimensión SCD2 |

**Decisiones asociadas:**
- La publicación NO se duplica como primer evento en `fact_cambios_precio`: esa tabla registra
  solo cambios. Coste asumido: reconstruir la trayectoria completa de precio requiere unir publicaciones + cambios.
- `fact_anuncios` se **deriva** de los transaction facts (nunca al revés): es la capa de consumo
  y la única fact que se actualiza (su grain es el ciclo de vida, no el evento).
- `operation` (sale/rent) es una **degenerate dimension**: una columna en el fact, sin tabla propia.
- **Columna de secuenciación: `event_ts`** (UTC). Es la única columna válida para decidir qué versión es posterior:
  deduplicación con `ROW_NUMBER() ... ORDER BY event_ts DESC` y `SEQUENCE BY` en AUTO CDC.
  Nunca se ordena por columnas sin significado temporal.
- **Deduplicación de eventos: por `event_id`, en silver.** Bronze conserva los duplicados tal como llegan.

---

## 3. Dimensiones

| Dimensión | Fuente | Notas |
|---|---|---|
| `dim_propiedad` | Derivada de los anuncios (carga inicial + publicaciones nuevas) + regla de identidad | SCD mixto por atributo (§4). SCD2 vía AUTO CDC (`SEQUENCE BY event_ts`); en gold se expone con `valid_from` / `valid_to` / `is_current` como vista sobre `__START_AT` / `__END_AT` |
| `dim_fecha` | Generada | Calendario estándar |
| `dim_barrio` | **Parseada** de `neighborhood_id` | El string empaqueta: id de barrio, nombre, €/m² medio, id de distrito, nombre de distrito. Jerarquía barrio → distrito |

### Bus matrix

| Proceso de negocio | dim_propiedad | dim_fecha | dim_barrio | operation (DD) |
|---|---|---|---|---|
| Publicar anuncio | ✔ | ✔ | ✔ | ✔ |
| Cambiar precio | ✔ | ✔ | ✔ | ✔ |
| Recibir contacto | ✔ | ✔ | ✔ | ✔ |

---

## 4. Clasificación de atributos (pertenencia × mutabilidad)

| Grupo | Columnas | Pertenencia | Mutabilidad | Tratamiento |
|---|---|---|---|---|
| Estructura física | sq_mt_built, sq_mt_useful, sq_mt_allotment, n_rooms, n_bathrooms, n_floors, floor, is_floor_under, is_exterior, house_type_id, built_year, orientaciones (×4) | Propiedad | Inmutable | dim_propiedad, sin versionado |
| Localización | street_name, street_number, raw_address, neighborhood_id | Propiedad | Inmutable | dim_propiedad + dim_barrio |
| Equipamiento del edificio | has_lift, has_garden, has_pool, has_terrace, has_balcony, has_storage_room, has_green_zones, has_parking | Propiedad | Casi inmutable | dim_propiedad, SCD1 |
| Estado / instalaciones | energy_certificate, is_renewal_needed, has_ac, has_central_heating, has_individual_heating, has_fitted_wardrobes, is_accessible, is_new_development | Propiedad | **Mutable lenta** | dim_propiedad, **SCD2** |
| Oferta | operation, rent_price, buy_price, is_rent_price_known, is_buy_price_known, parking_price, is_parking_included_in_price, is_exact_address_hidden | Anuncio | Mutable (cambio = evento) | Facts / atributos del anuncio |
| Texto del anuncio | title, subtitle | Anuncio | Mutable | Atributos del anuncio |
| Derivadas | buy_price_by_area | — | — | **No se almacena** (= precio/superficie; se calcula en gold). `rent_price_by_area` también es derivada, pero está 100% vacía (grupo Muertas) |
| Identificador | id | Anuncio | — | Es el `anuncio_id` (§1.3) |
| Muertas | latitude, longitude, portal, door, rent_price_by_area, are_pets_allowed, is_furnished, is_kitchen_equipped, has_private_parking, has_public_parking | — | 100% NULL | Se conservan en bronze/silver; gold las ignora; el simulador no las emite |

Notas:
- El SCD se decide **por atributo**, no por tabla: dim_propiedad mezcla atributos estáticos y versionados.
- `is_new_development` caduca con el tiempo por naturaleza (rareza documentada, sin tratamiento especial).
- Regla de negocio: los atributos que declara el anunciante sobre el estado del inmueble
  (p. ej. is_renewal_needed) se tratan como atributos de la **propiedad** (mutables), no de la oferta.

---

## 5. Política de calidad de datos (hallazgos del profiling)

| # | Hallazgo | Evidencia | Regla en silver |
|---|---|---|---|
| 1 | Ausencia de precio codificada con **centinelas**, no con NULL | ~2.647 rent_price negativos; también valores **positivos plausibles** con is_rent_price_known=false (caso Ferenc Puskas) | `rent_price := NULL WHERE is_rent_price_known = false` (ídem buy). **La regla sigue el flag declarado por la fuente, no el signo del valor.** Los flags nunca se sobrescriben |
| 2 | 10 columnas 100% NULL con tipo inferido erróneo (INTEGER por defecto) | Profiling de completitud | Se conservan hasta silver; gold las ignora |
| 3 | `built_year` 54% NULL, `sq_mt_useful` 62% NULL | Profiling de completitud | Se conservan como NULL (sin imputación en silver) |
| 4 | Outliers de precio (problema distinto de la ausencia) | Análisis de distribución | Expectations de rango en la pipeline, con umbrales basados en percentiles |
| 5 | `neighborhood_id` multivalor empaquetado en un string | Inspección | Se parsea a dim_barrio en silver |
| 6 | Atributos físicos repetidos con precios distintos | Grupos de hasta 10 filas | No es un error: obra nueva / republicaciones. Lo absorbe el modelo propiedad-anuncio |
| 7 | `street_number` 71% NULL, `street_name` 27% NULL | Profiling de completitud | Se conservan como NULL; impacto en la identidad documentado en §1.2 |
| 8 | Filas aparentemente idénticas salvo el `id` | ids 20862 y 20620 (Calle Ferenc Puskas, 50) | Se conservan todas: cada fila es un anuncio. Dos filas idénticas salvo el `id` son dos anuncios de la misma propiedad |

Principios transversales:
- **Bronze fiel a la fuente, siempre.** Ninguna corrección antes de silver.
- **Bronze solo añade metadatos de ingesta**, con prefijo `_` (`_ingested_at`, `_source_file`). Describen el viaje
  del dato; nunca se modifica una columna de negocio.
- **Silver conserva todas las columnas** (limpias y tipadas); la selección de columnas es una decisión de gold.
- **NULL es la representación canónica de "no aplica / desconocido"** en las tablas. Nunca 0, nunca un centinela propio.
  El token `'NA'` solo existe dentro del cálculo de `propiedad_key` (§1.2) y nunca se almacena.
- **Enriquecimiento sintético solo a través del simulador**, declarado en el README. El CSV nunca se "rellena".

---

## 6. Carga inicial y simulador de eventos

**Patrón: carga inicial + cambios incrementales.**
- **Carga inicial (batch, una vez):** el CSV es la foto del portal el día 0. Sus ~21.000 anuncios existen y están
  activos al empezar. Se carga una sola vez en `bronze.csv_houses_madrid`.
- **Cambios incrementales (streaming):** un simulador genera lo que pasa **después**, a partir del día 1,
  y lo escribe como eventos en la landing zone.

El simulador hace de **sistema fuente**, como el backend de un portal inmobiliario. Corre fuera de Databricks;
su única interfaz con el resto del sistema es S3.

**Principio: el simulador imita a la fuente, no la limpia.** Los anuncios nuevos llevan los datos en bruto, con
centinelas, NULLs y flags; la limpieza es trabajo de silver (§5). Tampoco emite identidades derivadas del
warehouse (`propiedad_key`, §1.2).

### 6.1 Estructura de un evento

- **Sobre** (común a todos los tipos): `event_id`, `event_type`, `event_ts` (ISO-8601 UTC), `anuncio_id`.
- **Cuerpo** (según el tipo): ver tabla.
- Estructura plana: todos los campos en el primer nivel del JSON.

| Evento | Cuerpo | Alimenta |
|---|---|---|
| `cambio_precio` | `precio_anterior`, `precio_nuevo` | fact_cambios_precio |
| `publicacion` | Las columnas vivas del CSV: un anuncio nuevo creado a partir de una fila del CSV como plantilla | fact_publicaciones, dim_propiedad |
| `retirada` | — (solo sobre) | ciclo de vida en fact_anuncios |
| `actualizacion_propiedad` | Atributos de estado modificados (grupo SCD2 de §4) | dim_propiedad (SCD2) |
| `contacto` | — (solo sobre) | fact_contactos |

Ejemplo de evento `cambio_precio`:

```json
{"event_id":"…","event_type":"cambio_precio","event_ts":"2026-01-02T10:14:05Z","anuncio_id":20862,"precio_anterior":348000,"precio_nuevo":340000}
```

### 6.2 Funcionamiento

- **Reloj simulado.** El día 0 es la carga inicial (fecha de inicio: parámetro). El estado guarda el último día
  simulado; cada ejecución (`--days N`) simula los N días siguientes.
- **Cambios de precio.** Cada día, cada anuncio activo con precio conocido baja de precio con cierta probabilidad
  (parámetro), dentro de un rango de bajada (parámetro). Precio conocido: `buy_price` si `operation = sale` y
  `is_buy_price_known`; `rent_price` si `operation = rent` y `is_rent_price_known`.
- **Anuncios nuevos.** Unos pocos al día, copiando una fila del CSV como plantilla, con un `anuncio_id` nuevo
  (que continúa la numeración tras el mayor `id` del CSV) y el precio ajustado.
- **Aleatoriedad reproducible:** generador con semilla derivada de (semilla global, fecha simulada).
  Regenerar un día produce exactamente los mismos eventos. El `event_id` sale del mismo generador,
  no de `uuid.uuid4()`, para que un día regenerado conserve sus `event_id`.
- **Estado** (`simulator/state.json`, fuera de git): último día simulado, precio actual de cada anuncio activo
  y siguiente `anuncio_id` libre.
- **Orden de escritura: primero subir a S3, después guardar el estado.** Si el proceso muere entre ambos pasos,
  la siguiente ejecución regenera el mismo día con los mismos `event_id`: bronze recibe duplicados y silver
  los elimina por `event_id`. Es el patrón de entrega *at-least-once* + consumidor idempotente. El orden inverso
  perdería eventos sin dejar rastro.

### 6.3 Formato y ubicación

- **JSON Lines**: un evento por línea, sin indentar.
- **Ruta:** `landing/eventos/<event_type>/`. Un prefijo por tipo de evento → una tabla bronze por tipo (§7).
- **Un fichero por tipo de evento y día simulado**, con un identificador de ejecución en el nombre:
  un nombre nunca se repite.
- **La landing zone es append-only:** nunca se sobrescribe un fichero. Por defecto, Auto Loader no reprocesa
  un fichero que ya ha visto aunque cambie su contenido.
- **Serialización:** nulos como `null` y enteros como enteros. pandas convierte las columnas enteras con nulos en
  float (`3` → `3.0`) y los nulos en `NaN`, que no es JSON válido; y `json.dumps` no acepta los enteros de numpy
  (`int64`) sin convertirlos a `int`.
- **Credenciales:** perfil de la AWS CLI con un usuario IAM de mínimo privilegio (solo `s3:PutObject`
  sobre `landing/*`). Nunca claves en el código.

---

## 7. Arquitectura de almacenamiento y operación

| Elemento | Decisión |
|---|---|
| Catálogos | `re_dev` y `re_prod`: un catálogo por entorno. El entorno es una sola variable al desplegar |
| Esquemas | `bronze`, `silver`, `gold` dentro de cada catálogo |
| Tablas | **Todas managed**: Unity Catalog gobierna su ciclo de vida y su mantenimiento. Ninguna external |
| Bucket del proyecto | `real-estate-lakehouse-s3`, registrado como external location. Solo guarda ficheros: la landing zone y el estado de los streams. Las tablas no viven aquí |
| Landing zone | `s3://real-estate-lakehouse-s3/landing/eventos/<event_type>/`. La escriben productores externos a Databricks (el simulador, con boto3) |
| Tablas bronze | `bronze.csv_houses_madrid` (carga inicial, batch, una vez) y una tabla por tipo de evento: `bronze.eventos_cambio_precio`, `bronze.eventos_publicacion`… |
| Ingesta | Carga inicial: lectura batch del CSV. Eventos: Auto Loader con `trigger(availableNow=True)`, semántica de streaming (checkpoint, exactly-once, incremental) con coste de batch |
| Esquema de ingesta | Tipos fijados con schema hints o esquema declarado. La inferencia solo se usa para explorar |
| Estado de streams escritos a mano | `s3://real-estate-lakehouse-s3/_pipeline_state/<stream>/{schema,checkpoint}`, separado de la landing zone. Un checkpoint por stream, nunca compartido. Las Spark Declarative Pipelines gestionan su propio estado |
| Checkpoint y tabla destino | **Son una pareja: nunca se borra uno sin el otro.** Borrar solo el checkpoint reprocesa todo → duplicados. Borrar solo la tabla → el checkpoint da por cargados los ficheros antiguos → pérdida silenciosa de datos |
| Retención Delta | `delta.deletedFileRetentionDuration = 15 días`; `delta.logRetentionDuration` ≥ 15 días (por defecto 30). VACUUM semanal programado. Retención y frecuencia de VACUUM son independientes |

**Restricciones de Databricks Free Edition que condicionan el diseño:**
- Solo compute serverless; no se pueden modificar configuraciones de Spark (p. ej. `retentionDurationCheck`).
- Una pipeline activa por tipo → todo el medallion vive en una única pipeline.
- Máximo de 5 tareas concurrentes en jobs.
- Superar la cuota diaria apaga el compute hasta el día siguiente → nunca streams continuos; siempre `availableNow`.

---

## 8. Alternativas consideradas y descartadas

| Alternativa | Por qué se descarta |
|---|---|
| Junk dimension con los flags `has_*` | Esos flags tienen dimensión natural: describen el inmueble → `dim_propiedad`. Sacarlos rompería el SCD2 de los mutables (`has_ac`, calefacción…). Los flags del anuncio (`is_*_price_known`, `is_exact_address_hidden`, `is_parking_included_in_price`) son cuatro columnas: en almacenamiento columnar no compensa una tabla aparte |
| `dim_tipo_operacion` | Solo tendría id + valor con dos valores posibles → degenerate dimension en el fact |
| Solo `fact_anuncios` (sin transaction facts) | De un resumen no se recuperan los eventos; se perdería el histórico de cambios de precio |
| Precio desconocido como 0 | 0 parece un precio real y contamina cualquier AVG; los agregados ignoran NULL por sí solos |
| Centinelas detectados por signo (`< 0 → NULL`) | Hay centinelas positivos plausibles con `is_*_price_known = false`; la regla se basa en el flag |
| Rellenar columnas vacías con valores aleatorios | Produce combinaciones incoherentes y quita credibilidad. El enriquecimiento va por el simulador, declarado |
| Tablas external | Tras un DROP los ficheros quedan huérfanos, no hay mantenimiento automático y existen dos vías de acceso a los mismos datos |
| JSON indentado + `multiLine` | Cada fichero lo lee una sola tarea y obliga a un evento por fichero → miles de ficheros pequeños |
| Volver a publicar poco a poco los anuncios del CSV con el simulador | Reemitir datos que ya existen es artificial. El patrón real es una carga inicial de lo que existe más cambios incrementales |
| El simulador emite `propiedad_key` | Es una identidad derivada del warehouse que la fuente no conoce. Si cambiara la regla de identidad habría que regenerar todos los eventos; calculada en silver, basta con reprocesar |
| `event_id` con `uuid.uuid4()` | No es reproducible: un día regenerado tendría `event_id` nuevos y sus duplicados serían indetectables |
| Deduplicar por clave natural (`anuncio_id` + `event_type` + `event_ts`) | No distingue un reenvío de dos eventos reales idénticos; solo el productor puede distinguirlos |
| Un único prefijo y una tabla bronze para todos los eventos | Esquema común lleno de columnas vacías y evolución acoplada entre tipos de evento |
| Un bucket por capa (`bronze-…`) | Bronze, silver y gold son tablas managed y no viven en el bucket. El bucket solo guarda la landing zone y el estado de los streams |
| Guardar en `dim_propiedad` un array con sus anuncios | La relación ya está en cada anuncio (`propiedad_key`): el array sería una segunda copia que se puede desincronizar, y cada anuncio nuevo crearía una versión SCD2 del inmueble sin que el inmueble cambie. Si hace falta la lista, se obtiene con una consulta |
| Propagar un cambio de precio a todos los anuncios de la misma propiedad | Inventa eventos que no ocurrieron: el otro anuncio mantiene su precio hasta que su anunciante lo cambie. Además, una bajada real contaría como N |
