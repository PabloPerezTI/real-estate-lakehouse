# Diseño del modelo de datos — Madrid Real Estate Pipeline

> Registra las decisiones de modelado, identidad, calidad y arquitectura del proyecto,
> tomadas tras el profiling del dataset fuente y **antes de implementar**.
> Fuente: CSV de Kaggle (~21k anuncios inmobiliarios de Madrid, estilo Idealista, 57 columnas).
> Última actualización: 9 de octubre de 2026 (ver §10).

---

## 1. Entidades y regla de identidad

### 1.1 Separación propiedad vs. anuncio

El dataset no distingue entre inmueble físico y publicación. Se modelan como entidades separadas:

- **Propiedad**: el inmueble físico. Entidad derivada (no existe en la fuente).
- **Anuncio**: cada fila del CSV / cada publicación emitida por el simulador. Entidad natural de la fuente.

Una propiedad puede tener N anuncios (republicaciones, obra nueva con múltiples unidades, venta y alquiler simultáneos).

### 1.2 Regla de identidad de propiedad (clave derivada por hash)

No existe clave natural en la fuente (verificado por profiling: el `id` es un secuencial de scraping;
ninguna combinación de columnas físicas es única). La identidad se **define por convención**:

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

**Por qué cada pieza de la normalización:**
- **`COALESCE(..., 'NA')`**: con `concat`, un solo NULL convierte toda la cadena en NULL y
  `sha2(NULL) = NULL`. Como `street_number` está vacío en ~71% de las filas, la mayoría de
  anuncios acabarían con clave NULL: agrupados en una única "propiedad fantasma" (GROUP BY
  trata los NULL como un grupo) o perdidos en los joins (`NULL = NULL` no es verdadero).
- **Separador `'|'` + `'NA'` en cada posición**: `concat_ws` *salta* los NULL en vez de propagarlos,
  lo que desplaza los valores y provoca colisiones entre propiedades distintas.
  Ejemplo: (calle mayor, número 3, planta NULL) y (calle mayor, número NULL, planta 3) darían ambas
  `mayor|3|...`. Con `'NA'` quedan `mayor|3|na|...` frente a `mayor|na|3|...`.
- **`lower(trim(...))`**: "Calle Mayor " y "calle mayor" son la misma calle.
- **`'NA'` es un token interno del cálculo de la clave. Nunca se escribe como valor en una tabla**:
  en las tablas, "desconocido" sigue siendo NULL (ver §5).

**Atributos excluidos del hash y por qué:**
- `sq_mt_useful` (62% NULL, ruidoso: mismo inmueble aparece con valores distintos).
- `raw_address` (redundante con street_name + street_number, y 25% NULL).
- Atributos mutables (`energy_certificate`, equipamiento): la identidad no puede depender de lo que cambia.

**Limitaciones asumidas:**
- Unidades de obra nueva indistinguibles (misma dirección, misma tipología, sin planta)
  colapsan en una sola propiedad con N anuncios. Caso verificado: Calle Ferenc Puskas, 50
  (promoción en El Cañaveral) → 10 anuncios, 2 propiedades (una por tipología).
- `street_number` falta en ~71% de las filas y `street_name` en ~27%: para la mayoría de anuncios
  la identidad efectiva es barrio + calle + planta + atributos físicos. Pisos idénticos en la misma
  calle sin número conocido colapsan. No hay información en la fuente para distinguirlos.
- Dos inmuebles genuinamente clónicos en el mismo edificio y planta colisionarían. Riesgo aceptado.

### 1.3 Linaje

El `id` de la fuente **se conserva** en bronze y silver como identificador de trazabilidad
(fila original del CSV), nunca como clave de negocio.

---

## 2. Grains declarados

| Tabla | Grain (una fila =) | Tipo |
|---|---|---|
| `fact_publicaciones` | un anuncio publicado | Transaction |
| `fact_cambios_precio` | un cambio de precio de un anuncio en una fecha | Transaction |
| `fact_contactos` | un contacto recibido por un anuncio | Transaction |
| `fact_anuncios` (gold, derivada) | el ciclo de vida completo de un anuncio | Accumulating snapshot |
| `dim_propiedad` | una versión de una propiedad | Dimensión SCD2 |

**Decisiones asociadas:**
- La publicación NO se duplica como primer evento en `fact_cambios_precio`: esa tabla registra
  solo cambios. Coste asumido: reconstruir la trayectoria completa de precio requiere unir
  publicaciones + cambios.
- `fact_anuncios` se **deriva** de los transaction facts (nunca al revés): es la capa de consumo,
  la única fact que se actualiza (su grain es el ciclo de vida, no el evento).
- `operation` (sale/rent) es una **degenerate dimension**: columna en el fact, sin tabla propia.
- **Columna de secuenciación: `event_ts`** (UTC). Es la única columna válida para decidir qué versión
  es posterior: deduplicación con `ROW_NUMBER() ... ORDER BY event_ts DESC` y `SEQUENCE BY` en AUTO CDC.
  Nunca se ordena por columnas sin significado temporal.

---

## 3. Dimensiones

| Dimensión | Fuente | Notas |
|---|---|---|
| `dim_propiedad` | Derivada del CSV + regla de identidad | SCD mixto por atributo (§4). SCD2 vía AUTO CDC (`SEQUENCE BY event_ts`); en gold se expone con `valid_from` / `valid_to` / `is_current` como vista sobre `__START_AT` / `__END_AT` |
| `dim_fecha` | Generada | Calendario estándar |
| `dim_barrio` | **Parseada** de `neighborhood_id` | El string empaqueta: id barrio, nombre, €/m² medio, id distrito, nombre distrito. Jerarquía barrio → distrito |

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
| Equipamiento edificio | has_lift, has_garden, has_pool, has_terrace, has_balcony, has_storage_room, has_green_zones, has_parking | Propiedad | Cuasi-inmutable | dim_propiedad, SCD1 |
| Estado / instalaciones | energy_certificate, is_renewal_needed, has_ac, has_central_heating, has_individual_heating, has_fitted_wardrobes, is_accessible, is_new_development | Propiedad | **Mutable lenta** | dim_propiedad, **SCD2** |
| Oferta | operation, rent_price, buy_price, is_rent_price_known, is_buy_price_known, parking_price, is_parking_included_in_price, is_exact_address_hidden | Anuncio | Mutable (cambio = evento) | Facts / atributos de anuncio |
| Texto del anuncio | title, subtitle | Anuncio | Mutable | Atributos de anuncio |
| Derivadas | buy_price_by_area | — | — | **No se almacena** (= precio/superficie; se calcula en gold). `rent_price_by_area` también es derivada, pero está 100% vacía (grupo Muertas) |
| Linaje técnico | id | Técnica | — | Se conserva (trazabilidad) |
| Muertas | latitude, longitude, portal, door, rent_price_by_area, are_pets_allowed, is_furnished, is_kitchen_equipped, has_private_parking, has_public_parking | — | 100% NULL | Bronze/silver las conservan; gold las ignora; el simulador no las emite |

Notas:
- El SCD es una decisión **por atributo**, no por tabla: dim_propiedad mezcla atributos estáticos y versionados.
- `is_new_development` caduca con el tiempo por naturaleza (rareza documentada, sin tratamiento especial).
- Regla de negocio elegida: los atributos declarados por el anunciante sobre el estado del inmueble
  (p. ej. is_renewal_needed) se tratan como atributos de la **propiedad** (mutables), no de la oferta.

---

## 5. Política de calidad de datos (hallazgos del profiling)

| # | Hallazgo | Evidencia | Regla en silver |
|---|---|---|---|
| 1 | Ausencia de precio codificada con **centinelas**, no NULL | ~2.647 rent_price negativos; también valores **positivos plausibles** con is_rent_price_known=false (caso Ferenc Puskas) | `rent_price := NULL WHERE is_rent_price_known = false` (ídem buy). **La regla se deriva del flag declarado por la fuente, no del signo del valor.** Los flags nunca se sobrescriben |
| 2 | 10 columnas 100% NULL con tipo inferido erróneo (INTEGER por defecto) | Profiling de completitud | Se conservan hasta silver; gold las ignora |
| 3 | `built_year` 54% NULL, `sq_mt_useful` 62% NULL | Profiling de completitud | Se conservan como NULL (sin imputación en silver) |
| 4 | Outliers de precio (problema distinto a la ausencia) | Pendiente de cuantificar | Expectations de rango en SDP, umbrales por percentiles |
| 5 | `neighborhood_id` multivalor empaquetado en string | Inspección | Parseo a dim_barrio en silver |
| 6 | Duplicados exactos de atributos físicos | Grupos de hasta 10 filas | No son error: obra nueva / republicaciones. Los absorbe el modelo propiedad-anuncio |
| 7 | `street_number` 71% NULL, `street_name` 27% NULL | Profiling de completitud | Se conservan como NULL; impacto en la identidad documentado en §1.2 |

Principios transversales:
- **Bronze fiel a la fuente, siempre.** Ninguna corrección aguas arriba de silver.
- **Silver conserva todas las columnas** (limpias y tipadas); la selección es decisión de gold.
- **NULL es la representación canónica de "no aplica / desconocido"** en las tablas. Nunca 0, nunca centinela propio.
  El token `'NA'` solo existe dentro del cálculo de `propiedad_key` (§1.2) y nunca se almacena.
- **Enriquecimiento sintético solo vía simulador**, declarado en README. El CSV nunca se "rellena".

---

## 6. Simulador de eventos

El CSV es el **catálogo semilla** (universo de propiedades/anuncios). La dimensión temporal
la aporta un generador Python que emite eventos a la landing zone:

| Evento | Payload mínimo | Alimenta |
|---|---|---|
| `publicacion` | anuncio, atributos, precio inicial, ts | fact_publicaciones |
| `cambio_precio` | anuncio, precio nuevo/anterior, ts | fact_cambios_precio |
| `contacto` | anuncio, ts | fact_contactos |
| `retirada` | anuncio, ts | ciclo de vida en fact_anuncios |
| `actualizacion_propiedad` | propiedad, atributo SCD2 modificado, ts | dim_propiedad (SCD2) |

**Formato:**
- **JSON Lines (JSONL)**: un evento por línea, sin indentar. Spark puede repartir la lectura de un
  fichero entre varias tareas; con JSON indentado hace falta `multiLine=true` y cada fichero lo lee
  una sola tarea.
- **Un fichero por ejecución del simulador**, con todos los eventos del lote (uno por línea).
  Evita miles de ficheros diminutos en la landing zone.
- **Campos comunes a todos los eventos**: `event_type`, `event_ts` (ISO-8601 UTC), `anuncio_id`,
  `propiedad_key`. El resto depende del tipo de evento.

Ejemplo de línea:

```json
{"event_type":"publicacion","event_ts":"2026-08-27T10:15:30Z","anuncio_id":"ANUN-MAD-2026-00124","propiedad_key":"4f9a3e81b2c7019d53c8e76a120419b8","price":345000.0,"operation":"sale","sq_mt_built":85,"n_rooms":3,"n_bathrooms":2}
```

**Reglas:**
- Cada evento lleva la `propiedad_key` por construcción (el simulador conoce el enlace anuncio→propiedad).
  El simulador y silver calculan la clave con **la misma función** del paquete Python del proyecto,
  para que nunca diverjan.
- Distribuciones controladas (frecuencia de bajadas, contactos por anuncio) — documentadas en el repo.
- Los eventos de contacto y retirada son 100% sintéticos (no existen en la fuente), declarado.

---

## 7. Arquitectura de almacenamiento y operación

| Elemento | Decisión |
|---|---|
| Catálogos | `re_dev` y `re_prod`: un catálogo por entorno. El entorno es una sola variable al desplegar |
| Esquemas | `bronze`, `silver`, `gold` dentro de cada catálogo |
| Tablas | **Todas managed**: UC gobierna su ciclo de vida y su mantenimiento. Ninguna external |
| Landing zone | S3 vía external location, prefijo `landing/eventos/`. La escriben productores externos a Databricks (simulador con boto3; en 2027, el consumidor de Kafka) |
| Ingesta | Auto Loader con `trigger(availableNow=True)`: semántica de streaming (checkpoint, exactly-once, incremental) con coste de batch |
| Esquema de ingesta | Tipos fijados con schema hints o esquema declarado. La inferencia solo se usa para explorar |
| Estado de streams escritos a mano | `s3://<bucket>/_pipeline_state/<stream>/{schema,checkpoint}`, separado de la landing. Un checkpoint por stream, nunca compartido. Deja de aplicar al pasar a SDP, que gestiona su propio estado |
| Retención Delta | `delta.deletedFileRetentionDuration = 15 días`; `delta.logRetentionDuration` ≥ 15 días (por defecto 30). VACUUM semanal programado. Retención y frecuencia de VACUUM son independientes |

**Bucket provisional:** la landing usa de momento el bucket `raw-weather-app`. Se sustituirá por un bucket
propio del proyecto con su external location. El checkpoint de Auto Loader guarda las rutas de origen, así que
cambiar de bucket implica un stream nuevo: bronze conserva lo ya ingerido y el stream nuevo arranca con
`cloudFiles.includeExistingFiles = false`, o se reingesta y la deduplicación de silver lo absorbe.

**Restricciones de Databricks Free Edition que condicionan el diseño:**
- Solo compute serverless; no se pueden modificar configuraciones de Spark (p. ej. `retentionDurationCheck`).
- Una pipeline activa por tipo → todo el medallion vive en una única pipeline SDP.
- Máximo 5 tareas concurrentes en jobs.
- Superar la cuota diaria apaga el compute hasta el día siguiente → nunca streams continuos; siempre `availableNow`.

---

## 8. Alternativas consideradas y descartadas

| Alternativa | Por qué se descarta |
|---|---|
| Junk dimension con los flags `has_*` | Esos flags tienen dimensión natural: describen el inmueble → `dim_propiedad`. Sacarlos rompería el SCD2 de los mutables (`has_ac`, calefacción…). Los flags del anuncio (`is_*_price_known`, `is_exact_address_hidden`, `is_parking_included_in_price`) son cuatro columnas: en almacenamiento columnar no compensa una tabla aparte |
| `dim_tipo_operacion` | Solo tendría id + valor con dos valores posibles → degenerate dimension en el fact |
| Solo `fact_anuncios` (sin transaction facts) | Del resumen no se recuperan los eventos; se perdería el histórico de cambios de precio |
| Precio desconocido como 0 | 0 parece un precio real y contamina cualquier AVG; los agregados ignoran NULL solos |
| Centinelas detectados por signo (`< 0 → NULL`) | Hay centinelas positivos plausibles con `is_*_price_known = false`; la regla se basa en el flag |
| Rellenar columnas vacías con valores aleatorios | Produce combinaciones incoherentes y quita credibilidad. El enriquecimiento va por el simulador, declarado |
| Tablas external | Tras un DROP los ficheros quedan huérfanos, no hay mantenimiento automático y existen dos vías de acceso a los mismos datos |
| JSON indentado + `multiLine` | Cada fichero lo lee una sola tarea y fuerza un evento por fichero → miles de ficheros pequeños |

---

## 9. Decisiones pendientes (se abordarán en su bloque)

- Umbrales de outliers de precio (Bloque 4, expectations).
- Organización interna de silver (Bloque 4, diseño de la pipeline SDP).
- Monitorización de `_rescued_data` como expectation (Bloque 4).
- ¿`event_id` único por evento? Permitiría descartar reenvíos exactos del mismo evento (al construir el simulador).
- Bucket propio del proyecto y su external location.
- Resto de naming de columnas gold (Bloque 5).

---

## 10. Historial de cambios

- **v1** — Cierre del Bloque 0: identidad, grains, dimensiones, clasificación de atributos, calidad y diseño del simulador.
- **v2 (9 oct 2026)** — Detalle del cálculo de `propiedad_key` y limitación por `street_number` (§1.2);
  columna de secuenciación (§2); naming SCD2 de `dim_propiedad` (§3); corrección de `rent_price_by_area`,
  que aparecía en dos grupos (§4); hallazgo 7 y aclaración NULL vs. `'NA'` (§5); formato JSONL y campos
  comunes del simulador (§6); arquitectura de almacenamiento y operación (§7); alternativas descartadas (§8).
