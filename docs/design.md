# Data Model Design — Madrid Real Estate Lakehouse

> Modelling, identity, data quality and architecture decisions of the project, made after profiling the source dataset.
> Source: Kaggle CSV (~21k Madrid property listings, Idealista-style, 57 columns).

---

## 1. Entities and identity rule

### 1.1 Property vs. listing

The dataset does not distinguish between the physical property and its listing. They are modelled as separate entities:

- **Listing**: each CSV row and each new listing emitted by the simulator. Natural entity of the source.
- **Property**: the physical dwelling. Derived entity: it does not exist in the source; it is inferred by grouping
  the listings that describe the same dwelling (§1.2).

A property can have N listings (re-publications, new developments with multiple units, simultaneous sale and rent).

Listings of the same property are **never merged or corrected against each other**: each one keeps its own
price and history, as on the real portal. Metrics choose the grain: per listing, `COUNT(*)`;
per property, `COUNT(DISTINCT propiedad_key)`.

### 1.2 Property identity rule (hash-derived key)

The source has no natural property key: `id` identifies the row, not the dwelling, and profiling showed that
no combination of physical columns is unique. Identity is therefore **defined by convention**:

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

**Where it is computed: in silver only**, from the physical attributes of each listing (initial load rows and
`publicacion` events). It is the single implementation. The simulator does not emit it: it is a warehouse-derived
identity that the source system does not know about. Events without physical attributes (`cambio_precio`,
`retirada`…) are linked to their property through `anuncio_id`.

**Why each normalisation step:**
- **`COALESCE(..., 'NA')`**: with `concat`, a single NULL turns the whole string into NULL and
  `sha2(NULL) = NULL`. Since `street_number` is empty in ~71% of rows, most listings would end up with a NULL key:
  grouped into a single "ghost property" (GROUP BY treats NULLs as one group) or lost in joins
  (`NULL = NULL` is not true).
- **`'|'` separator + `'NA'` in every position**: `concat_ws` *skips* NULLs instead of propagating them,
  which shifts values and causes collisions between different properties.
  Example: (calle mayor, number 3, floor NULL) and (calle mayor, number NULL, floor 3) would both produce
  `mayor|3|...`. With `'NA'` they become `mayor|3|na|...` and `mayor|na|3|...`.
- **`lower(trim(...))`**: "Calle Mayor " and "calle mayor" are the same street.
- **`'NA'` is an internal token of the key computation. It is never written as a value in any table**:
  in tables, "unknown" is always NULL (see §5).

**Attributes excluded from the hash, and why:**
- `sq_mt_useful` (62% NULL and noisy: the same dwelling appears with different values).
- `raw_address` (redundant with street_name + street_number, and 25% NULL).
- Mutable attributes (`energy_certificate`, amenities): identity cannot depend on what changes.

**Accepted limitations:**
- Indistinguishable new-development units (same address, same layout, no floor) collapse into a single
  property with N listings. Verified case: Calle Ferenc Puskas, 50 (development in El Cañaveral)
  → 10 listings, 2 properties (one per layout).
- `street_number` is missing in ~71% of rows and `street_name` in ~27%: for most listings the effective identity
  is neighbourhood + street + floor + physical attributes. Identical flats on the same street with no known number
  collapse. The source holds no information to tell them apart.
- Two genuinely identical dwellings in the same building and floor would collide. Accepted risk.

### 1.3 Listing identifier

Each CSV row is a listing, so its `id` is the **listing identifier** (`anuncio_id`).
It cannot identify the property: the same dwelling appears under several `id`s.
In bronze the CSV keeps the original name `id`; silver renames it to `anuncio_id`.

### 1.4 Model identifiers

| Identifier | Identifies | Origin | How |
|---|---|---|---|
| `event_id` | A specific event (something that happened) | The simulator, when creating the event | UUID drawn from the generator seeded with the simulated day (§6.2) |
| `anuncio_id` | A listing | The CSV; for new listings, the simulator | The CSV `id` as is (integer). New listings continue the numbering |
| `propiedad_key` | A physical dwelling | Silver | Hash of physical attributes (§1.2) |

Example: Calle Ferenc Puskas, 50 → 10 rows = 10 `anuncio_id` → 2 `propiedad_key`.
If one of those listings drops its price twice, it generates 2 `event_id`.

---

## 2. Declared grains

| Table | Grain (one row =) | Type |
|---|---|---|
| `fact_publicaciones` | one published listing | Transaction |
| `fact_cambios_precio` | one price change of a listing on a date | Transaction |
| `fact_contactos` | one contact received by a listing | Transaction |
| `fact_anuncios` (derived) | the full lifecycle of a listing | Accumulating snapshot |
| `dim_propiedad` | one version of a property | SCD2 dimension |

**Related decisions:**
- The publication is NOT duplicated as the first event in `fact_cambios_precio`: that table records
  changes only. Accepted cost: rebuilding the full price trajectory requires joining publications + changes.
- `fact_anuncios` is **derived** from the transaction facts (never the other way round): it is the consumption layer
  and the only fact table that gets updated (its grain is the lifecycle, not the event).
- `operation` (sale/rent) is a **degenerate dimension**: a column in the fact table, with no table of its own.
- **Sequencing column: `event_ts`** (UTC). It is the only valid column to decide which version is later:
  deduplication with `ROW_NUMBER() ... ORDER BY event_ts DESC` and `SEQUENCE BY` in AUTO CDC.
  Never order by columns with no temporal meaning.
- **Event deduplication: by `event_id`, in silver.** Bronze keeps duplicates exactly as they arrive.

---

## 3. Dimensions

| Dimension | Source | Notes |
|---|---|---|
| `dim_propiedad` | Derived from listings (initial load + new publications) + identity rule | Mixed SCD per attribute (§4). SCD2 via AUTO CDC (`SEQUENCE BY event_ts`); exposed in gold with `valid_from` / `valid_to` / `is_current` as a view over `__START_AT` / `__END_AT` |
| `dim_fecha` | Generated | Standard calendar |
| `dim_barrio` | **Parsed** from `neighborhood_id` | The string packs: neighbourhood id, name, average €/m², district id, district name. Hierarchy neighbourhood → district |

### Bus matrix

| Business process | dim_propiedad | dim_fecha | dim_barrio | operation (DD) |
|---|---|---|---|---|
| Publish listing | ✔ | ✔ | ✔ | ✔ |
| Change price | ✔ | ✔ | ✔ | ✔ |
| Receive contact | ✔ | ✔ | ✔ | ✔ |

---

## 4. Attribute classification (ownership × mutability)

| Group | Columns | Belongs to | Mutability | Treatment |
|---|---|---|---|---|
| Physical structure | sq_mt_built, sq_mt_useful, sq_mt_allotment, n_rooms, n_bathrooms, n_floors, floor, is_floor_under, is_exterior, house_type_id, built_year, orientations (×4) | Property | Immutable | dim_propiedad, not versioned |
| Location | street_name, street_number, raw_address, neighborhood_id | Property | Immutable | dim_propiedad + dim_barrio |
| Building amenities | has_lift, has_garden, has_pool, has_terrace, has_balcony, has_storage_room, has_green_zones, has_parking | Property | Quasi-immutable | dim_propiedad, SCD1 |
| Condition / installations | energy_certificate, is_renewal_needed, has_ac, has_central_heating, has_individual_heating, has_fitted_wardrobes, is_accessible, is_new_development | Property | **Slowly mutable** | dim_propiedad, **SCD2** |
| Offer | operation, rent_price, buy_price, is_rent_price_known, is_buy_price_known, parking_price, is_parking_included_in_price, is_exact_address_hidden | Listing | Mutable (change = event) | Facts / listing attributes |
| Listing text | title, subtitle | Listing | Mutable | Listing attributes |
| Derived | buy_price_by_area | — | — | **Not stored** (= price/area; computed in gold). `rent_price_by_area` is also derived, but it is 100% empty (Dead group) |
| Identifier | id | Listing | — | It is the `anuncio_id` (§1.3) |
| Dead | latitude, longitude, portal, door, rent_price_by_area, are_pets_allowed, is_furnished, is_kitchen_equipped, has_private_parking, has_public_parking | — | 100% NULL | Kept in bronze/silver; ignored in gold; not emitted by the simulator |

Notes:
- SCD is decided **per attribute**, not per table: dim_propiedad mixes static and versioned attributes.
- `is_new_development` expires with time by nature (documented quirk, no special treatment).
- Business rule: attributes declared by the advertiser about the condition of the dwelling
  (e.g. is_renewal_needed) are treated as attributes of the **property** (mutable), not of the offer.

---

## 5. Data quality policy (profiling findings)

| # | Finding | Evidence | Rule in silver |
|---|---|---|---|
| 1 | Missing prices encoded as **sentinel values**, not NULL | ~2,647 negative rent_price values; also **plausible positive** values with is_rent_price_known=false (Ferenc Puskas case) | `rent_price := NULL WHERE is_rent_price_known = false` (same for buy). **The rule follows the flag declared by the source, not the sign of the value.** Flags are never overwritten |
| 2 | 10 columns 100% NULL with a wrongly inferred type (INTEGER by default) | Completeness profiling | Kept up to silver; ignored in gold |
| 3 | `built_year` 54% NULL, `sq_mt_useful` 62% NULL | Completeness profiling | Kept as NULL (no imputation in silver) |
| 4 | Price outliers (a different problem from missing values) | Distribution analysis | Range expectations in the pipeline, with thresholds based on percentiles |
| 5 | Multi-valued `neighborhood_id` packed into one string | Inspection | Parsed into dim_barrio in silver |
| 6 | Repeated physical attributes with different prices | Groups of up to 10 rows | Not an error: new developments / re-publications. Absorbed by the property-listing model |
| 7 | `street_number` 71% NULL, `street_name` 27% NULL | Completeness profiling | Kept as NULL; impact on identity documented in §1.2 |
| 8 | Rows apparently identical except for `id` | ids 20862 and 20620 (Calle Ferenc Puskas, 50) | All rows are kept: each row is a listing. Two rows identical except for `id` are two listings of the same property |

Cross-cutting principles:
- **Bronze stays faithful to the source, always.** No corrections upstream of silver.
- **Bronze only adds ingestion metadata**, prefixed with `_` (`_ingested_at`, `_source_file`). It describes the
  data's journey; business columns are never modified.
- **Silver keeps every column** (cleaned and typed); column selection is a gold decision.
- **NULL is the canonical representation of "not applicable / unknown"** in tables. Never 0, never a custom sentinel.
  The `'NA'` token only exists inside the `propiedad_key` computation (§1.2) and is never stored.
- **Synthetic enrichment only through the simulator**, declared in the README. The CSV is never "filled in".

---

## 6. Initial load and event simulator

**Pattern: initial load + incremental changes.**
- **Initial load (batch, once):** the CSV is the snapshot of the portal on day 0. Its ~21,000 listings exist and
  are active at the start. It is loaded once into `bronze.csv_houses_madrid`.
- **Incremental changes (streaming):** a simulator generates what happens **afterwards**, from day 1 onwards,
  and writes it as events to the landing zone.

The simulator plays the role of the **source system**, like the backend of a property portal. It runs outside
Databricks; its only interface with the rest of the system is S3.

**Principle: the simulator imitates the source, it does not clean it.** New listings carry raw data, with
sentinels, NULLs and flags; cleaning is silver's job (§5). Nor does it emit warehouse-derived identities
(`propiedad_key`, §1.2).

### 6.1 Event structure

- **Envelope** (common to all types): `event_id`, `event_type`, `event_ts` (ISO-8601 UTC), `anuncio_id`.
- **Body** (depends on the type): see table.
- Flat structure: every field at the top level of the JSON.

| Event | Body | Feeds |
|---|---|---|
| `cambio_precio` | `precio_anterior`, `precio_nuevo` | fact_cambios_precio |
| `publicacion` | The live CSV columns: a new listing created from a CSV row used as a template | fact_publicaciones, dim_propiedad |
| `retirada` | — (envelope only) | lifecycle in fact_anuncios |
| `actualizacion_propiedad` | Modified condition attributes (SCD2 group in §4) | dim_propiedad (SCD2) |
| `contacto` | — (envelope only) | fact_contactos |

Example `cambio_precio` event:

```json
{"event_id":"…","event_type":"cambio_precio","event_ts":"2026-01-02T10:14:05Z","anuncio_id":20862,"precio_anterior":348000,"precio_nuevo":340000}
```

### 6.2 How it works

- **Simulated clock.** Day 0 is the initial load (start date: parameter). The state stores the last simulated
  day; each run (`--days N`) simulates the next N days.
- **Price changes.** Each day, every active listing with a known price drops its price with a given probability
  (parameter), within a drop range (parameter). Known price: `buy_price` if `operation = sale` and
  `is_buy_price_known`; `rent_price` if `operation = rent` and `is_rent_price_known`.
- **New listings.** A few per day, copying a CSV row as a template, with a new `anuncio_id`
  (continuing the numbering after the highest CSV `id`) and an adjusted price.
- **Reproducible randomness:** a generator seeded from (global seed, simulated date).
  Regenerating a day produces exactly the same events. `event_id` comes from the same generator,
  not from `uuid.uuid4()`, so a regenerated day keeps its `event_id`s.
- **State** (`simulator/state.json`, outside git): last simulated day, current price of every active listing
  and next free `anuncio_id`.
- **Write order: upload to S3 first, then save the state.** If the process dies between both steps, the next
  run regenerates the same day with the same `event_id`s: bronze receives duplicates and silver removes them
  by `event_id`. This is the *at-least-once* delivery + idempotent consumer pattern. The reverse order would
  lose events without leaving a trace.

### 6.3 Format and location

- **JSON Lines**: one event per line, not indented.
- **Path:** `landing/eventos/<event_type>/`. One prefix per event type → one bronze table per type (§7).
- **One file per event type and simulated day**, with a run identifier in the name: a name is never repeated.
- **The landing zone is append-only:** a file is never overwritten. By default, Auto Loader does not reprocess
  a file it has already seen, even if its content changes.
- **Serialisation:** nulls as `null` and integers as integers. pandas turns integer columns with nulls into floats
  (`3` → `3.0`) and nulls into `NaN`, which is not valid JSON; and `json.dumps` does not accept numpy integers
  (`int64`) unless converted to `int`.
- **Credentials:** AWS CLI profile with a least-privilege IAM user (only `s3:PutObject` on `landing/*`).
  Never keys in the code.

---

## 7. Storage architecture and operations

| Element | Decision |
|---|---|
| Catalogs | `re_dev` and `re_prod`: one catalog per environment. The environment is a single variable at deploy time |
| Schemas | `bronze`, `silver`, `gold` inside each catalog |
| Tables | **All managed**: Unity Catalog governs their lifecycle and maintenance. No external tables |
| Project bucket | `real-estate-lakehouse-s3`, registered as an external location. It only stores files: the landing zone and stream state. Tables do not live here |
| Landing zone | `s3://real-estate-lakehouse-s3/landing/eventos/<event_type>/`. Written by producers outside Databricks (the simulator with boto3) |
| Bronze tables | `bronze.csv_houses_madrid` (initial load, batch, once) and one table per event type: `bronze.eventos_cambio_precio`, `bronze.eventos_publicacion`… |
| Ingestion | Initial load: batch read of the CSV. Events: Auto Loader with `trigger(availableNow=True)`, streaming semantics (checkpoint, exactly-once, incremental) at batch cost |
| Ingestion schema | Types fixed with schema hints or a declared schema. Inference is only used for exploration |
| State of hand-written streams | `s3://real-estate-lakehouse-s3/_pipeline_state/<stream>/{schema,checkpoint}`, separate from the landing zone. One checkpoint per stream, never shared. Spark Declarative Pipelines manage their own state |
| Checkpoint and target table | **They are a pair: never delete one without the other.** Deleting only the checkpoint reprocesses everything → duplicates. Deleting only the table → the checkpoint considers old files already loaded → silent data loss |
| Delta retention | `delta.deletedFileRetentionDuration = 15 days`; `delta.logRetentionDuration` ≥ 15 days (default 30). Weekly scheduled VACUUM. Retention and VACUUM frequency are independent |

**Databricks Free Edition constraints that shape the design:**
- Serverless compute only; Spark configurations cannot be changed (e.g. `retentionDurationCheck`).
- One active pipeline per type → the whole medallion lives in a single pipeline.
- Maximum of 5 concurrent tasks in jobs.
- Exceeding the daily quota shuts compute down until the next day → never continuous streams; always `availableNow`.

---

## 8. Alternatives considered and rejected

| Alternative | Why it was rejected |
|---|---|
| Junk dimension with the `has_*` flags | Those flags have a natural dimension: they describe the dwelling → `dim_propiedad`. Moving them out would break SCD2 for the mutable ones (`has_ac`, heating…). The listing flags (`is_*_price_known`, `is_exact_address_hidden`, `is_parking_included_in_price`) are four columns: in columnar storage a separate table is not worth it |
| `dim_tipo_operacion` | It would only hold id + value with two possible values → degenerate dimension in the fact table |
| Only `fact_anuncios` (no transaction facts) | Events cannot be recovered from a summary; the price change history would be lost |
| Unknown price as 0 | 0 looks like a real price and pollutes any AVG; aggregates ignore NULL on their own |
| Sentinels detected by sign (`< 0 → NULL`) | There are plausible positive sentinels with `is_*_price_known = false`; the rule is based on the flag |
| Filling empty columns with random values | Produces incoherent combinations and hurts credibility. Enrichment goes through the simulator, declared |
| External tables | After a DROP the files are left orphaned, there is no automatic maintenance, and there are two access paths to the same data |
| Indented JSON + `multiLine` | Each file is read by a single task, and it forces one event per file → thousands of small files |
| Re-publishing the CSV listings gradually through the simulator | Re-emitting data that already exists is artificial. The real-world pattern is an initial load of what exists plus incremental changes |
| The simulator emits `propiedad_key` | It is a warehouse-derived identity the source does not know about. If the identity rule changed, every event would have to be regenerated; computed in silver, reprocessing is enough |
| `event_id` with `uuid.uuid4()` | Not reproducible: a regenerated day would have new `event_id`s and its duplicates would be undetectable |
| Deduplicating by natural key (`anuncio_id` + `event_type` + `event_ts`) | Cannot tell a re-delivery apart from two identical real events; only the producer can |
| A single prefix and bronze table for all events | Shared schema full of empty columns and coupled evolution across event types |
| One bucket per layer (`bronze-…`) | Bronze, silver and gold are managed tables and do not live in the bucket. The bucket only holds the landing zone and stream state |
| Storing an array of listings in `dim_propiedad` | The relationship already lives in each listing (`propiedad_key`): the array would be a second copy that can drift out of sync, and every new listing would create an SCD2 version of the property without the property changing. The list can be obtained with a query if needed |
| Propagating a price change to every listing of the same property | It invents events that never happened: the other listing keeps its price until its advertiser changes it. Also, one real price drop would count as N |
