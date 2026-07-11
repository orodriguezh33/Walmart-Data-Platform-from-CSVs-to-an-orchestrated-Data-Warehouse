# Conceptos técnicos a profundidad

*[English version](CONCEPTS.md)*

Este documento explica, con mecánica y ejemplos concretos, el "por qué" detrás de cada decisión de diseño del proyecto. El [README](../README.es.md) las menciona en una línea; acá está el detalle completo — qué problema resuelve cada patrón, qué pasaría sin él, y cómo funciona exactamente en este pipeline.

## Índice

- [Arquitectura medallion (bronze → silver → gold)](#arquitectura-medallion-bronze--silver--gold)
- [Base "ghost" como base de datos agentic](#base-ghost-como-base-de-datos-agentic)
- [CDC en vez de extract-and-replace](#cdc-en-vez-de-extract-and-replace)
- [Modelos incrementales en `silver_t`](#modelos-incrementales-en-silver_t)
- [OBT metadata-driven con Jinja (`silver_b`)](#obt-metadata-driven-con-jinja-silver_b)
- [Materialización `ephemeral` (gold)](#materialización-ephemeral-gold)
- [Snapshots de dbt para dimensiones (SCD2)](#snapshots-de-dbt-para-dimensiones-scd2)
- [Tests después de cada capa, no solo al final](#tests-después-de-cada-capa-no-solo-al-final)
- [CeleryExecutor de Airflow](#celeryexecutor-de-airflow)
- [Secretos fuera del código](#secretos-fuera-del-código)
- [S3 aislado vs. CDC del flujo principal](#s3-aislado-vs-cdc-del-flujo-principal)

## Arquitectura medallion (bronze → silver → gold)

**El problema que resuelve:** si transformas los datos crudos directamente en el modelo final de negocio en un solo paso, cualquier error de calidad de datos, cualquier cambio de requerimiento, o cualquier debugging se vuelve una pesadilla — no hay forma de aislar en qué punto del proceso se rompió algo, ni de reprocesar solo una parte.

**La idea central:** separar el pipeline en capas con una responsabilidad clara cada una:

- **Bronze** (`sources.yml` → `bronze.orders`, etc.): los datos tal cual llegan de la fuente, sin ninguna transformación. Es la "verdad cruda" — si algo se corrompe más adelante, siempre puedes reconstruir desde acá.
- **Silver** (`silver_t` → `silver_b`): datos limpios, tipados, deduplicados y unidos entre sí. Todavía no están modelados para un caso de negocio específico, pero ya son confiables.
- **Gold** (`gold/ephemeral`, `snapshots/dim_*`, `gold/fact`): datos modelados específicamente para consumo analítico — dimensiones, hechos, STAR schema. Lo que consulta un dashboard de BI.

**Por qué importa en la práctica:** si `dim_products` tiene un dato raro, puedes preguntarte "¿el problema está en `bronze.products` (llegó mal desde la fuente), en `silver_t.products_t` (se rompió la limpieza), en `obt_b` (se rompió el join), o en el snapshot mismo?" — cada capa es un punto de verificación independiente. Además, si necesitas rehacer solo la capa `gold` (por ejemplo, cambiaste la lógica del STAR schema), no tienes que re-ingerir todo desde la fuente — `silver` ya está ahí, listo.

## Base "ghost" como base de datos agentic

**El contraste:** una base Postgres "tradicional" la administras tú — corres `CREATE DATABASE`, `CREATE TABLE`, gestionas migraciones a mano o con una herramienta como Alembic/Flyway, y si quieres una copia para probar algo, normalmente clonas el volumen de disco o haces un `pg_dump`/`pg_restore` completo (lento, y consume espacio duplicado).

**Lo que es una base "agentic" (Ghost, `db.ghost.build`):** en vez de correr los comandos DDL a mano, le das a un agente el archivo `walmart_dataset/ddl/walmart_schema.sql` más una instrucción en lenguaje natural, y el agente interpreta el DDL, provisiona la base, crea el schema y las tablas. No hay un `psql` corriendo `CREATE TABLE` línea por línea manualmente — el agente hizo esa traducción.

**La ventaja concreta: forking de base de datos.** Esto es el punto más interesante. En Postgres tradicional, "probar algo sin arriesgar la base real" significa:
- Levantar una instancia nueva desde cero y volver a cargar todos los datos, o
- Hacer un `pg_dump` completo y restaurarlo en otra instancia (lento con datasets grandes, y duplica el almacenamiento).

Con **forking** en una base agentic como Ghost, puedes crear una copia de la base completa de forma casi instantánea — conceptualmente como un `git branch`, pero para datos: el fork comparte el estado hasta el momento de la bifurcación, y de ahí en adelante los cambios en el fork no afectan a la instancia original (ni viceversa). Esto habilita cosas como: probar una migración de schema arriesgada, correr un seed de datos de prueba, o experimentar con una columna nueva — todo en el fork, y si algo sale mal, simplemente lo descartas sin haber tocado la base que alimenta el pipeline real de CDC.

## CDC en vez de extract-and-replace

**El problema que resuelve:** ¿cómo traes datos desde un sistema operacional (Postgres "ghost") hacia el warehouse (Databricks) sin (a) sobrecargar la fuente y (b) mover más datos de los necesarios en cada corrida?

**La alternativa ingenua — extract-and-replace:** cada corrida, conectas a Postgres y haces `SELECT * FROM orders` (la tabla completa) para sobrescribir `bronze.orders` en Databricks. Con una tabla de millones de filas, esto significa:
- Un scan completo de la tabla en la fuente cada vez — si "ghost" fuera una base transaccional real sirviendo tráfico de producción (un POS, un ERP), este scan competiría por recursos (CPU, I/O, locks) con las operaciones reales del negocio.
- Transferir por la red el 100% de los datos, aunque solo haya cambiado el 0.01%.

**Cómo funciona CDC (Change Data Capture) en cambio:** Postgres ya mantiene internamente un **write-ahead log (WAL)** — un registro secuencial de cada `INSERT`, `UPDATE` y `DELETE` que ocurre en la base, que originalmente existe para garantizar durabilidad/replicación. Un mecanismo de CDC lee ese WAL (en vez de volver a consultar las tablas) y extrae exactamente qué filas cambiaron y cómo, desde el último punto en que se sincronizó. Solo esas filas viajan hacia `bronze`.

**La analogía:** extract-and-replace es fotocopiar el libro entero cada vez que alguien anota algo al margen. CDC es leer el registro de "qué páginas se modificaron" y solo reimprimir esas páginas.

**Dónde vive esto en el proyecto:** el job de CDC (`postgres_to_bronze`) corre como un job independiente en Databricks — no hay código de detección de cambios en este repo, porque el mecanismo está configurado del lado de Databricks (Lakeflow Connect / su conector de ingestión CDC para Postgres). Lo que sí ves en `orchestrate.py` es la tarea `ingest_cdc`, que dispara ese job vía `WorkspaceClient` y espera (polling cada 5s) a que termine antes de dejar avanzar el DAG.

## Modelos incrementales en `silver_t`

**El problema:** una vez que los cambios ya llegaron a `bronze` vía CDC, ¿cómo construyes `silver_t.orders_t` (con su columna extra `processed_at`) sin reconstruir la tabla completa cada vez?

**Full refresh (la alternativa):** en cada corrida, `TRUNCATE silver_t.orders_t` y reconstrúyela completa desde `bronze.orders`. Si `bronze.orders` tiene 5 millones de filas y solo 200 cambiaron hoy, procesas 5 millones para actualizar 200 — el costo de cómputo crece con el tamaño *total* de la tabla, no con lo que realmente cambió.

**Lo que hace un modelo incremental de dbt (`materialized: incremental`):**

1. El SQL del modelo tiene un bloque `{% if is_incremental() %}` — ese bloque de Jinja solo se activa si la tabla destino (`silver_t.orders_t`) **ya existe**. En la primera corrida no existe, así que dbt hace un build completo (`CREATE TABLE AS SELECT ...`); de ahí en adelante, toma la rama incremental.
2. Dentro de ese bloque hay un filtro del tipo `WHERE updated_timestamp > (SELECT MAX(updated_timestamp) FROM {{ this }})` — traduce a "dame solo las filas de `bronze.orders` que son más nuevas que la fila más reciente que ya tengo procesada".
3. Con `unique_key = 'order_id'` configurado, dbt no hace un `INSERT` simple (que duplicaría filas si una orden que ya existía cambió) — genera un `MERGE` (o el equivalente en Databricks: `MERGE INTO ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`). Así, si la orden `O456` cambió de estado, se actualiza la fila existente en lugar de crear un duplicado.

**Resultado:** el costo de cada corrida es proporcional a *lo que cambió desde ayer*, no al tamaño acumulado del histórico. Es lo que permite correr esto todos los días sin que el tiempo de ejecución crezca sin control a medida que pasan los meses y la tabla acumula años de datos.

**Relación con CDC:** son el mismo principio aplicado en capas distintas del pipeline — CDC resuelve la eficiencia de *bronze* (Postgres → Databricks), los modelos incrementales resuelven la eficiencia de *silver* (bronze → silver_t dentro de Databricks).

## OBT metadata-driven con Jinja (`silver_b`)

**El problema:** `obt_b.sql` necesita unir (LEFT JOIN) las 6 tablas de `silver_t` (orders, customers, products, order_items, employees, stores) en un solo modelo ancho. Escrito a mano, serían 6 bloques de JOIN casi idénticos, cada uno con su propio `SELECT` de columnas, su alias, y su condición de join — mucho código repetido, y agregar una séptima fuente significaría copiar/pegar otro bloque completo.

**El patrón metadata-driven:** en vez de escribir el SQL de cada JOIN a mano, defines una lista de configuración en Jinja:

```jinja
{% set configs = [
    {'table': 'orders_t', 'alias': 'o', 'join_condition': '...'},
    {'table': 'customers_t', 'alias': 'c', 'join_condition': '...'},
    ...
] %}
```

Y un loop de Jinja (`{% for config in configs %}`) genera el SQL del JOIN para cada entrada de esa lista, en tiempo de **compilación** (antes de que el SQL llegue a Databricks — dbt renderiza el Jinja a SQL plano, y eso es lo que efectivamente se ejecuta).

**Por qué importa:** agregar una séptima fuente (digamos, `suppliers_t`) significa agregar una entrada más a la lista `configs` — no escribir un bloque de SQL nuevo. Es el mismo principio de "no te repitas" (DRY) que aplicarías en código de aplicación, pero llevado a SQL vía la capa de templating de dbt.

## Materialización `ephemeral` (gold)

**El problema:** quieres modularizar `models/gold/ephemeral/eph_*.sql` — un archivo por entidad (`eph_products`, `eph_customers`, etc.), cada uno un `SELECT` simple que recorta columnas de `obt_b` para una entidad específica. Pero estos modelos **no necesitas consultarlos directamente** — solo existen para alimentar el siguiente paso (los snapshots de SCD2). Si los materializas como `table` o `view`, cada uno ocupa espacio/cómputo en Databricks para algo que nadie consulta standalone.

**Lo que hace `materialized: ephemeral`:** dbt **no crea ninguna tabla ni vista física** en el warehouse para estos modelos. En cambio, en tiempo de compilación, cuando otro modelo hace `{{ ref('eph_products') }}`, dbt **inyecta el SQL de `eph_products` directamente como un CTE (`WITH ... AS (...)`)** dentro del SQL del modelo que lo referencia. El modelo ephemeral nunca se ejecuta de forma independiente — literalmente no existe como objeto en Databricks, solo como texto SQL insertado en otra query.

**Por qué importa:** obtienes la organización de "un archivo, una responsabilidad, por entidad" (más fácil de leer y mantener que un único archivo gigante) **sin pagar el costo de storage/compute** de materializar un paso intermedio que solo existe para alimentar al siguiente. Es modularidad de código, gratis en tiempo de ejecución.

**Cómo se ve en el DAG:** por esto la tarea `gold_ephemeral` (`dbt run --select gold.ephemeral`) en `orchestrate.py` **resuelve** los 5 nodos `eph_*` pero **ejecuta 0** — comportamiento esperado, no un bug. dbt valida que existan y estén bien formados, pero no hay nada que correr contra el warehouse porque no generan artefactos propios; solo se materializan (como CTE) cuando el snapshot que los referencia corre.

## Snapshots de dbt para dimensiones (SCD2)

**El problema:** el producto `P123` tenía `category = 'Electrónica'` cuando se vendió en enero. El 15 de marzo, alguien lo recategoriza a `'Accesorios'`. Si `dim_products` solo guarda el **estado actual** (un `UPDATE` normal que sobrescribe la fila), entonces cuando hoy analices las ventas de enero, ese producto va a aparecer como `'Accesorios'` — aunque en enero, cuando se vendió, era `'Electrónica'`. Reescribiste el pasado, y tu reporte histórico queda mal.

**Lo que hace un dbt snapshot con `strategy: timestamp`:** en vez de `UPDATE` sobre la fila existente cuando detecta un cambio, hace lo siguiente:

1. Compara la fila actual de la fuente (`eph_products`) contra la última versión que tiene guardada en `dim_products`.
2. Si algo cambió, **no sobrescribe** — en su lugar:
   - Cierra la versión vieja: le asigna `dbt_valid_to = 2026-03-15` (la fecha en que detectó el cambio).
   - Inserta una **fila nueva**: `dbt_valid_from = 2026-03-15`, `dbt_valid_to = NULL` (NULL = "esta es la versión vigente ahora mismo").
3. Si nada cambió, no hace nada — esa fila sigue con `dbt_valid_to = NULL`.

`dim_products` termina con **dos filas** para `P123`:

| product_id | category     | dbt_valid_from | dbt_valid_to |
|------------|--------------|-----------------|---------------|
| P123       | Electrónica  | 2025-01-01      | 2026-03-15    |
| P123       | Accesorios   | 2026-03-15      | NULL          |

**Cómo se consulta:** un JOIN de `fact_orders` contra `dim_products` filtrando `WHERE order_date BETWEEN dbt_valid_from AND COALESCE(dbt_valid_to, '9999-12-31')` trae automáticamente la fila `'Electrónica'` para una orden de enero, y `'Accesorios'` para una orden de abril.

**Por qué se llama "SCD2":** es el patrón estándar de **Slowly Changing Dimension**, técnica **Type 2** (existen Type 1 = sobrescribir sin guardar historial, y Type 3 = guardar solo el valor anterior en una columna extra — Type 2 es el más completo, porque preserva *todo* el historial de versiones, no solo la última).

**Por qué esto sería doloroso a mano:** implementarías tú mismo la lógica "comparar fila actual vs. última guardada → decidir si cambió → cerrar la vieja → insertar la nueva" como un `MERGE` con lógica condicional — y repetirlo para las 5 dimensiones (`dim_orders`, `dim_customers`, `dim_products`, `dim_stores`, `dim_employees`). Con dbt snapshots, es la misma configuración declarativa de ~10 líneas YAML (`strategy: timestamp`, `updated_at: updated_timestamp`) aplicada idéntica a las 5 — dbt genera el `MERGE` correcto por ti, sin que escribas SQL de detección de cambios.

## Tests después de cada capa, no solo al final

**El problema:** si solo corres `dbt test` una vez, al final de todo el pipeline (después de construir `gold`), y algo estaba mal desde `silver_t`, ya construiste **toda** la capa `silver_b` y `gold` sobre datos inválidos — desperdiciaste cómputo, y el error se propagó a las tablas que consume el negocio antes de que nadie se diera cuenta.

**El patrón fail-fast:** el DAG intercala `dbt run` y `dbt test` **por cada capa**:

```text
silver_technical → silver_technical_tests → silver_business → silver_business_tests → ...
```

Si `silver_technical_tests` falla (por ejemplo, un test de `not_null` o `unique` sobre `order_id` no pasa), el DAG **se detiene ahí** — `silver_business` nunca llega a correr sobre datos que ya sabes que están mal. Es más barato fallar temprano (solo perdiste el cómputo de una capa) que fallar tarde (perdiste el cómputo de todo el pipeline, y encima el dato malo pudo llegar hasta un dashboard).

## CeleryExecutor de Airflow

**El problema:** el *scheduler* de Airflow decide qué tarea debe correr y cuándo, pero necesita un mecanismo para **ejecutar** esas tareas — y ese mecanismo (el "executor") determina si Airflow puede escalar o no.

**Las alternativas:**

- `SequentialExecutor`: una tarea a la vez, en el mismo proceso que el scheduler. Solo sirve para pruebas triviales.
- `LocalExecutor`: paraleliza tareas usando multiprocessing en la misma máquina — mejor, pero limitado a los recursos de un solo host.
- `CeleryExecutor`: **distribuye** las tareas entre múltiples *workers*, que pueden vivir en máquinas distintas.

**Cómo funciona `CeleryExecutor` mecánicamente:**

1. El **scheduler** no ejecuta tareas — decide cuál toca correr y publica un mensaje en una **cola** (en este proyecto, **Redis** actúa como *broker* de esa cola — por eso aparece como servicio en `docker-compose.yaml`).
2. Uno o más procesos **worker** (Celery workers) están escuchando esa cola. El primero que esté libre toma el mensaje y ejecuta la tarea (en este proyecto, típicamente un `BashOperator` corriendo `dbt run`/`dbt test`).
3. El resultado y el estado se escriben de vuelta en la base de metadata de Airflow (Postgres).

**Por qué importa:** con `CeleryExecutor`, escalar el pipeline significa agregar más contenedores `worker` (`docker-compose up --scale worker=3`), no agregarle más CPU a una sola máquina. Es el patrón de producción real cuando tienes múltiples DAGs corriendo simultáneamente, o tareas independientes dentro de un mismo DAG que sí se pueden paralelizar. El DAG `orchestrate` de este proyecto encadena sus 10 tareas secuencialmente (`>>`), así que no explota el paralelismo en la práctica — pero el stack (`webserver`, `scheduler`, `dag-processor`, `worker`, `triggerer`, `Postgres`, `Redis` como contenedores separados en `docker-compose.yaml`) está montado exactamente como lo estaría en un despliegue real que sí lo necesite.

## Secretos fuera del código

**El problema:** si hardcodeas un token de Databricks o una connection string de Postgres directamente en un archivo `.py` o `profiles.yml`, ese secreto queda en el historial de git para siempre (incluso si lo borras en un commit posterior, sigue recuperable en commits anteriores) — cualquiera con acceso al repo (o a un fork, o a un leak accidental) puede usarlo.

**Lo que se hizo en este proyecto:** `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, y `DATABRICKS_INGEST_JOB_ID` viven únicamente en `airflow/.env` (gitignorado), y Docker Compose los inyecta a los contenedores vía `env_file`. `orchestrate.py` los lee con `os.environ[...]`, nunca como literales. `profiles.yml` usa `{{ env_var('DATABRICKS_TOKEN') }}` — dbt resuelve esa variable en tiempo de ejecución, nunca queda escrito el valor real en el archivo versionado.

**Por qué importa más allá de "es buena práctica":** un secreto en texto plano en disco no es un riesgo teórico — basta un `git add .` distraído, un repo que se hace público, o un fork para que la credencial quede expuesta y haya que rotarla. Por eso este repo sigue una política de gitignore proactivo: cualquier archivo de secretos o credencial nueva se agrega al `.gitignore` en el momento en que aparece, no después.

## S3 aislado vs. CDC del flujo principal

Este no es tanto un patrón técnico como una decisión consciente de **qué mecanismo usar para qué tipo de fuente**:

- El dataset **core** del negocio (customers, orders, products, employees, stores, order_items) viene de un sistema **operacional vivo** — simulado con Postgres "ghost", que representa lo que en un caso real sería un ERP o un POS emitiendo transacciones constantemente. Para ese tipo de fuente, **CDC** es el patrón correcto: refleja cambios incrementales de un sistema que nunca deja de escribir.
- El dataset de **reseñas** (`gold.reviews`) es un archivo estático (`reviews.csv`) que no nace de ningún sistema operacional propio — es un dataset complementario, cargado una vez. Para ese tipo de fuente, un **data lake (S3) + carga puntual** es un patrón razonable — no tendría sentido montar un pipeline de CDC para un CSV que no cambia.

La elección de mecanismo (CDC vs. carga desde data lake) depende de la naturaleza de la fuente, no es una preferencia arbitraria. Ver también la sección "[Ingesta externa aislada](../README.es.md#5-ingesta-externa-aislada--aws-s3--databricks-goldreviews)" del README para el contexto completo de por qué `gold.reviews` se mantiene fuera del modelado dbt.
