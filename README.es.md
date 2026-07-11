# Walmart Data Platform — de CSVs a un Data Warehouse orquestado

*[English version](README.md)*

Pipeline de datos end-to-end que simula el caso real de una cadena de retail (dataset "Walmart"): captura datos operacionales, los mueve a un lakehouse en Databricks vía CDC, los transforma con dbt siguiendo una arquitectura medallion, y orquesta todo el proceso diario con Airflow en Docker.

Este proyecto son en realidad **dos repos independientes que se conectan entre sí**:

| Repo                                           | Rol                                                                                                     |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| [`data_project_setup`](../data_project_setup) | Ingesta semilla: carga los CSV originales a una base Postgres ("ghost") que actúa como sistema fuente. |
| `data_project_dbt` (este repo)               | Todo lo que pasa después: CDC hacia Databricks, transformación con dbt y orquestación con Airflow.   |

## Índice

- [Por qué lo construí](#por-qué-lo-construí)
- [Arquitectura](#arquitectura)
- [Componentes](#componentes)
- [Stack tecnológico](#stack-tecnológico)
- [Decisiones de diseño (y por qué importan)](#decisiones-de-diseño-y-por-qué-importan)
- [Qué habilidades de Data Engineer demuestra este proyecto](#qué-habilidades-de-data-engineer-demuestra-este-proyecto)
- [Cómo correrlo localmente](#cómo-correrlo-localmente)
- [Créditos](#créditos)

## Por qué lo construí

Quería un proyecto que no fuera "un notebook con un CSV", sino algo que se pareciera a un pipeline de producción real: con una fuente operacional separada del warehouse, ingesta incremental (no full-reload cada vez), un modelo de capas con responsabilidades claras, tests automatizados, y un orquestador que corre solo todos los días. La idea era tocar, de punta a punta, las piezas con las que se trabaja como Data Engineer: una base transaccional, un pipeline de CDC, un data warehouse cloud, un framework de transformación (dbt) y un orquestador (Airflow) corriendo en contenedores — no solo la parte de SQL.

## Arquitectura

![Mapa visual del pipeline: Postgres/S3 → CDC/Files → dbt (incremental, one big table, star schema) → Airflow](docs/architecture-overview.png)
*Vista general: la fuente OLTP (Postgres "ghost") y la fuente externa (S3) llegan a Databricks por caminos distintos; solo el primero pasa por dbt/Airflow y llega al STAR schema.*

## Componentes

### 1. Ingesta semilla — `data_project_setup`

- `walmart_dataset/data/*.csv` — el dataset crudo (customers, stores, products, employees, orders, order_items).
- `walmart_dataset/ddl/walmart_schema.sql` — DDL de las tablas destino en Postgres.
- `load_data.py` — script en Python (`psycopg2`) que hace `COPY ... FROM STDIN WITH CSV HEADER` de cada CSV hacia el schema `raw` de la base Postgres.

Este repo se mantiene **independiente** de `data_project_dbt` porque resuelve una responsabilidad distinta: poner los datos en el sistema fuente, no transformarlos. En un caso real esto sería el rol de un sistema operacional (un ERP, un POS) — aquí se simula con esta carga puntual de CSVs.

### 2. Base "ghost" (Postgres) — sistema fuente agentic

"ghost" (`db.ghost.build`) no es una instancia Postgres administrada a mano: es una **base de datos agentic**. Se creó a partir únicamente del DDL (`walmart_dataset/ddl/walmart_schema.sql`) y lenguaje natural — un agente leyó ese archivo y aprovisionó la base de datos, el schema y las tablas sin que nadie corriera `CREATE DATABASE`/`CREATE TABLE` a mano. Ya con el schema creado, `load_data.py` (punto 1) hizo la ingesta real de los CSVs.

Trabajar con una base agentic como Ghost da ventajas que una instancia Postgres tradicional no tiene de forma nativa — por ejemplo, **fork de la base de datos**: se puede crear una copia/branch completa de la base (a nivel de git-branch, pero de datos) para probar migraciones, seeds o cambios de schema sin tocar la instancia principal, y descartarla o promoverla después. ([Más detalle sobre bases agentic y forking →](docs/CONCEPTOS.md#base-ghost-como-base-de-datos-agentic))

Ghost es el punto desde el que Databricks hace **CDC (Change Data Capture)**: en vez de recargar todo el dataset cada vez, el job de Databricks solo trae los cambios (`INSERT`, `UPDATE`, `DELETE`) desde la última corrida.

### 3. Ingesta a Databricks (Bronze) — pipeline que terminó en Airflow

Ese pipeline de CDC arrancó como un job independiente en Databricks (referenciado por `DATABRICKS_INGEST_JOB_ID`) y más adelante quedó integrado como la primera tarea del DAG de Airflow (`ingest_cdc` en `orchestrate.py`), que lo dispara vía `WorkspaceClient` y hace *polling* de su estado hasta que termina. El job deposita los datos en el catálogo `walmart`, schema `bronze` — la capa "raw" dentro del lakehouse, sin transformar.

![Jobs & Pipelines en Databricks: ingest_walmart, ingest_walmart job (scheduled) y postgres_to_bronze](docs/Databricks_jobs_pipelines.png)
*El job de CDC (`postgres_to_bronze`) corriendo en Databricks Workflows, junto al job aislado `ST-walmart.gold.reviews` de la sección 5.*

![Catalog Explorer — catálogo walmart, schema bronze con las 6 tablas fuente](docs/Databricks_bronze.png)
*Las 6 tablas fuente ya aterrizadas en `walmart.bronze`, sin transformar.*

### 4. Transformación con dbt — arquitectura medallion

Todo vive en `airflow/walmart_project/`, target Databricks (catálogo `walmart`):

1. **Source** (`models/source/sources.yml`) — declara `bronze.orders`, `customers`, `products`, `order_items`, `employees`, `stores`.
2. **Silver técnica** (`models/silver_t/`, schema `silver_t`) — un modelo **incremental** por entidad. Cada uno agrega `processed_at` y solo procesa filas con `updated_timestamp` más nuevo que lo ya cargado (`is_incremental()`), en vez de reprocesar toda la tabla cada corrida.
3. **Silver de negocio** (`models/silver_b/obt_b.sql`, schema `silver_b`) — una "one big table" (OBT) que joinea las 6 tablas `silver_t` en un solo modelo ancho. Es un **pipeline metadata-driven**: el JOIN no está escrito a mano por tabla, sino generado por un loop de Jinja sobre una lista de configuración (`configs = [{table, columns, alias, join_condition}, ...]`) — agregar una nueva fuente es agregar una entrada a esa lista, no escribir SQL nuevo.
4. **Gold ephemeral** (`models/gold/ephemeral/eph_*.sql`) — recortes de `obt_b` por entidad. Al materializarlos como `ephemeral`, dbt no crea tabla ni vista física en Databricks: en tiempo de compilación los **inyecta como CTE** dentro de cualquier modelo que los referencie con `ref()`. Es modularidad gratis — un archivo, una responsabilidad, por entidad — sin pagar el costo de storage/compute de un paso que solo existe para alimentar al siguiente.
5. **Gold — dimensiones SCD2** (`snapshots/dim_*.yml`) — snapshots de dbt (`strategy: timestamp`) sobre los modelos `eph_*`, que arman dimensiones históricas (`dim_orders`, `dim_customers`, `dim_products`, `dim_stores`, `dim_employees`). Cada vez que una fila cambia, dbt cierra la versión anterior (`dbt_valid_to`) y abre una nueva (`dbt_valid_from`) automáticamente — el patrón **Slowly Changing Dimension Type 2**, que a mano significa escribir (y mantener) un `MERGE` con detección de cambios, llaves surrogadas y control de concurrencia por cada dimensión. Acá es la misma config declarativa de ~10 líneas, aplicada igual a las 5.
6. **Gold — hechos** (`models/gold/fact/fact_orders.sql`) — la tabla de hechos al grano de línea de orden, lista para consumo analítico.

Los puntos 4 y 5 son, para mí, el argumento más fuerte para usar dbt en este proyecto: convierten dos problemas clásicamente manuales de ingeniería de datos — modularizar transformaciones sin pagar costo de materialización, y mantener historial tipo SCD2 por dimensión — en configuración declarativa y reutilizable, en vez de cientos de líneas de SQL/`MERGE` escritas y mantenidas a mano, una por una, por cada tabla. (Mecánica completa de ambos patrones en [docs/CONCEPTOS.md](docs/CONCEPTOS.md#materialización-ephemeral-gold).)

Las dimensiones (`dim_*`) más la tabla de hechos (`fact_orders`) forman un **STAR Schema** clásico en la capa `gold`: `fact_orders` en el centro, referenciando cada `dim_*` por su llave natural — el modelo estándar para que herramientas de BI (Looker, Power BI, Tableau) consuman los datos sin tener que reconstruir joins complejos.

Un macro (`macros/custom_schema.sql`) sobreescribe `generate_schema_name` de dbt para que cada modelo caiga exactamente en el schema declarado (`silver_t`, `silver_b`, `gold`), sin la concatenación por defecto de dbt.

![Catalog Explorer — schema silver_t (6 tablas técnicas) y silver_b (obt_b)](docs/Databricks_silver.png)
*Las 6 tablas incrementales de `silver_t` y la OBT (`obt_b`) en `silver_b`, ya joineadas.*

### 5. Ingesta externa aislada — AWS S3 → Databricks (`gold.reviews`)

![Detalle de cada componente del pipeline y por qué gold.reviews (S3 → Databricks) queda fuera del modelado dbt](docs/architecture-detailed.png)
*Mismo mapa que la sección "Arquitectura", pero con el detalle de cada componente y la nota explícita de que `gold.reviews` no está integrado al STAR schema por diseño.*

![Catalog Explorer — schema gold: dim_*, fact_orders y reviews conviviendo sin joins entre sí](docs/Databricks_gold.png)

La captura de arriba lo muestra en vivo: `reviews` está en el mismo schema `gold` que `fact_orders`/`dim_*`, pero como tabla suelta — no hay ningún modelo dbt que la referencie.

El objetivo de esta conexión fue puramente de **aprendizaje de infraestructura**: entender cómo se conecta S3 con Databricks, no enriquecer el modelo de datos. Se subió un CSV de reseñas de producto a un bucket de **S3** y se conectó a Databricks vía una **External Location** (Unity Catalog): Databricks emite el token/credencial que AWS confía para autorizar el acceso al bucket. Esta conexión es completamente independiente del flujo principal (Postgres → CDC) y creó un job en Databricks (`ST-walmart.gold.reviews`) que materializa los datos directamente en `walmart.gold.reviews`.

Este es un cambio de **infraestructura/ecosistema** (configurado en las consolas de AWS y Databricks), no de código: no hay ningún archivo en este repo que lo represente. `gold.reviews` vive **aislada a propósito** — no forma parte del modelado dbt (medallion) de este proyecto ni hay plan de integrarla; si en algún momento se quisiera unir al STAR schema, faltaría escribir un modelo dbt (`source`/`ref`) que la conecte con `fact_orders`/`dim_products`.

### 6. Orquestación — Airflow + Docker Compose

`airflow/dags/orchestrate.py` define un único DAG (`orchestrate`) que encadena todo el flujo diario:

```text
ingest_cdc → clean_target → source_freshness → silver_technical → silver_technical_tests
  → silver_business → silver_business_tests → gold_ephemeral → gold_dimensions → gold_facts
```

![Airflow UI — ejecución exitosa del DAG orchestrate, las 10 tareas en verde](docs/Airflow_dags.png)
*Corrida completa del DAG `orchestrate`, las 10 tareas en verde de punta a punta.*

- `ingest_cdc` dispara y espera el job de Databricks (SDK `WorkspaceClient`, polling cada 5s).
- `clean_target` (`@task.bash`) borra `target/` y `logs/` del proyecto dbt antes de cada corrida, para que ningún artefacto de una ejecución previa (manifest, compilación cacheada) contamine la actual.
- `source_freshness` corre `dbt source freshness` antes de transformar, para no construir sobre datos fuente obsoletos.
- Cada capa (`silver_t`, `silver_b`) corre su `dbt run` seguido de su `dbt test`, para no dejar avanzar el pipeline si una capa no pasa sus validaciones.
- `gold_dimensions` corre `dbt snapshot --select dim_orders dim_customers dim_products dim_stores dim_employees` para materializar el historial SCD2, en vez de un `dbt snapshot` sin filtro.

Todo el stack de Airflow (webserver, scheduler, dag-processor, worker, triggerer, Postgres, Redis) corre containerizado vía `docker-compose.yaml` con `CeleryExecutor` — el scheduler encola las tareas en Redis y uno o más workers las ejecutan, en vez de correr todo en un solo proceso ([cómo funciona →](docs/CONCEPTOS.md#celeryexecutor-de-airflow)).

## Stack tecnológico

- **Orquestación:** Apache Airflow 3.x (CeleryExecutor, Docker Compose)
- **Transformación:** dbt-core + dbt-databricks
- **Warehouse / Lakehouse:** Databricks (catálogo `walmart`)
- **Sistema fuente:** PostgreSQL
- **Ingesta:** Python (`psycopg2`), Databricks Jobs (CDC)
- **Data lake externo:** AWS S3, conectado a Databricks vía External Location (Unity Catalog)
- **Infraestructura:** Docker / Docker Compose
- **Gestión de dependencias:** `uv`
- **CI:** GitHub Actions (`.github/workflows/ci.yml`) — `ruff` sobre `airflow/dags`, `sqlfluff` (templater de dbt, contra el target real de Databricks) y `dbt test`, en cada push/PR a `main`.

## Decisiones de diseño (y por qué importan)

Resumen corto de cada decisión — el mecanismo completo (qué problema resuelve, qué pasaría sin él, cómo funciona paso a paso) está en **[docs/CONCEPTOS.md](docs/CONCEPTOS.md)**.

- **Medallion architecture (bronze → silver → gold):** separa "datos tal cual llegan" de "datos limpios" de "datos listos para negocio", aislando en qué capa se rompió algo. → [detalle](docs/CONCEPTOS.md#arquitectura-medallion-bronze--silver--gold)
- **Modelos incrementales en `silver_t`:** solo procesa lo que cambió desde la última corrida, no el histórico completo — el patrón real en pipelines de volumen alto. → [detalle](docs/CONCEPTOS.md#modelos-incrementales-en-silver_t)
- **CDC en vez de extract-and-replace:** refleja cómo se integran sistemas operacionales reales con un warehouse, sin tumbar la fuente ni mover más datos de los necesarios. → [detalle](docs/CONCEPTOS.md#cdc-en-vez-de-extract-and-replace)
- **Snapshots de dbt para dimensiones (SCD2):** conserva el historial de cambios de `customers`/`products`, no solo el estado actual — necesario para análisis histórico correcto. → [detalle](docs/CONCEPTOS.md#snapshots-de-dbt-para-dimensiones-scd2)
- **OBT (`obt_b`) generado con Jinja:** metaprogramación en dbt para no repetir 6 bloques de JOIN casi idénticos a mano. → [detalle](docs/CONCEPTOS.md#obt-metadata-driven-con-jinja-silver_b)
- **Tests después de cada capa, no solo al final:** el DAG falla rápido si `silver_t` o `silver_b` no pasan sus tests, en vez de construir `gold` sobre datos ya inválidos. → [detalle](docs/CONCEPTOS.md#tests-después-de-cada-capa-no-solo-al-final)
- **Repos separados para ingesta vs. transformación:** cada repo tiene una responsabilidad y un ciclo de vida propios — se puede tocar la ingesta sin tocar el warehouse, y viceversa.
- **Secretos fuera del código:** las credenciales de Databricks se leen de variables de entorno inyectadas vía `.env` + Docker Compose, nunca hardcodeadas en el DAG. → [detalle](docs/CONCEPTOS.md#secretos-fuera-del-código)
- **CDC directo desde OLTP para el flujo principal, S3 solo para datos externos:** el dataset core no pasa por un data lake — se simula un sistema fuente OLTP y se hace CDC directo a Databricks; S3 se reserva para un dataset externo y complementario (reseñas) que no nace de ningún sistema operacional propio. → [detalle](docs/CONCEPTOS.md#s3-aislado-vs-cdc-del-flujo-principal) / ver [Ingesta externa aislada](#5-ingesta-externa-aislada--aws-s3--databricks-goldreviews).

## Qué habilidades de Data Engineer demuestra este proyecto

- **Modelado de datos:** arquitectura medallion, OBT, **Slowly Changing Dimensions (SCD2)**, **STAR Schema** (`fact_orders` + `dim_*`) — los patrones que se usan en warehouses reales, no solo tablas planas.
- **dbt en profundidad:** modelos incrementales, modelos ephemeral, snapshots, tests, macros, configuración por carpeta, **pipeline metadata-driven** (generación dinámica de SQL con Jinja a partir de una lista de configuración, no SQL repetido a mano).
- **Orquestación:** diseño de un DAG con dependencias explícitas, *bash operators* vs. *python tasks*, y un patrón run→test por capa.
- **Integración de sistemas:** mover datos entre un sistema operacional (Postgres) y un lakehouse (Databricks) vía CDC, no solo cargas manuales.
- **Infraestructura como código:** stack completo de Airflow reproducible con Docker Compose.
- **Higiene de secretos y control de versiones:** identificar credenciales hardcodeadas, moverlas a variables de entorno, y estructurar `.gitignore`/repos para que nunca terminen en el historial de git — un error común que aprendí a detectar y corregir en este mismo proyecto.
- **CI/CD:** pipeline de GitHub Actions que corre lint (`ruff`, `sqlfluff` vía el templater de dbt) y `dbt test` contra el warehouse real en cada push/PR, para no depender de que el desarrollador se acuerde de correrlo localmente.

## Cómo correrlo localmente

```bash
# 1. Dependencias Python (incluye ruff/sqlfluff con --all-groups)
uv sync --all-groups

# 2. Levantar Airflow
cd airflow
cp .env.example .env   # completar DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_INGEST_JOB_ID
docker-compose up airflow-init
docker-compose up

# 3. dbt (contra el mismo proyecto que usa Airflow) — requiere DATABRICKS_TOKEN exportado en el shell local
cd walmart_project
export DATABRICKS_TOKEN=<tu-token>
dbt run
dbt test

# 4. Lint (lo que corre en CI)
uv run ruff check airflow/dags
uv run sqlfluff lint airflow/walmart_project/models airflow/walmart_project/snapshots
```

Airflow queda disponible en `http://localhost:8080`.

## Créditos

Este proyecto sigue el recorrido del [video de Ansh Lamba](https://www.youtube.com/watch?v=ZEE-jNAthB0), adaptado y extendido con mis propios cambios y decisiones de diseño.
