# goit-rdb-hw-07

Домашнє завдання 7 з курсу реляційних баз даних (GoIT Neoversity). Тема: часові ряди та напівструктуровані дані в PostgreSQL. У notebook є time-series ML-ознаки через window functions на реальних поїздках таксі Нью-Йорка, JSONB event-log, GIN і expression індекси з `EXPLAIN (ANALYZE, BUFFERS)` і порівняння JSONB з нормалізованою таблицею.

## Дані

Реальні поїздки жовтого таксі Нью-Йорка, **NYC TLC Yellow Taxi Trip Records за лютий і березень 2024**. Stub-schema fallback **не використано**.

- Офіційна сторінка: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [`yellow_tripdata_2024-02.parquet`](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet)
- [`yellow_tripdata_2024-03.parquet`](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet)

**Як notebook отримує файли.** Перша секція качає обидва Parquet у папку `data/`, якщо їх там ще немає. Parquet-файлів у репозиторії немає (`.gitignore`). З кожного файлу читаються 7 колонок шаблону, без `head()`: 3 007 526 рядків за лютий і 3 582 628 за березень.

**Чому два місяці, а не `head(500_000)` з шаблону.** Перші 500 000 рядків березневого файлу покривають лише 7 дат, від 2024-02-29 до 2024-03-06, і жодна зона не має там більше 7 днів. Тоді `trips_lag_30d` завжди `NULL`, а view з `WHERE trips_lag_30d IS NOT NULL` порожній. Один місяць дає lag-30d лише для свого останнього дня. Два місяці дають 60 днів, і lag-30d та rolling 30d мають сенс.

**Фільтри перед завантаженням.** Фільтри шаблону (`fare_amount` у (0, 500), `trip_distance` у (0, 100), `PULocationID` не `NULL`) і додатковий фільтр дати посадки `[2024-02-01, 2024-04-01)`. Фільтр дати прибирає 16 рядків з датами на кшталт 2002-12-31 чи 2024-04-01, щоб у daily-таблиці не було днів поза періодом. З 6 590 154 рядків лишилось 6 341 305.

**Завантаження.** `nyc_taxi_raw` створюється явним DDL з тими самими назвами і типами, що дав би `to_sql`, а дані йдуть через `COPY ... FROM STDIN` порціями по 1 млн рядків. `to_sql` на 6+ млн рядків надто повільний.

## Таблиці

| Таблиця | Що всередині | Рядків |
|---|---|---|
| `nyc_taxi_raw` | поїздки після фільтрів, 7 колонок як у Parquet | 6 341 305 |
| `t7_hw_taxi_daily` | зона посадки × день: `trip_count`, `avg_fare`, `total_distance`; 258 зон, 60 дат | 13 511 |
| `t7_hw_taxi_trips_typed` | поїздки з `pickup_at` / `dropoff_at` типу `TIMESTAMPTZ` | 6 341 305 |
| `v_t7_hw_taxi_features` (view) | усі time-series ознаки з календарною охороною | 5 167 |
| `t7_hw_events` | синтетичний JSONB event-log, 5 типів подій | 50 000 |
| `t7_hw_checkouts_norm` | checkout-події в типізованих колонках | 10 000 |

## Time-series features

Усі ознаки рахуються window functions на `t7_hw_taxi_daily`, і **кожне вікно має `PARTITION BY pickup_zone_id`**, щоб історія однієї зони не перетікала в іншу.

| Ознака | Як рахується |
|---|---|
| `trips_lag_1d`, `trips_lag_7d`, `trips_lag_30d` | `LAG(trip_count, n)` у вікні `ORDER BY ts_day` |
| `rolling_avg_fare_7d` | `AVG(avg_fare)` з `RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW`, тобто 7 календарних днів |
| `rolling_std_fare_30d` | `STDDEV_SAMP(avg_fare)` з `RANGE BETWEEN INTERVAL '29 days' PRECEDING AND CURRENT ROW` |
| `cumulative_trips`, `expanding_avg_trips` | `SUM` і `AVG` з `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |

Усі ознаки зібрані у view `v_t7_hw_taxi_features`.

**Календарна охорона.** У 102 з 258 зон є дні без жодної поїздки, і рядків за ці дні в daily-таблиці немає. `LAG(trip_count, 30)` рахує 30 рядків назад, тому в такій зоні lag-30d може дивитися на 49 днів назад. View рахує ще `LAG(ts_day, 30)` і лишає тільки рядки, де `ts_day - ts_day_lag_30d = 30`: тоді 30 днів перед поточним ідуть без пропусків, і всі три lag-и календарно точні. Умова шаблону дає 6 395 рядків у 227 зонах, з охороною лишається 5 167 рядків у 192 зонах.

## JSONB-частина

`t7_hw_events` створює SQL з умови: 50 000 подій, 5 типів (`page_view`, `click`, `add_to_cart`, `checkout`, `recommendation_shown`) по 10 000, у кожного типу своя структура `payload`. Запити: containment `@>`, key existence `?` і `?|`, JSONPath через `jsonb_path_query` і `jsonb_path_exists`.

Плани `EXPLAIN (ANALYZE, BUFFERS)` у notebook:

| Запит | Без індексу | З індексом |
|---|---|---|
| `payload @> {"payment_method": "card"}` | `Seq Scan`, 20.521 ms | GIN `jsonb_path_ops`: `Bitmap Index Scan` + `Bitmap Heap Scan`, 2.305 ms |
| `payload @> {"referrer": "google"}` | | GIN `jsonb_path_ops`: `Bitmap Index Scan` + `Bitmap Heap Scan`, 1.635 ms |
| `payload ->> 'device' = 'mobile'` | `Seq Scan` (GIN не підходить), 4.317 ms | expression B-tree: `Bitmap Index Scan` + `Bitmap Heap Scan`, 1.428 ms |
| `payload ? 'tags'` | `Seq Scan` при `jsonb_path_ops`, 3.597 ms | GIN `jsonb_ops`: `Bitmap Index Scan` + `Bitmap Heap Scan`, 2.579 ms |

Розміри при таблиці 8000 kB: GIN `jsonb_ops` 1696 kB, GIN `jsonb_path_ops` 1144 kB, expression B-tree на `device` 360 kB.

**JSONB vs normalized.** Той самий запит (середній `total` checkout за способом оплати) дає однакову відповідь з обох таблиць. JSONB-версія читає `Seq Scan` усі 1 000 сторінок `t7_hw_events` і виконується за 5.866 ms, normalized-версія читає 94 сторінки `t7_hw_checkouts_norm` за 1.768 ms. Bonus: той самий запит як MongoDB aggregation pipeline (pseudocode у markdown-клітинці).

Час виконання в таблицях узято з виконаного notebook у репозиторії. Після нового запуску числа трохи відрізнятимуться, оператори плану лишаються ті самі.

## Де що в notebook

| Завдання | Розділ `RDB_HOMEWORK7.ipynb` |
|---|---|
| 1. Setup і завантаження | 1.1 завантаження Parquet, 1.2 чому не `head(500_000)`, 1.3 фільтри, 1.4 `nyc_taxi_raw` через `COPY`, 1.5 `t7_hw_taxi_daily`, 1.6 typed-таблиця з `TIMESTAMPTZ` |
| 2. Time-series features | 2.1-2.4 lag, rolling 7d, rolling 30d, expanding; 2.5 аудит пропусків днів; 2.6 view з календарною охороною |
| 3. JSONB event-log | створення `t7_hw_events`, 3.1 `@>`, 3.2 `?` і `?|`, 3.3 JSONPath |
| 4. GIN + EXPLAIN | Step 1-4 з умови, 4.5 `?` при `jsonb_path_ops` і `jsonb_ops`, 4.6 розміри індексів |
| 5. JSONB vs normalized | 5.1 `t7_hw_checkouts_norm`, 5.2 два `EXPLAIN`, 5.3 bonus MongoDB pseudocode |
| Reflection | останній розділ |

## Як запустити

**Google Colab.** Відкрити `RDB_HOMEWORK7.ipynb` у Colab і вибрати Runtime → Run all. Перша клітинка ставить `pixeltable-pgserver==0.4.0`, `psycopg2-binary`, `sqlalchemy`, `pandas`, `pyarrow`, `requests` і `jupysql`. PostgreSQL 16 запускається прямо в runtime, окремий сервер не потрібен. Оригінальний пакет `pgserver` на поточній версії Python у Colab не встановлюється, а цей форк має той самий API.

**Локально:**

```bash
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python pip jupyter nbconvert ipykernel
.venv/bin/jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1800 RDB_HOMEWORK7.ipynb
```

Кластер PostgreSQL лежить у `/tmp/hw7_pg`. Notebook можна запускати повторно на тому самому кластері, бо кожен блок починається з `DROP ... IF EXISTS`.

## Файли

| Файл | Що всередині |
|---|---|
| `RDB_HOMEWORK7.ipynb` | notebook з усіма завданнями, SQL і output клітинок |
| `README.md` | цей файл |
| `.gitignore` | `data/*.parquet`, `.venv/`, службові файли |
