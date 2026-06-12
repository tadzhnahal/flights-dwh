# Часть №1 проекта. STG слой и общий запуск пайплайна

<img width="2879" height="1249" alt="изображение" src="https://github.com/user-attachments/assets/7ddd0c2a-5037-45d9-90f8-550345702e9a" />

## Описание

Эта часть проекта закрывает первый слой хранилища данных.

STG слой берёт исходные файлы, читает их, приводит поля к нужному виду и кладёт строки в PostgreSQL. Команда дальше читает эти таблицы и строит DDS и витрины.

В проекте есть два источника:

```text
S3 с рейсами в формате .csv.gz
ourairports.csv со справочником аэропортов
```

Airflow запускает пайплайн. PostgreSQL хранит таблицы. Jupyter помогает проверить результат SQL-запросами.

## Что я сделал

1. Создал схемы `team_vdga_stg` и `team_vdga_metadata`.
2. Создал таблицу рейсов `team_vdga_stg.flights_raw`.
3. Создал таблицу аэропортов `team_vdga_stg.airports`.
4. Создал технический лог `team_vdga_metadata.etl_load_log`.
5. Написал скрипт, который грузит справочник аэропортов.
6. Написал скрипт, который грузит рейсы из S3.
7. Добавил защиту от дублей по `source_file`.
8. Собрал Airflow DAG `team_vdga_stg_dag`.
9. Добавил проверки STG после каждого запуска.
10. Научил скрипт искать все `.csv.gz` внутри папки за дату.
11. Научил DAG принимать дату через параметр `flight_date`.
12. Настроил плановый STG-запуск по дате Airflow-интервала.
13. Догрузил исторические даты из S3 в `dwh_training`.
14. Проверил STG из Jupyter через PostgreSQL.
15. Добавил общий DAG `team_vdga_pipeline_dag`, который запускает слои в порядке `STG -> DDS -> DM`.
16. Проверил общий запуск из Airflow и PostgreSQL.

## Где лежат таблицы

Проект использует три основные схемы команды:

```text
team_vdga_stg
team_vdga_dds
team_vdga_dm
```

STG таблицы:

```text
team_vdga_stg.flights_raw
team_vdga_stg.airports
team_vdga_metadata.etl_load_log
```

DDS таблицы:

```text
team_vdga_dds.airports_timezones
team_vdga_dds.flights_cancelled
team_vdga_dds.flights_performed
```

DM таблицы:

```text
team_vdga_dm.flight_cancellations
team_vdga_dm.flight_delays
```

`team_vdga_stg.flights_raw` хранит рейсы.

`team_vdga_stg.airports` хранит справочник аэропортов.

`team_vdga_metadata.etl_load_log` хранит историю файлов. По этой таблице скрипт понимает, какой файл уже попал в STG.

## База данных

Команда работает с базой:

```text
POSTGRES_HOST=postgres_edu
POSTGRES_PORT=5432
POSTGRES_DB=dwh_training
POSTGRES_USER=student_dwh
```

Файл `.env.example` хранит шаблон без паролей.

Файл `.env` хранит реальные значения для локального запуска. Его нельзя класть в Git и нельзя отправлять в общий чат.

Airflow не берёт пароли из Python-кода. DAG-и читают доступы из Airflow Connections:

```text
edu_dwh_postgres
team_vdga_s3
```

`edu_dwh_postgres` даёт доступ к PostgreSQL.

`team_vdga_s3` даёт доступ к S3.

## Источник рейсов

Рейсы лежат в S3:

```text
s3://gsbdwhdata/flights_us_data/<flight_date>/*.csv.gz
```

Пример:

```text
s3://gsbdwhdata/flights_us_data/2026-06-11/flights_2026-06-11.csv.gz
```

Дата в пути S3 показывает техническую дату файла. Дата рейса лежит внутри файла в поле `flightdate`. STG кладёт её в поле `flight_dt`.

Для аналитики нужно брать `flight_dt`, а не дату из пути S3.

## Как STG грузит рейсы

Один запуск STG грузит одну техническую дату из S3.

Скрипт получает дату через аргумент:

```bash
python scripts/load_flights_raw.py --flight-date 2026-06-11 --load-to-postgres
```

Скрипт ищет все файлы внутри папки:

```text
flights_us_data/2026-06-11/
```

Скрипт берёт только файлы с расширением:

```text
.csv.gz
```

Перед вставкой строк скрипт смотрит в таблицу:

```text
team_vdga_metadata.etl_load_log
```

Если файл уже имеет статус `success`, скрипт пропускает файл и не пишет строки второй раз.

Так STG защищает таблицу `team_vdga_stg.flights_raw` от дублей.

## Как STG грузит аэропорты

Скрипт `scripts/load_airports.py` скачивает `ourairports.csv`, читает строки и кладёт их в таблицу:

```text
team_vdga_stg.airports
```

Если таблица уже содержит строки, скрипт пропускает повторный запуск.

Справочник аэропортов нужен, чтобы связать рейсы с названиями аэропортов, странами, регионами и координатами.

Главное поле для связи:

```text
airports.iata_code
```

В рейсах ему соответствуют поля:

```text
flights_raw.origin_code
flights_raw.dest_code
```

## Backfill старых дат

Backfill нужен, если в S3 уже лежит много дат, а STG ещё не содержит эти файлы.

Сначала нужно получить список дат в файл:

```text
available_flight_dates.txt
```

Потом можно пройти по датам циклом:

```bash
LOG_FILE="stg_backfill_$(date +%Y%m%d_%H%M%S).log"

while read -r flight_date; do
  echo
  echo "===== load ${flight_date} ====="

  python scripts/load_flights_raw.py \
    --flight-date "$flight_date" \
    --load-to-postgres || break

  python scripts/check_stg_quality.py \
    --flight-date "$flight_date" || break

done < available_flight_dates.txt 2>&1 | tee "$LOG_FILE"

echo "log saved to: $LOG_FILE"
```

Backfill нужен для истории.

Ежедневный режим должен идти через Airflow.

## Текущий объём STG

На момент проверки в `team_vdga_stg.flights_raw` лежат:

```text
source_files_cnt = 103
rows_cnt = 2005449
min_flight_dt = 2024-03-01
max_flight_dt = 2024-06-11
```

Эти числа могут вырасти после новых запусков. Поэтому актуальный объём нужно проверять SQL-запросом:

```sql
select
    count(distinct source_file) as source_files_cnt,
    count(*) as rows_cnt,
    min(flight_dt) as min_flight_dt,
    max(flight_dt) as max_flight_dt
from team_vdga_stg.flights_raw;
```

Строки по файлам можно проверить так:

```sql
select
    source_file,
    count(*) as rows_cnt,
    min(flight_dt) as min_flight_dt,
    max(flight_dt) as max_flight_dt
from team_vdga_stg.flights_raw
group by source_file
order by source_file;
```

Лог рейсов можно проверить так:

```sql
select
    status,
    count(*) as log_rows_cnt,
    sum(rows_loaded) as rows_loaded_sum
from team_vdga_metadata.etl_load_log
where flow_name = 'stg_flights_raw'
group by status
order by status;
```

Дубли успешных загрузок можно проверить так:

```sql
select
    flow_name,
    source_file,
    count(*) as success_rows_cnt
from team_vdga_metadata.etl_load_log
where flow_name = 'stg_flights_raw'
  and status = 'success'
group by flow_name, source_file
having count(*) > 1
order by source_file;
```

Хороший результат:

```text
0 rows
```

## Как зайти в PostgreSQL из Jupyter

Данные лежат в PostgreSQL, а не в Git. Поэтому для чтения STG, DDS и DM не нужно клонировать репозиторий. Достаточно открыть Jupyter Terminal и зайти в базу.

Задайте переменные:

```bash
export POSTGRES_HOST="postgres_edu"
export POSTGRES_PORT="5432"
export POSTGRES_DB="dwh_training"
export POSTGRES_USER="student_dwh"
export POSTGRES_PASSWORD="sql"
```

Пароль не выводите в терминал и не отправляйте в чат.

Проверьте вход в базу:

```bash
PGCONNECT_TIMEOUT=5 PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -P pager=off \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "select current_database(), current_user, now();"
```

Хороший результат:

```text
dwh_training | student_dwh
```

Если команда вернула строку, Jupyter видит PostgreSQL.

## Как зайти в psql

Выполните:

```bash
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -P pager=off \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB"
```

После входа появится приглашение:

```text
dwh_training=#
```

После этого нужно вставлять только SQL.

Для выхода выполните:

```sql
\q
```

Если вставить SQL прямо в обычный терминал, Bash выдаст ошибки вроде `command not found`. Сначала нужно зайти в `psql`, дождаться `dwh_training=#`, и только потом вставлять SQL.

## Как работать после клона проекта

Этот вариант нужен тем, кто меняет код проекта.

Склонируйте репозиторий через HTTPS:

```bash
cd ~
git clone https://github.com/tadzhnahal/flights-dwh.git
cd ~/flights-dwh
```

Или через SSH:

```bash
cd ~
git clone git@github.com:tadzhnahal/flights-dwh.git
cd ~/flights-dwh
```

Если проект уже есть в Jupyter, обновите код:

```bash
cd ~/flights-dwh
git pull
```

Файл `.env` не лежит в Git. Создайте его локально или возьмите значения у команды:

```text
POSTGRES_HOST=postgres_edu
POSTGRES_PORT=5432
POSTGRES_DB=dwh_training
POSTGRES_USER=student_dwh
POSTGRES_PASSWORD=sql
```

Загрузите переменные из `.env`:

```bash
set -a
source .env
set +a
```

Проверьте переменные без пароля:

```bash
echo "POSTGRES_HOST=$POSTGRES_HOST"
echo "POSTGRES_PORT=$POSTGRES_PORT"
echo "POSTGRES_DB=$POSTGRES_DB"
echo "POSTGRES_USER=$POSTGRES_USER"
```

## Структура проекта

```text
.
├── dags
│   ├── team_vdga_pipeline_dag.py
│   ├── team_vdga_stg_dag.py
│   ├── team_vdga_dds_dag.py
│   └── team_vdga_dm_dag.py
├── dbt
│   ├── dbt_project.yml
│   ├── flights.yml
│   ├── packages.yml
│   ├── sources.yml
│   └── models
│       ├── flights_cancelled.sql
│       ├── flights_performed.sql
│       └── darandgoncharova
│           ├── flight_cancellations.sql
│           └── flight_delays.sql
├── scripts
│   ├── check_connections.py
│   ├── check_stg_quality.py
│   ├── cleaning.py
│   ├── common.py
│   ├── load_airports.py
│   ├── load_flights_raw.py
│   ├── load_log.py
│   └── run_sql.py
├── sql
│   ├── 01_create_schemas.sql
│   ├── 02_create_load_log.sql
│   ├── 03_create_stg_airports.sql
│   ├── 04_create_stg_flights_raw.sql
│   └── create_schema_dds.sql
├── .airflowignore
├── .env.example
├── .gitignore
└── README.md
```

Папка `dags` хранит Airflow DAG-и.

Папка `scripts` хранит Python-скрипты для STG.

Папка `sql` хранит SQL для схем и таблиц.

Папка `dbt` хранит модели DDS и DM.

## Таблица рейсов

Таблица `team_vdga_stg.flights_raw` хранит сырые рейсы.

Ключевые поля:

```text
flight_dt
carrier_code
tail_num
carrier_flight_number
origin_code
dest_code
distance
scheduled_dep_tm
actual_dep_tm
dep_delay_min
scheduled_arr_tm
actual_arr_tm
arr_delay_min
taxi_out_min
wheels_off_tm
wheels_on_tm
taxi_in_min
carrier_delay_min
weather_delay_min
nas_delay_min
security_delay_min
late_aircraft_min
cancelled
cancellation_code
loaded_at
source_file
```

Для даты рейса нужно брать `flight_dt`.

Поле `source_file` хранит путь к исходному файлу в S3.

В S3 файлы лежат в папках вида `2026-06-11`, но внутри файлов даты рейсов относятся к 2024 году. Это не ошибка. STG хранит бизнес-дату в `flight_dt`, а технический путь — в `source_file`.

В старом описании встречались поля:

```text
origin_city_name
dest_city_name
dep_delay_group_num
arr_delay_group_num
flights_cnt
distance_group_num
```

Реальный `.csv.gz` не содержит эти поля. Поэтому STG не хранит их напрямую.

Города можно взять из справочника аэропортов.

Группы задержек и расстояний можно посчитать в DDS или DM.

Счётчик рейсов можно задать как один рейс на строку.

## Таблица аэропортов

Таблица `team_vdga_stg.airports` хранит справочник из `ourairports.csv`.

Ключевые поля:

```text
airport_id
ident
iata_code
type
name
municipality
iso_region
iso_country
latitude_deg
longitude_deg
elevation_ft
scheduled_service
timezone
loaded_at
```

Главное поле для связи с рейсами:

```text
iata_code
```

В рейсах ему соответствуют:

```text
origin_code
dest_code
```

В таблице есть поле `timezone`, но текущий `airports.csv` не содержит таймзоны. Поэтому STG оставляет `timezone` пустым.

Если следующему слою нужны таймзоны, команда должна взять их из другого источника. Сейчас DDS использует отдельную таблицу:

```text
team_vdga_dds.airports_timezones
```

## Технический лог

Таблица `team_vdga_metadata.etl_load_log` хранит историю файлов.

Лог содержит:

```text
flow_name
source_file
flight_dt
rows_loaded
status
error_message
loaded_at
updated_at
```

Для рейсов используется поток:

```text
stg_flights_raw
```

Скрипт смотрит в лог перед вставкой строк. Если файл уже имеет статус `success`, скрипт пропускает файл.

В таблице есть уникальный индекс для успешных файлов. Он не даёт записать один и тот же `source_file` два раза со статусом `success`.

## Контракт для следующих слоёв

Следующие слои читают две STG-таблицы:

```text
team_vdga_stg.flights_raw
team_vdga_stg.airports
```

Рейсы связываются со справочником аэропортов через IATA-коды:

```text
flights_raw.origin_code = airports.iata_code
flights_raw.dest_code = airports.iata_code
```

Дата рейса лежит здесь:

```text
flights_raw.flight_dt
```

Путь к исходному файлу лежит здесь:

```text
flights_raw.source_file
```

DDS берёт STG, добавляет справочник таймзон и строит таблицы выполненных и отменённых рейсов.

DM берёт DDS и строит витрины для анализа задержек и отмен.

## STG DAG

STG DAG называется:

```text
team_vdga_stg_dag
```

Файл:

```text
dags/team_vdga_stg_dag.py
```

DAG выполняет задачи в таком порядке:

```text
start
create_schemas
create_load_log_table
create_airports_table
create_flights_raw_table
load_airports
load_flights_raw
check_stg_quality
finish
```

DAG работает по расписанию:

```text
0 6 * * *
```

Один плановый запуск грузит одну дату. Если дату не передать вручную, DAG берёт дату прошлого Airflow-интервала.

DAG также принимает ручной параметр:

```json
{
  "flight_date": "2026-06-11"
}
```

Если файл уже есть в `etl_load_log` со статусом `success`, загрузчик пропустит файл и не добавит дубли.

## DDS DAG

DDS DAG называется:

```text
team_vdga_dds_dag
```

Файл:

```text
dags/team_vdga_dds_dag.py
```

Он создаёт DDS-схему, грузит таблицу таймзон аэропортов и запускает dbt-модели:

```text
flights_performed
flights_cancelled
```

Основные DDS-таблицы:

```text
team_vdga_dds.airports_timezones
team_vdga_dds.flights_performed
team_vdga_dds.flights_cancelled
```

## DM DAG

DM DAG называется:

```text
team_vdga_dm_dag
```

Файл:

```text
dags/team_vdga_dm_dag.py
```

Он запускает dbt-модели витрин:

```text
flight_delays
flight_cancellations
```

Основные DM-таблицы:

```text
team_vdga_dm.flight_delays
team_vdga_dm.flight_cancellations
```

## Общий DAG

Общий DAG называется:

```text
team_vdga_pipeline_dag
```

Файл:

```text
dags/team_vdga_pipeline_dag.py
```

Он запускает три DAG-а по порядку:

```text
team_vdga_stg_dag -> team_vdga_dds_dag -> team_vdga_dm_dag
```

Этот DAG не содержит SQL, Python-загрузчиков и dbt-моделей. Он только запускает готовые DAG-и и ждёт, пока каждый слой дойдёт до `success`.

Такой подход сохраняет разделение по слоям:

```text
STG — загрузка исходных данных
DDS — слой данных для модели хранилища
DM — витрины для анализа
```

У команды остаются отдельные DAG-и по зонам ответственности. При этом появляется один общий запуск полного пайплайна.

## Как запустить полный пайплайн

Откройте Airflow и найдите DAG:

```text
team_vdga_pipeline_dag
```

Нажмите trigger.

Если интерфейс не даёт передать JSON, можно запустить DAG без параметров. В DAG есть запасная дата:

```text
2026-06-11
```

Если интерфейс даёт передать JSON, можно указать дату явно:

```json
{
  "flight_date": "2026-06-11"
}
```

После запуска в `team_vdga_pipeline_dag` должны стать зелёными задачи:

```text
start
run_stg
run_dds
run_dm
finish
```

Если `run_stg` упал, нужно открыть `team_vdga_stg_dag`.

Если `run_dds` упал, нужно открыть `team_vdga_dds_dag`.

Если `run_dm` упал, нужно открыть `team_vdga_dm_dag`.

Ошибка будет в дочернем DAG-е.

## Проверки после общего запуска

Сначала проверьте вход в PostgreSQL:

```bash
PGCONNECT_TIMEOUT=5 PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -P pager=off \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "select current_database(), current_user, now();"
```

Проверьте STG:

```bash
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -P pager=off \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "
select
    count(distinct source_file) as source_files_cnt,
    count(*) as rows_cnt,
    min(flight_dt) as min_flight_dt,
    max(flight_dt) as max_flight_dt
from team_vdga_stg.flights_raw;
"
```

Проверьте, что STG не добавил дубли:

```bash
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -P pager=off \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "
select
    flow_name,
    source_file,
    count(*) as success_rows_cnt
from team_vdga_metadata.etl_load_log
where flow_name = 'stg_flights_raw'
  and status = 'success'
group by flow_name, source_file
having count(*) > 1
order by source_file;
"
```

Проверьте таблицы команды:

```bash
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -P pager=off \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" <<'SQL'
select
    'team_vdga_stg.flights_raw' as table_name,
    count(*) as rows_cnt
from team_vdga_stg.flights_raw

union all

select
    'team_vdga_dds.flights_performed' as table_name,
    count(*) as rows_cnt
from team_vdga_dds.flights_performed

union all

select
    'team_vdga_dds.flights_cancelled' as table_name,
    count(*) as rows_cnt
from team_vdga_dds.flights_cancelled

union all

select
    'team_vdga_dm.flight_delays' as table_name,
    count(*) as rows_cnt
from team_vdga_dm.flight_delays

union all

select
    'team_vdga_dm.flight_cancellations' as table_name,
    count(*) as rows_cnt
from team_vdga_dm.flight_cancellations;
SQL
```

## Результат последней проверки

После запуска `team_vdga_pipeline_dag` Airflow показал зелёный run.

Jupyter увидел PostgreSQL:

```text
current_database = dwh_training
current_user = student_dwh
```

STG показал:

```text
source_files_cnt = 103
rows_cnt = 2005449
min_flight_dt = 2024-03-01
max_flight_dt = 2024-06-11
```

Запрос на дубли успешных загрузок вернул:

```text
0 rows
```

DDS показал строки:

```text
team_vdga_dds.airports_timezones = 6284
team_vdga_dds.flights_cancelled = 19692
team_vdga_dds.flights_performed = 1966672
```

DM показал строки:

```text
team_vdga_dm.flight_cancellations = 15133
team_vdga_dm.flight_delays = 1054416
```

DM показал диапазон дат:

```text
team_vdga_dm.flight_cancellations: 2024-03-01 — 2024-06-12
team_vdga_dm.flight_delays: 2024-02-29 — 2024-06-12
```

DM может иметь даты чуть шире STG, потому что DDS собирает локальное время из даты, времени рейса и таймзоны. Часть рейсов может перейти на соседний календарный день.

## Что важно помнить дальше

STG уже можно использовать как вход для DDS и DM.

Новые слои должны читать PostgreSQL внутри учебной инфраструктуры, а не исходные `.csv.gz` напрямую.

Дату рейса нужно брать из `flight_dt`.

Дата в пути S3 показывает техническую папку файла, а не дату рейса.

STG не заполняет `timezone`, потому что текущий источник аэропортов не содержит это поле.

Airflow использует connection `edu_dwh_postgres`. Jupyter читает переменные из `.env`. Оба окружения должны смотреть в одну базу:

```text
dwh_training
```

Если следующие слои содержат фильтры только на первые три даты, эти фильтры нужно убрать.

Для полного запуска проекта можно запускать `team_vdga_pipeline_dag`. Он сам запустит STG, потом DDS, потом DM.

## Ограничения

STG обрабатывает все `.csv.gz` файлы внутри папки за дату.

Справочник аэропортов не обновляет отдельные строки. Если таблица уже содержит данные, скрипт пропускает повторный запуск. Для учебного проекта этого достаточно.

STG не заполняет поле `timezone`, потому что текущий `ourairports.csv` не содержит это поле.

Исторический backfill запускается вручную из Jupyter.

Ежедневный STG-режим идёт через Airflow.

Полный командный запуск идёт через `team_vdga_pipeline_dag`.
