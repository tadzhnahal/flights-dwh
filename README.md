# Часть №1 проекта. STG слой и общий запуск пайплайна

<img width="2879" height="1249" alt="изображение" src="https://github.com/user-attachments/assets/7ddd0c2a-5037-45d9-90f8-550345702e9a" />

## Описание

Эта часть проекта отвечает за первый слой хранилища данных. STG слой принимает исходные файлы, приводит поля к единому виду и кладёт строки в PostgreSQL. Следующие слои уже не читают сырые `.csv.gz` напрямую. Они берут данные из STG и строят DDS и витрины.

Проект использует два источника. Первый источник хранит рейсы в S3 в формате `.csv.gz`. Второй источник хранит справочник аэропортов в файле `ourairports.csv`. Airflow запускает пайплайн, PostgreSQL хранит таблицы, а Jupyter помогает проверить результат SQL-запросами.

После доработки в проекте есть отдельные DAG-и по слоям и общий DAG-оркестратор. Отдельные DAG-и сохраняют зоны ответственности, а оркестратор запускает всю цепочку `STG -> DDS -> DM` одной кнопкой.

## Что входит в мою часть

STG часть создаёт схемы `team_vdga_stg` и `team_vdga_metadata`, таблицу рейсов `team_vdga_stg.flights_raw`, таблицу аэропортов `team_vdga_stg.airports` и технический лог `team_vdga_metadata.etl_load_log`.

Python-скрипты загружают справочник аэропортов и рейсы из S3. После загрузки проверки смотрят, что данные появились, ключевые поля не пустые, а лог совпадает с фактическими строками в STG.

Загрузчик рейсов работает инкрементально. Он берёт дату запуска, ищет все `.csv.gz` файлы в папке S3 за эту дату и перед вставкой строк проверяет `etl_load_log` по `source_file`. Если файл уже имеет статус `success`, скрипт пропускает его и не создаёт дубли в `team_vdga_stg.flights_raw`.

Для истории был выполнен backfill. После него STG содержит 103 исходных файла и 2 005 449 строк. Диапазон бизнес-дат в поле `flight_dt` идёт с `2024-03-01` по `2024-06-11`.

## Основные таблицы

Проект использует три командные схемы:

```text
team_vdga_stg
team_vdga_dds
team_vdga_dm
```

STG слой хранит исходные данные и технический лог:

```text
team_vdga_stg.flights_raw
team_vdga_stg.airports
team_vdga_metadata.etl_load_log
```

DDS слой хранит обработанные таблицы для модели хранилища:

```text
team_vdga_dds.airports_timezones
team_vdga_dds.flights_cancelled
team_vdga_dds.flights_performed
```

DM слой хранит витрины для анализа:

```text
team_vdga_dm.flight_cancellations
team_vdga_dm.flight_delays
```

`team_vdga_stg.flights_raw` хранит рейсы. `team_vdga_stg.airports` хранит справочник аэропортов. `team_vdga_metadata.etl_load_log` хранит историю файлов и помогает скриптам не грузить один и тот же файл повторно.

## База данных и секреты

Команда работает с базой `dwh_training`:

```text
POSTGRES_HOST=postgres_edu
POSTGRES_PORT=5432
POSTGRES_DB=dwh_training
POSTGRES_USER=student_dwh
```

Файл `.env.example` хранит только шаблон переменных без секретов. Файл `.env` хранит реальные значения для локального запуска и Jupyter. Его нельзя класть в Git и нельзя отправлять в общий чат.

Airflow не хранит пароли в Python-файлах проекта. DAG-и берут доступы из Airflow Connections:

```text
edu_dwh_postgres
team_vdga_s3
```

`edu_dwh_postgres` даёт доступ к PostgreSQL. `team_vdga_s3` даёт доступ к S3. В коде DAG лежат только имена connections, а не пароли.

## Источник рейсов

Рейсы лежат в S3 по такому пути:

```text
s3://gsbdwhdata/flights_us_data/<flight_date>/*.csv.gz
```

Пример файла:

```text
s3://gsbdwhdata/flights_us_data/2026-06-11/flights_2026-06-11.csv.gz
```

Дата в пути S3 показывает техническую дату папки. Дата рейса лежит внутри файла в поле `flightdate`. STG кладёт эту дату в поле `flight_dt`.

Поэтому для аналитики нужно брать `flight_dt`, а не дату из пути S3. В S3 папка может называться `2026-06-11`, но строки внутри файла могут относиться к рейсам за 2024 год. Это не ошибка загрузки, а особенность исходного датасета.

## Как STG грузит рейсы

Скрипт `scripts/load_flights_raw.py` грузит одну техническую дату за один запуск. Дату он получает через аргумент `--flight-date`.

Пример запуска:

```bash
python scripts/load_flights_raw.py --flight-date 2026-06-11 --load-to-postgres
```

Для даты `2026-06-11` скрипт читает папку:

```text
flights_us_data/2026-06-11/
```

Внутри этой папки скрипт берёт все файлы с расширением `.csv.gz`. Затем он читает строки, проверяет обязательные поля, приводит значения к нужным типам и готовит строки для вставки в `team_vdga_stg.flights_raw`.

Перед вставкой скрипт проверяет таблицу `team_vdga_metadata.etl_load_log`. Если файл уже имеет строку со статусом `success`, скрипт пропускает файл. Так повторный запуск не добавляет дубли.

## Как STG грузит аэропорты

Скрипт `scripts/load_airports.py` скачивает `ourairports.csv`, читает строки и кладёт их в таблицу:

```text
team_vdga_stg.airports
```

Если таблица уже содержит строки, скрипт пропускает повторный запуск. Для текущего учебного проекта этого достаточно, потому что справочник нужен как стабильная таблица для связей.

Главное поле для связи рейсов с аэропортами — `iata_code`. В таблице рейсов ему соответствуют поля `origin_code` и `dest_code`.

## Backfill старых дат

Backfill нужен, если в S3 уже лежит много дат, а STG ещё не содержит эти файлы. Для этого сначала формируется файл `available_flight_dates.txt` со списком технических дат, а затем скрипты проходят по датам циклом.

Команда backfill:

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

Backfill нужен для исторической догрузки. Ежедневный режим должен идти через Airflow. Повторный запуск backfill не должен создавать дубли, потому что загрузчик проверяет `etl_load_log` по `source_file`.

## Как зайти в PostgreSQL из Jupyter

Для чтения таблиц не нужно клонировать репозиторий. Данные лежат в PostgreSQL, поэтому достаточно открыть Jupyter Terminal и задать переменные подключения.

Задайте переменные:

```bash
export POSTGRES_HOST="postgres_edu"
export POSTGRES_PORT="5432"
export POSTGRES_DB="dwh_training"
export POSTGRES_USER="student_dwh"
export POSTGRES_PASSWORD="sql"
```

Пароль не выводите в терминал и не отправляйте в чат. Его нужно использовать только в командах подключения.

Сначала проверьте, что Jupyter видит PostgreSQL:

```bash
PGCONNECT_TIMEOUT=5 PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -P pager=off \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "select current_database(), current_user, now();"
```

Хороший результат содержит базу `dwh_training` и пользователя `student_dwh`.

После этого зайдите в интерактивный `psql`:

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

Дальше вставляйте только SQL-запросы. Команды `PGPASSWORD=... psql ...` внутри `psql` вставлять не нужно.

Для выхода выполните:

```sql
\q
```

Если вставить SQL прямо в обычный терминал, Bash выдаст ошибки вроде `command not found`. В этом случае сначала зайдите в `psql`, дождитесь приглашения `dwh_training=#`, а затем вставьте SQL.

## Контрольные проверки STG

Этот блок выполняется внутри `psql`. Сначала зайдите в базу по команде из предыдущего раздела, дождитесь приглашения `dwh_training=#`, а затем выполняйте SQL-запросы ниже.

### Проверка подключения

```sql
select
    current_database() as database_name,
    current_user as user_name,
    now() as check_dttm;
```

Этот запрос показывает, что подключение идёт к нужной базе и под нужным пользователем.

### Текущий объём STG

На момент проверки таблица `team_vdga_stg.flights_raw` содержит:

```text
source_files_cnt = 103
rows_cnt = 2005449
min_flight_dt = 2024-03-01
max_flight_dt = 2024-06-11
```

Эти числа могут измениться после новых запусков. Поэтому README фиксирует текущую контрольную проверку, но актуальный объём нужно смотреть SQL-запросом:

```sql
select
    count(distinct source_file) as source_files_cnt,
    count(*) as rows_cnt,
    min(flight_dt) as min_flight_dt,
    max(flight_dt) as max_flight_dt
from team_vdga_stg.flights_raw;
```

Этот запрос показывает общий объём STG, число исходных файлов и диапазон бизнес-дат.

### Дубли по source_file

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

Хороший результат — пустой вывод с `0 rows`. Это значит, что повторный запуск не создал дубли успешной загрузки.

### Лог STG

```sql
select
    status,
    count(*) as log_rows_cnt,
    sum(rows_loaded) as rows_loaded_sum,
    min(flight_dt) as min_flight_dt,
    max(flight_dt) as max_flight_dt
from team_vdga_metadata.etl_load_log
where flow_name = 'stg_flights_raw'
group by status
order by status;
```

Этот запрос показывает, сколько файлов прошли успешно и сколько строк скрипт записал в лог.

### Основные STG-таблицы

```sql
select
    'team_vdga_stg.flights_raw' as table_name,
    count(*) as rows_cnt
from team_vdga_stg.flights_raw

union all

select
    'team_vdga_stg.airports' as table_name,
    count(*) as rows_cnt
from team_vdga_stg.airports;
```

Этот запрос показывает, что таблица рейсов и справочник аэропортов существуют и содержат строки.

### Связь рейсов со справочником аэропортов

```sql
select
    f.origin_code,
    ao.name as origin_airport_name,
    f.dest_code,
    ad.name as dest_airport_name,
    count(*) as flights_cnt
from team_vdga_stg.flights_raw f
left join team_vdga_stg.airports ao
    on f.origin_code = ao.iata_code
left join team_vdga_stg.airports ad
    on f.dest_code = ad.iata_code
group by
    f.origin_code,
    ao.name,
    f.dest_code,
    ad.name
order by flights_cnt desc
limit 10;
```

Запрос показывает, что `origin_code` и `dest_code` из рейсов можно связать с `iata_code` из справочника аэропортов.

### Важные поля для следующих слоёв

```sql
select
    count(*) as rows_without_flight_dt
from team_vdga_stg.flights_raw
where flight_dt is null;

select
    count(*) as rows_without_source_file
from team_vdga_stg.flights_raw
where source_file is null;
```

Хороший результат — нули. `flight_dt` нужен следующим слоям для работы с датами. `source_file` нужен для контроля происхождения строк и защиты от дублей.

## Как работать после клона проекта

Этот вариант нужен тем, кто меняет код проекта. Склонируйте репозиторий через HTTPS:

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

Папка `dags` хранит Airflow DAG-и. Папка `scripts` хранит Python-скрипты для STG. Папка `sql` хранит SQL для схем и таблиц. Папка `dbt` хранит модели DDS и DM.

## Таблица рейсов

Таблица `team_vdga_stg.flights_raw` хранит сырые рейсы. В ней есть дата рейса, перевозчик, номер рейса, аэропорт вылета, аэропорт прилёта, задержки, отмены и технические поля.

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

Для даты рейса нужно брать `flight_dt`. Поле `source_file` хранит путь к исходному файлу в S3. В S3 файлы лежат в папках вида `2026-06-11`, но внутри файлов даты рейсов относятся к 2024 году. STG хранит бизнес-дату в `flight_dt`, а технический путь — в `source_file`.

В старом описании встречались поля `origin_city_name`, `dest_city_name`, `dep_delay_group_num`, `arr_delay_group_num`, `flights_cnt` и `distance_group_num`. Реальный `.csv.gz` не содержит эти поля. Поэтому STG не хранит их напрямую. Города можно взять из справочника аэропортов, группы задержек и расстояний можно посчитать в DDS или DM, а счётчик рейсов можно задать как один рейс на строку.

## Таблица аэропортов

Таблица `team_vdga_stg.airports` хранит справочник из `ourairports.csv`. В ней есть коды аэропортов, названия, типы, страны, регионы, координаты и техническое поле загрузки.

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

Главное поле для связи с рейсами — `iata_code`. В таблице рейсов ему соответствуют `origin_code` и `dest_code`.

В таблице есть поле `timezone`, но текущий `airports.csv` не содержит таймзоны. Поэтому STG оставляет `timezone` пустым. Если следующему слою нужны таймзоны, команда должна взять их из другого источника. Сейчас DDS использует отдельную таблицу `team_vdga_dds.airports_timezones`.

## Технический лог

Таблица `team_vdga_metadata.etl_load_log` хранит историю файлов. Лог содержит имя потока, путь к файлу, дату рейса, число строк, статус, текст ошибки и время записи.

Ключевые поля:

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

Для рейсов используется поток `stg_flights_raw`. Скрипт смотрит в лог перед вставкой строк. Если файл уже имеет статус `success`, скрипт пропускает файл. В таблице есть уникальный индекс для успешных файлов, поэтому один и тот же `source_file` не должен получить два успешных лога.

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

Дата рейса лежит в `flights_raw.flight_dt`. Путь к исходному файлу лежит в `flights_raw.source_file`. DDS берёт STG, добавляет справочник таймзон и строит таблицы выполненных и отменённых рейсов. DM берёт DDS и строит витрины для анализа задержек и отмен.

## STG DAG

STG DAG называется `team_vdga_stg_dag`. Он лежит в файле:

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

Один плановый запуск грузит одну дату. Если дату не передать вручную, DAG берёт дату прошлого Airflow-интервала. При ручном запуске можно передать дату через JSON:

```json
{
  "flight_date": "2026-06-11"
}
```

Если файл уже есть в `etl_load_log` со статусом `success`, загрузчик пропустит файл и не добавит дубли.

## DDS DAG

DDS DAG называется `team_vdga_dds_dag`. Он лежит в файле:

```text
dags/team_vdga_dds_dag.py
```

DAG создаёт DDS-схему, грузит таблицу таймзон аэропортов и запускает dbt-модели:

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

DM DAG называется `team_vdga_dm_dag`. Он лежит в файле:

```text
dags/team_vdga_dm_dag.py
```

DAG запускает dbt-модели витрин:

```text
flight_delays
flight_cancellations
```

Основные DM-таблицы:

```text
team_vdga_dm.flight_delays
team_vdga_dm.flight_cancellations
```

## Общий DAG-оркестратор

Общий DAG называется `team_vdga_pipeline_dag`. Он лежит в файле:

```text
dags/team_vdga_pipeline_dag.py
```

Оркестратор запускает три DAG-а по порядку:

```text
team_vdga_stg_dag -> team_vdga_dds_dag -> team_vdga_dm_dag
```

Этот DAG не содержит SQL, Python-загрузчиков и dbt-моделей. Он только запускает готовые DAG-и и ждёт, пока каждый слой завершится со статусом `success`.

Такой подход сохраняет отдельные зоны ответственности. STG отвечает за исходные данные, DDS отвечает за обработанный слой, DM отвечает за витрины. При этом у команды появляется один общий запуск полного пайплайна.

## Как запустить полный пайплайн

Откройте Airflow и найдите DAG `team_vdga_pipeline_dag`. Нажмите trigger. Если интерфейс не даёт передать JSON, можно запустить DAG без параметров: в DAG есть запасная дата `2026-06-11`.

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

Если упала задача `run_stg`, нужно открыть `team_vdga_stg_dag`. Если упала задача `run_dds`, нужно открыть `team_vdga_dds_dag`. Если упала задача `run_dm`, нужно открыть `team_vdga_dm_dag`. Ошибка будет в дочернем DAG-е, потому что оркестратор только ждёт результат.

## Проверка после общего запуска

Сначала зайдите в PostgreSQL из Jupyter:

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

Затем выполните SQL-запрос внутри `psql`:

```sql
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
```

Эта проверка показывает, что все слои команды существуют и содержат строки.

## Результат последней проверки

После запуска `team_vdga_pipeline_dag` Airflow показал зелёный run. Jupyter увидел PostgreSQL:

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

Запрос на дубли успешных загрузок вернул `0 rows`.

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

DM показал такой диапазон дат:

```text
team_vdga_dm.flight_cancellations: 2024-03-01 — 2024-06-12
team_vdga_dm.flight_delays: 2024-02-29 — 2024-06-12
```

DM может иметь даты чуть шире STG, потому что DDS собирает локальное время из даты рейса, времени рейса и таймзоны. Часть рейсов может перейти на соседний календарный день.

## Что учитывать дальше

Следующие слои должны читать данные из PostgreSQL внутри учебной инфраструктуры, а не из исходных `.csv.gz` напрямую. STG уже хранит загруженный и проверенный слой, поэтому DDS и DM должны опираться на `team_vdga_stg.flights_raw` и `team_vdga_stg.airports`.

Для аналитики нужно брать дату из `flight_dt`. Дата в пути S3 показывает техническую папку файла, а не дату рейса. Поле `timezone` в STG сейчас пустое, потому что текущий `ourairports.csv` не содержит таймзоны. Для локального времени DDS использует отдельный справочник таймзон.

Если следующие слои содержат фильтры только на первые три даты, эти фильтры нужно убрать. После backfill STG хранит больше данных, и DDS/DM должны работать по всему доступному диапазону.

Для полного запуска проекта можно запускать `team_vdga_pipeline_dag`. Он запустит STG, потом DDS, потом DM.

## Ограничения

STG обрабатывает все `.csv.gz` файлы внутри папки за дату. Справочник аэропортов не обновляет отдельные строки: если таблица уже содержит данные, скрипт пропускает повторный запуск. Для учебного проекта этого достаточно.

STG не заполняет поле `timezone`, потому что текущий `ourairports.csv` не содержит это поле. Исторический backfill запускается вручную из Jupyter. Ежедневный STG-режим идёт через Airflow. Полный командный запуск идёт через `team_vdga_pipeline_dag`.
