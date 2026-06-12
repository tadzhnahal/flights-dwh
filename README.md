# Часть №1 проекта. STG слой и загрузка данных

<img width="2879" height="1249" alt="изображение" src="https://github.com/user-attachments/assets/7ddd0c2a-5037-45d9-90f8-550345702e9a" />

## Описание

Эта часть проекта закрывает первый слой хранилища данных. STG слой берёт исходные данные, кладёт их в PostgreSQL и даёт команде основу для DDS и витрин.

В этой части используются два источника. Первый источник хранит рейсы в S3 в формате `.csv.gz`. Второй источник хранит справочник аэропортов в файле `ourairports.csv`.

Airflow запускает пайплайн. PostgreSQL хранит результат. Jupyter помогает проверить данные SQL-запросами.

## Что сделано

1. Созданы схемы `team_vdga_stg` и `team_vdga_metadata`.
2. Создана таблица рейсов `team_vdga_stg.flights_raw`.
3. Создана таблица аэропортов `team_vdga_stg.airports`.
4. Создан технический лог `team_vdga_metadata.etl_load_log`.
5. Написан загрузчик справочника аэропортов.
6. Написан загрузчик рейсов из S3.
7. Добавлена защита от повторной загрузки файлов.
8. Собран Airflow DAG `team_vdga_stg_dag`.
9. Добавлены STG проверки после загрузки.
10. Загрузчик рейсов ищет все `.csv.gz` внутри папки за дату.
11. DAG принимает дату через параметр `flight_date`.
12. DAG берёт дату прошлого Airflow-интервала для планового запуска.
13. Исторические даты из S3 догружены в `dwh_training`.
14. Результат проверен из Jupyter через PostgreSQL.

## Где лежат данные

Основные таблицы:

```text
team_vdga_stg.flights_raw
team_vdga_stg.airports
team_vdga_metadata.etl_load_log
```

`team_vdga_stg.flights_raw` хранит рейсы. Таблица даёт основу для работы с перелётами, задержками и отменами.

`team_vdga_stg.airports` хранит справочник аэропортов. Таблица помогает связать коды аэропортов из рейсов с названиями, странами, регионами и координатами.

`team_vdga_metadata.etl_load_log` хранит лог файлов. По нему видно, какой файл прошёл через загрузчик, сколько строк попало в STG и какой статус получил запуск.

## База данных

Команда работает с базой:

```text
POSTGRES_HOST=postgres_edu
POSTGRES_PORT=5432
POSTGRES_DB=dwh_training
POSTGRES_USER=student_dwh
```

Файл `.env.example` хранит шаблон без паролей. Реальный `.env` не кладём в Git.

## Источник рейсов

Рейсы лежат в S3:

```text
s3://gsbdwhdata/flights_us_data/<flight_date>/*.csv.gz
```

Пример:

```text
s3://gsbdwhdata/flights_us_data/2026-06-11/flights_2026-06-11.csv.gz
```

Дата в папке S3 показывает техническую дату файла. Дата рейса лежит внутри файла в поле `flightdate`. В STG эта дата попадает в поле `flight_dt`.

Для аналитики нужно брать `flight_dt`, а не дату из пути S3.

## Инкрементальная логика

Один запуск грузит одну техническую дату из S3.

Скрипт `scripts/load_flights_raw.py` получает дату через аргумент:

```bash
python scripts/load_flights_raw.py --flight-date 2026-06-11 --load-to-postgres
```

Скрипт ищет все `.csv.gz` в папке:

```text
flights_us_data/2026-06-11/
```

Перед записью скрипт смотрит в `team_vdga_metadata.etl_load_log`.

Если файл уже имеет статус `success`, скрипт пропускает файл и не пишет дубликаты в `team_vdga_stg.flights_raw`.

## Backfill старых дат

Если в S3 уже лежит много старых дат, их можно догрузить из Jupyter.

Сначала нужно получить список дат в файл `available_flight_dates.txt`.

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

Backfill нужен для истории. Ежедневный режим должен идти через Airflow.

## Текущий объём STG

На момент проверки в `team_vdga_stg.flights_raw` лежат:

```text
source_files_cnt = 103
rows_cnt = 2005449
min_flight_dt = 2024-03-01
max_flight_dt = 2024-06-11
```

Эти числа могут измениться после новых загрузок. Поэтому актуальный объём лучше проверять SQL-запросом.

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

Лог загрузки рейсов можно проверить так:

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

## Как подключиться к PostgreSQL из Jupyter

Чтобы читать STG-таблицы, не нужно клонировать репозиторий. Данные лежат не в Git, а в PostgreSQL. Для проверки данных достаточно зайти в базу из Jupyter.

Репозиторий нужен тем, кто продолжает проект в общем коде: добавляет SQL, DAG, dbt-модели, скрипты или правит README.

### Вариант №1. Подключиться без клона проекта

Откройте Jupyter Terminal и задайте переменные подключения к PostgreSQL:

```bash
export POSTGRES_HOST="postgres_edu"
export POSTGRES_PORT="5432"
export POSTGRES_DB="dwh_training"
export POSTGRES_USER="student_dwh"
export POSTGRES_PASSWORD="sql"
```

Пароль не выводите в терминал и не отправляйте в общий чат.

Зайдите в `psql`:

```bash
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -P pager=off \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB"
```

Если подключение прошло, появится приглашение:

```text
dwh_training=#
```

После этого терминал уже находится внутри PostgreSQL. Вставляйте только SQL-запросы, без `PGPASSWORD` и без команды `psql`.

Проверьте, что STG-данные видны:

```sql
select
    count(distinct source_file) as source_files_cnt,
    count(*) as rows_cnt,
    min(flight_dt) as min_flight_dt,
    max(flight_dt) as max_flight_dt
from team_vdga_stg.flights_raw;
```

Если запрос вернул строки, значит база выбрана верно и поверх STG можно строить следующие слои.

Для выхода из `psql` выполните:

```sql
\q
```

### Вариант №2. Подключиться после клона проекта

Этот вариант нужен для работы в общем репозитории.

Склонируйте проект через HTTPS:

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

Файл `.env` не лежит в Git. Создайте его локально в папке проекта или возьмите актуальные значения у команды:

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

Проверьте, что переменные появились. Пароль не выводите:

```bash
echo "POSTGRES_HOST=$POSTGRES_HOST"
echo "POSTGRES_PORT=$POSTGRES_PORT"
echo "POSTGRES_DB=$POSTGRES_DB"
echo "POSTGRES_USER=$POSTGRES_USER"
```

Зайдите в `psql`:

```bash
PGPASSWORD="$POSTGRES_PASSWORD" psql \
  -P pager=off \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB"
```

Внутри `psql` выполните проверочный запрос:

```sql
select
    count(distinct source_file) as source_files_cnt,
    count(*) as rows_cnt,
    min(flight_dt) as min_flight_dt,
    max(flight_dt) as max_flight_dt
from team_vdga_stg.flights_raw;
```

Для выхода из `psql` выполните:

```sql
\q
```

### Частая ошибка

Если выполнить SQL прямо в Jupyter Terminal, Bash выдаст ошибки вроде `command not found`. Это значит, что запрос вставили не туда. Сначала нужно зайти в `psql`, дождаться приглашения `dwh_training=#`, и только потом вставлять SQL.

Если `psql` пытается подключиться через локальный сокет `/var/run/postgresql/.s.PGSQL.5432`, значит переменные окружения не подгрузились. Проверьте `POSTGRES_HOST` и `POSTGRES_PORT`, а потом снова зайдите в `psql`.

## Текущая структура проекта

```text
.
├── dags
│   └── team_vdga_stg_dag.py
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
│   └── 04_create_stg_flights_raw.sql
├── .airflowignore
├── .env.example
└── README.md
```

Папка `sql` хранит SQL для схем и таблиц. Папка `scripts` хранит загрузчики, общие функции и проверки. Папка `dags` хранит DAG для Airflow.

## Таблица рейсов

Таблица `team_vdga_stg.flights_raw` хранит данные по рейсам. В ней есть дата рейса, перевозчик, номер рейса, аэропорт вылета, аэропорт прилёта, задержки, отмены и технические поля.

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

Для даты рейса используем `flight_dt`. Поле `source_file` хранит путь к исходному файлу в S3 и помогает понять, из какого файла пришли строки.

Есть важный нюанс по датам. В S3 файлы лежат в папках вида `2026-06-11`, но внутри файлов даты рейсов относятся к 2024 году. Это не ошибка загрузки. STG хранит бизнес-дату рейса в `flight_dt`, а технический путь к файлу в `source_file`. Поэтому для аналитики берём `flight_dt`, а не дату из пути S3.

В старом описании встречались поля `origin_city_name`, `dest_city_name`, `dep_delay_group_num`, `arr_delay_group_num`, `flights_cnt` и `distance_group_num`. Реальный `.csv.gz` не содержит эти поля, поэтому STG не хранит их напрямую. Города можно получить через справочник аэропортов. Группы задержек и расстояний можно посчитать в следующих слоях. Счётчик рейсов можно задать как один рейс на строку.

## Таблица аэропортов

Таблица `team_vdga_stg.airports` хранит справочник аэропортов из `ourairports.csv`.

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

В структуре таблицы есть поле `timezone`, но сейчас в нём нет значений. В реальном `airports.csv` поля `timezone` нет, поэтому STG не может заполнить его из текущего источника. Если дальше понадобится локальное время, команде нужно взять таймзоны из другого источника или добавить отдельную логику.

## Технический лог

Таблица `team_vdga_metadata.etl_load_log` хранит историю загрузок. Она показывает, какой файл уже прошёл через пайплайн, сколько строк загрузчик положил в STG и была ли ошибка.

Для рейсов используется поток `stg_flights_raw`. Загрузчик контролирует повторный запуск через `source_file`. Если файл уже имеет успешную запись в логе, загрузчик не добавляет его строки ещё раз.

Для справочника аэропортов тоже есть простая защита. Если таблица `team_vdga_stg.airports` уже содержит строки, загрузчик пропускает повторную загрузку.

## Контракт для следующих слоёв

Дальше можно опираться на две таблицы:

```text
team_vdga_stg.flights_raw
team_vdga_stg.airports
```

Рейсы связываем со справочником аэропортов через IATA коды:

```text
flights_raw.origin_code соответствует airports.iata_code
flights_raw.dest_code соответствует airports.iata_code
```

Дата рейса лежит в `flights_raw.flight_dt`.

Технический путь к файлу лежит в `flights_raw.source_file`.

Если дальше понадобятся города аэропортов, их можно взять из справочника аэропортов. Если понадобятся группы задержек, группы расстояний или счётчик рейсов, их лучше посчитать уже в следующих слоях. В реальном файле рейсов этих полей нет, поэтому STG не добавляет их как отдельные поля.

## Airflow

DAG называется `team_vdga_stg_dag`.

DAG лежит в файле:

```text
dags/team_vdga_stg_dag.py
```

DAG выполняет задачи в таком порядке:

1. `start`
2. `create_schemas`
3. `create_load_log_table`
4. `create_airports_table`
5. `create_flights_raw_table`
6. `load_airports`
7. `load_flights_raw`
8. `check_stg_quality`
9. `finish`

DAG работает по расписанию:

```text
0 6 * * *
```

Один плановый запуск грузит одну дату. Если дату не передать вручную, DAG берёт дату прошлого Airflow-интервала.

DAG также поддерживает ручной запуск с параметром `flight_date`.

Пример JSON для ручного запуска:

```json
{
  "flight_date": "2026-06-11"
}
```

Если передать дату, DAG загрузит файлы из папки:

```text
flights_us_data/<flight_date>/*.csv.gz
```

Если файл уже имеет статус `success` в `team_vdga_metadata.etl_load_log`, загрузчик пропустит его и не добавит дубликаты.

Для работы DAG нужны два connection в Airflow:

```text
edu_dwh_postgres
team_vdga_s3
```

`edu_dwh_postgres` хранит доступ к PostgreSQL. `team_vdga_s3` хранит доступ к S3.

Команде не нужно заходить в Airflow, чтобы читать данные. Airflow нужен для запуска STG, проверки зелёного run и просмотра логов.

## Проверки

После backfill я проверил результат из Jupyter через PostgreSQL:

1. PostgreSQL отвечает из Jupyter.
2. Таблица `team_vdga_stg.flights_raw` отдаёт строки.
3. В STG лежит больше трёх `source_file`.
4. В `flight_dt` нет пустых значений.
5. В `source_file` нет пустых значений.
6. В `etl_load_log` есть успешные записи.
7. Число строк в логе совпадает с числом строк в `flights_raw`.
8. Связь рейсов с аэропортами через IATA коды работает.

Проверка связи подтянула названия аэропортов из справочника. Среди них были `Asheville Regional Airport`, `LaGuardia Airport`, `Eppley Airfield` и `Kansas City International Airport`.

## Что важно помнить дальше

STG уже можно использовать как вход для следующих слоёв. Данные нужно читать из PostgreSQL внутри учебной инфраструктуры, например через Jupyter или другой сервис, который видит базу.

Не стоит брать данные напрямую из исходных `.csv.gz`, если задача строится поверх общего хранилища. STG уже хранит загруженный и проверенный слой.

Дату рейса нужно брать из `flight_dt`. Дата в пути S3 описывает техническую папку, а не дату рейса.

Поле `timezone` сейчас пустое. Это важно учесть, если дальше появится логика с локальным временем.

Airflow использует connection `edu_dwh_postgres`. Jupyter читает переменные из `.env`. Оба окружения должны смотреть в одну базу `dwh_training`.

После догрузки новых дат нужно перезапустить DDS, потом витрины. Если в следующих слоях есть фильтры только на первые три даты, их надо убрать.

## Ограничения

Загрузчик рейсов обрабатывает все `.csv.gz` файлы внутри папки за дату.

Справочник аэропортов не обновляет отдельные строки. Если таблица уже содержит данные, загрузчик пропускает повторную загрузку. Для текущего учебного проекта этого достаточно.

STG не заполняет поле `timezone`, потому что текущий источник аэропортов не содержит это поле.

Исторический backfill запускается вручную из Jupyter. Ежедневный режим должен идти через Airflow.
