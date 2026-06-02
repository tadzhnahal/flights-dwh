# Часть №1 проекта. STG слой и загрузка данных

<img width="2879" height="1249" alt="изображение" src="https://github.com/user-attachments/assets/7ddd0c2a-5037-45d9-90f8-550345702e9a" />

## Описание

Эта часть проекта закрывает первый слой хранилища данных. Я подготовил STG слой, который принимает исходные данные, кладёт их в PostgreSQL и даёт команде основу для следующих слоёв.

В этой части я работал с двумя источниками. Первый источник хранит рейсы в S3 в формате `.csv.gz`. Второй источник хранит справочник аэропортов в файле `ourairports.csv`.

Результат лежит в PostgreSQL. Airflow запускает пайплайн и показывает логи.

## Что я сделал

1. Создал схемы `team_vdga_stg` и `team_vdga_metadata`.
2. Создал таблицу рейсов `team_vdga_stg.flights_raw`.
3. Создал таблицу аэропортов `team_vdga_stg.airports`.
4. Создал технический лог `team_vdga_metadata.etl_load_log`.
5. Написал загрузчик справочника аэропортов.
6. Написал загрузчик рейсов из S3.
7. Добавил защиту от повторной загрузки.
8. Собрал Airflow DAG `team_vdga_stg_dag`.
9. Добавил STG проверки после загрузки.
10. Научил загрузчик искать все `.csv.gz` внутри папки за дату.
11. Научил DAG принимать дату запуска через параметр `flight_date`.
12. Запустил DAG в Airflow и получил зелёный run.
13. Проверил результат из Jupyter через PostgreSQL.

## Где лежат данные

Основные таблицы:

```text
team_vdga_stg.flights_raw
team_vdga_stg.airports
team_vdga_metadata.etl_load_log
```

`team_vdga_stg.flights_raw` хранит рейсы. Эта таблица даёт основу для работы с перелётами, задержками и отменами.

`team_vdga_stg.airports` хранит справочник аэропортов. Эта таблица помогает связать коды аэропортов из рейсов с названиями, странами, регионами и координатами.

`team_vdga_metadata.etl_load_log` хранит лог загрузки. По нему видно, какой файл прошёл через загрузчик, сколько строк попало в STG и с каким статусом завершился запуск.

Сейчас в PostgreSQL, который видит Jupyter, лежат три технические даты:

```text
flights_us_data/2026-03-01/flights_2026-03-01.csv.gz -> 19157 строк -> flight_dt = 2024-03-01
flights_us_data/2026-03-02/flights_2026-03-02.csv.gz -> 16492 строки -> flight_dt = 2024-03-02
flights_us_data/2026-03-03/flights_2026-03-03.csv.gz -> 19079 строк -> flight_dt = 2024-03-03
```

Всего в `team_vdga_stg.flights_raw` сейчас лежит `54728` строк.

<img width="1381" height="844" alt="изображение" src="https://github.com/user-attachments/assets/b0fd15bf-cb19-4b1a-94cc-c7e91e022edc" />

## Как подключиться к PostgreSQL из Jupyter

Для простого чтения STG таблиц клон этого репозитория не нужен. Достаточно зайти в PostgreSQL из Jupyter и проверить таблицы `team_vdga_stg`.

Если вы продолжаете проект в этом же репозитории, добавляете SQL, DAG-и или общие файлы проекта, склонируйте репозиторий в свой Jupyter.

Через HTTPS:

```bash
cd ~
git clone https://github.com/tadzhnahal/flights-dwh.git
cd ~/flights-dwh
```

Через SSH:

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

Клон репозитория не переносит данные из PostgreSQL. Данные уже лежат в общей базе. Чтобы увидеть те же STG таблицы, нужен локальный `.env` с правильными переменными подключения к PostgreSQL.

Файл `.env` не лежит в Git. Для подключения нужны такие переменные:

```text
POSTGRES_HOST=...
POSTGRES_PORT=...
POSTGRES_DB=...
POSTGRES_USER=...
POSTGRES_PASSWORD=...
```

Пароль не выводите в терминал и не коммитьте в Git.

Если у вас есть `.env`, загрузите переменные:

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

Если `psql` подключился, появится приглашение вида:

```text
dwh=#
```

Внутри `psql` выполните проверочный запрос:

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

Ожидаемый результат:

```text
flights_us_data/2026-03-01/flights_2026-03-01.csv.gz | 19157 | 2024-03-01 | 2024-03-01
flights_us_data/2026-03-02/flights_2026-03-02.csv.gz | 16492 | 2024-03-02 | 2024-03-02
flights_us_data/2026-03-03/flights_2026-03-03.csv.gz | 19079 | 2024-03-03 | 2024-03-03
```

Если вы видите три строки, значит вы подключились к нужной базе и можете строить следующие слои поверх STG.

Для выхода из `psql` выполните:

```sql
\q
```

Если после перезапуска Jupyter `psql` пытается подключиться через локальный сокет `/var/run/postgresql/.s.PGSQL.5432`, значит переменные окружения не подгрузились. В этом случае снова выполните:

```bash
set -a
source .env
set +a
```

## Текущая структура проекта

```text
.
├── dags
│   └── team_vdga_stg_dag.py
├── scripts
│   ├── check_stg_quality.py
│   ├── cleaning.py
│   ├── common.py
│   ├── load_airports.py
│   ├── load_flights_raw.py
│   ├── load_log.py
│   └── run_sql.py
└── sql
    ├── 01_create_schemas.sql
    ├── 02_create_load_log.sql
    ├── 03_create_stg_airports.sql
    └── 04_create_stg_flights_raw.sql
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

Есть важный нюанс по датам. В S3 файлы лежат в папках `2026-03-01`, `2026-03-02` и `2026-03-03`, но внутри файлов даты рейсов равны `2024-03-01`, `2024-03-02` и `2024-03-03`. Это не ошибка загрузки. STG хранит бизнес-дату рейса в `flight_dt`, а технический путь к файлу в `source_file`. Поэтому для аналитики берём `flight_dt`, а не дату из пути S3.

В старом описании встречались поля `origin_city_name`, `dest_city_name`, `dep_delay_group_num`, `arr_delay_group_num`, `flights_cnt` и `distance_group_num`. Я сверился с реальным `.csv.gz` и не нашёл этих полей в источнике, поэтому не добавлял их в STG. Города можно получить через справочник аэропортов, группы задержек и расстояний можно посчитать дальше по модели, а счётчик рейсов можно задать как один рейс на строку.

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

Главное поле для связи с рейсами это `iata_code`. В таблице рейсов ему соответствуют `origin_code` и `dest_code`.

В структуре таблицы есть поле `timezone`, но сейчас в нём нет значений. Я оставил это поле заранее, потому что оно может понадобиться для работы с локальным временем. В реальном `airports.csv` поля `timezone` нет, поэтому STG не может заполнить его из текущего источника. Если дальше понадобится локальное время, команде нужно будет взять таймзоны из другого источника или добавить отдельную логику.

## Технический лог

Таблица `team_vdga_metadata.etl_load_log` хранит историю загрузок. Она помогает понять, какой файл уже прошёл через пайплайн, сколько строк загрузчик положил в STG и была ли ошибка.

Для рейсов я использовал поток `stg_flights_raw`. Загрузчик контролирует повторный запуск через `source_file`. Если файл уже имеет успешную запись в логе, загрузчик не добавляет его строки ещё раз.

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

Если дальше понадобятся города аэропортов, их можно взять из справочника аэропортов. Если понадобятся группы задержек, группы расстояний или счётчик рейсов, их лучше посчитать уже в следующих слоях. В реальном файле рейсов этих полей нет, поэтому я не добавлял их в STG.

## Airflow

DAG называется `team_vdga_stg_dag`. Я загрузил его в Airflow и запустил вручную.

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

DAG можно оставить включённым. У него нет расписания. В Airflow видно `Schedule: None` и `Next Run ID: None`, поэтому он не стартует сам и не создаст новые загрузки без ручного запуска.

DAG поддерживает ручной запуск с параметром `flight_date`. Если при запуске передать дату, DAG загрузит файлы из папки:

```text
flights_us_data/<flight_date>/*.csv.gz
```

Если дату не передать, DAG возьмёт значение по умолчанию:

```text
2026-03-01
```

Для работы DAG нужны два connection в Airflow:

```text
edu_dwh_postgres
team_vdga_s3
```

`edu_dwh_postgres` хранит доступ к PostgreSQL. `team_vdga_s3` хранит доступ к S3.

Команде не нужно заходить в Airflow, чтобы читать данные. Airflow нужен для ручного перезапуска STG, проверки зелёного run и просмотра логов.

## Проверки

После запуска я проверил результат из Jupyter через PostgreSQL:

1. PostgreSQL отвечает из Jupyter.
2. Таблица `team_vdga_stg.flights_raw` отдаёт строки.
3. В STG лежат три `source_file` за технические даты `2026-03-01`, `2026-03-02` и `2026-03-03`.
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

Airflow и Jupyter берут доступы к PostgreSQL из разных мест. Airflow использует соединение `edu_dwh_postgres`, а Jupyter читает переменные из `.env`. На случай, если вы будете перезапускать свои DAG-и через Airflow и потом проверять результат из Jupyter, нужно убедиться, что оба окружения смотрят в один и тот же PostgreSQL. Для текущей передачи данные уже лежат в PostgreSQL, который видит Jupyter.

## Ограничения

В текущих папках `2026-03-01`, `2026-03-02` и `2026-03-03` лежит по одному `.csv.gz` файлу. При этом загрузчик уже умеет обрабатывать все `.csv.gz` файлы внутри папки за дату.

Справочник аэропортов не обновляет отдельные строки. Если таблица уже содержит данные, загрузчик пропускает повторную загрузку. Для текущего учебного проекта этого достаточно.
