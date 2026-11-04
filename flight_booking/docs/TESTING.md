# Проверка задания

## Автоматический прогон

```bash
docker compose up --build -d
docker compose --profile test run --rm tests
```

Тесты создают отдельные рейсы с уникальными номерами; seed не расходуется. Тестовые бронирования, журнал и cancellation fences остаются для просмотра. Только `docker compose down -v` полностью удаляет данные стенда.

## Проверка транзакций через SQL

Открыть БД рейсов:

```bash
docker compose exec flight-db psql -U flights -d flights
```

```sql
-- Должно вернуть 0 строк: остаток и активные резервы всегда согласованы.
SELECT f.id, f.total_seats, f.available_seats,
       COALESCE(sum(r.seat_count) FILTER (WHERE r.status='ACTIVE'), 0) AS reserved
FROM flights f
LEFT JOIN seat_reservations r ON r.flight_id=f.id
GROUP BY f.id
HAVING f.total_seats - f.available_seats <>
       COALESCE(sum(r.seat_count) FILTER (WHERE r.status='ACTIVE'), 0);

-- Должно вернуть 0 строк (дополнительно обеспечено UNIQUE).
SELECT booking_id, count(*) FROM seat_reservations
GROUP BY booking_id HAVING count(*) > 1;
```

Журнал Booking:

```bash
docker compose exec booking-db psql -U bookings -d bookings \
  -c 'SELECT kind,state,count(*) FROM operations GROUP BY kind,state ORDER BY kind,state'
```

После восстановления зависимостей старые PENDING должны перейти в DONE либо ABORTED. Сумма бронирования не меняется при UpdateFlight.

## Retry и Circuit Breaker на настоящих контейнерах

Все три сценария отказа ниже можно выполнить автоматически (Python 3, Docker Compose):

```bash
python3 scripts/check_failures.py
```

Скрипт временно останавливает контейнеры **этого** проекта, проверяет ответы и возвращает сервисы в работу в `finally`. Запускать на свободном учебном стенде, без параллельных запросов/тестов. Если параметры Circuit Breaker изменены, передать скрипту те же переменные окружения. Результат выводится в JSON.

```bash
docker compose stop flight-service
for i in 1 2 3 4 5 6; do
  curl -s -w '\nHTTP %{http_code}, %{time_total}s\n' \
    http://localhost:8080/flights/11111111-1111-4111-8111-111111111111
done
docker compose logs --since 2m booking-service
```

Первые пять логических вызовов содержат максимум три попытки; интервалы 100/200 мс видны в логах. Шестой запрос получает 503 без сетевого вызова, в сообщении есть `circuit is OPEN`. Лог: `CLOSED -> OPEN`.

```bash
docker compose start flight-service
sleep 16
curl -i http://localhost:8080/flights/11111111-1111-4111-8111-111111111111
docker compose logs --since 2m booking-service
```

Ожидается 200 и переходы `OPEN -> HALF_OPEN -> CLOSED`. При другой настройке CB_RESET_SECONDS ждать нужно дольше этого значения. Остановка Flight не мешает получить уже сохранённое бронирование через `GET /bookings/{id}`.

Тест `test_wire_retry.py` отдельно моделирует потерю ответа **после реального COMMIT резерва**: первый RPC возвращает UNAVAILABLE или DEADLINE_EXCEEDED, второй передаёт тот же booking_id, места списаны ровно один раз.

## Redis Sentinel failover

Определить текущего master, не предполагая, что он всё ещё `redis-master`:

```bash
docker compose exec sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name flight-cache
docker compose exec redis-master redis-cli ROLE
docker compose exec redis-replica redis-cli ROLE
```

На свежем стенде master — `redis-master`. Остановить **контейнер, который сейчас master**:

```bash
docker compose stop redis-master
```

В течение примерно 5–20 секунд Sentinel должен выбрать `redis-replica`. Пока происходит переключение, REST продолжает получать данные из PostgreSQL:

```bash
curl -i 'http://localhost:8080/flights?origin=SVO&destination=LED'
docker compose exec sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name flight-cache
docker compose exec redis-replica redis-cli ROLE
docker compose logs --since 2m flight-service sentinel-1
```

Вернуть бывший master:

```bash
docker compose start redis-master
```

Sentinel переведёт его в replica нового master. Проверьте ROLE после нескольких секунд. Не останавливайте одновременно оба Redis и все Sentinel в этом сценарии: проверяется одиночный отказ. У Redis асинхронная репликация, поэтому часть кеш-записей может потеряться; источник данных — PostgreSQL.

## Недоступность всего кеша

```bash
docker compose stop redis-master redis-replica
curl -i 'http://localhost:8080/flights?origin=SVO&destination=LED'
docker compose start redis-master redis-replica
```

REST должен ответить 200, логи — `cache unavailable; read PostgreSQL`. После восстановления кеш снова заполняется. Эта проверка не требует пересоздавать или удалять volumes.

## TTL и hit/miss

Дважды вызвать один GetFlight или SearchFlights. На первом чтении после мутации — miss, затем hit. На текущем master:

```bash
docker compose exec redis-master redis-cli --scan --pattern 'flight:*'
```

Подставить найденный ключ в `redis-cli TTL '<key>'`: ожидается от 1 до 300 (до 600, если изменена настройка), никогда -1. Если уже произошло переключение, выполнить команду на `redis-replica`.

## Проверка миграций

Повторный `docker compose up` не пересоздаёт таблицы и seed; Flyway хранит версии и контрольные суммы в `flyway_schema_history` каждой БД. История должна содержать успешные V1/V2 для Flight и V1 для Booking.
