# Схема данных и 3NF

```mermaid
erDiagram
    AIRLINES ||--o{ FLIGHTS : operates
    FLIGHTS ||--o{ SEAT_RESERVATIONS : reserves
    AIRLINES {
        varchar code PK
        text name
    }
    FLIGHTS {
        uuid id PK
        varchar flight_number "UNIQUE with UTC departure date"
        varchar airline_code FK
        varchar origin "IATA"
        varchar destination "IATA"
        timestamptz departure_time
        timestamptz arrival_time
        int total_seats "greater than zero"
        int available_seats "between zero and total_seats"
        decimal price "RUB, greater than zero"
        enum status "SCHEDULED | DEPARTED | CANCELLED | COMPLETED"
    }
    SEAT_RESERVATIONS {
        uuid id PK
        uuid flight_id FK
        uuid booking_id UK "logical reference to Booking Service"
        int seat_count "greater than zero"
        enum status "ACTIVE | RELEASED | EXPIRED"
        timestamptz created_at
    }
    RELEASE_FENCES {
        uuid booking_id PK
        timestamptz created_at
    }
    CACHE_REVISION {
        int id PK "singleton: 1"
        bigint revision
    }
```

Все таблицы выше принадлежат **Flight PostgreSQL**. Ниже — отдельная **Booking PostgreSQL**:

```mermaid
erDiagram
    BOOKINGS {
        uuid id PK
        text user_id "external opaque identifier"
        uuid flight_id "logical reference to Flight Service"
        text passenger_name
        text passenger_email
        int seat_count "greater than zero"
        decimal total_price "snapshot in RUB"
        enum status "CONFIRMED | CANCELLED"
        timestamptz created_at
    }
    OPERATIONS {
        uuid id PK
        uuid booking_id "may precede a booking"
        text kind "CREATE | CANCEL"
        text state "PENDING | DONE | ABORTED"
        timestamptz created_at
    }
```

`Booking.flight_id → Flight.id`, `SeatReservation.booking_id → Booking.id` — логические связи через контракт. Между БД нет внешних ключей, SQL JOIN или доступа одного приложения к БД другого. `operations.booking_id` не имеет FK: запись CREATE обязана пережить сбой ещё до появления бронирования. Успешному бронированию соответствует ровно одна резервация благодаря UNIQUE(booking_id).

Значения атомарны, все неключевые атрибуты зависят от ключа, транзитивные зависимости не дублируются. Название авиакомпании вынесено в `airlines`. Аэропорты представлены только кодами, без зависимых названий и городов. Номер рейса обозначает повторяющийся рейс, но маршрут и авиакомпания могут меняться по датам; конкретный вылет определяется номером и датой UTC. Дата не дублируется отдельной колонкой: уникальность реализована индексом выражения.

`user_id` — идентификатор заказчика, а `passenger_name`/`passenger_email` — данные пассажира конкретной покупки: заказчик может покупать билеты разным людям. Поэтому зависимости `user_id → passenger_name` нет. `total_price` — историческая сумма, текущая цена рейса в Booking не копируется; отдельной вычисляемой колонки `unit_price` нет.

`available_seats` — требуемый заданием счётчик. Его согласованность с суммой ACTIVE-резерваций поддерживается транзакциями; это межтабличный агрегат, а не транзитивная зависимость неключевых полей одной строки. CHECK защищает диапазон, SELECT FOR UPDATE — конкурентные изменения.

EXPIRED предусмотрен контрактом и схемой. Автоматического истечения оплаченных/подтверждённых резерваций нет: отдельный срок действия ТЗ не задаёт.
