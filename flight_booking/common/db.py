import os
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


def create_pool():
    pool = ConnectionPool(
        os.environ["DATABASE_URL"], min_size=2, max_size=16,
        kwargs={"row_factory": dict_row, "options": "-c statement_timeout=15000 -c lock_timeout=10000"},
        timeout=5,
    )
    pool.wait(timeout=30)
    return pool
