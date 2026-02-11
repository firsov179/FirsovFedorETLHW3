import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

POSTGRES_CONFIG = {
    'host': 'postgres',
    'database': 'iot_data',
    'user': 'airflow',
    'password': 'airflow',
    'port': 5432
}


def get_postgres_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def load_to_postgres(table_name, data_df):
    if data_df.empty:
        print(f"Нет данных для загрузки в таблицу {table_name}")
        return 0

    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        values = [(row['date_only'], float(row['temp'])) for _, row in data_df.iterrows()]

        insert_query = sql.SQL("""
            INSERT INTO {} (date_only, temp)
            VALUES %s
            ON CONFLICT (date_only)
            DO UPDATE SET
                temp = EXCLUDED.temp,
                updated_at = CURRENT_TIMESTAMP
        """).format(sql.Identifier(table_name))

        execute_values(cursor, insert_query, values)
        rows_affected = cursor.rowcount
        conn.commit()
        print(f"Успешно загружено/обновлено {rows_affected} записей в таблицу {table_name}")

        return rows_affected
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке данных в {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
