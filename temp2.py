import psycopg2
from config import user, password, db_name, host

# --- КОНФИГУРАЦИЯ ---
DB_CONFIG = {
    "dbname": db_name,
    "user": user,
    "password": password,
    "host": host
}

TABLE_NAME = "users_detailed"

def drop_unwanted_columns():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        print(f"🛠 Удаление колонок из таблицы {TABLE_NAME}...")

        # SQL запрос на удаление нескольких колонок сразу
        # IF EXISTS предотвращает ошибку, если колонки уже удалены
        query = f"""
            ALTER TABLE {TABLE_NAME}
            DROP COLUMN IF EXISTS universities_json,
            DROP COLUMN IF EXISTS pages_count,
            DROP COLUMN IF EXISTS friends_count;
        """

        cur.execute(query)
        conn.commit()

        print("✅ Колонки успешно удалены (universities_json, pages_count, friends_count).")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    drop_unwanted_columns()