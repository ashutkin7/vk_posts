import psycopg2
from config import user, password, db_name, host

# --- КОНФИГУРАЦИЯ ---
DB_CONFIG = {
    "dbname": db_name,
    "user": user,
    "password": password,
    "host": host
}


def filter_full_profiles():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        print("🛠 1. Создаю таблицу new_users (если нет)...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS new_users (
                LIKE users INCLUDING ALL
            );
        """)
        conn.commit()

        # Формируем SQL запрос с условиями для КАЖДОГО поля из вашего списка
        # updated_at > NOW() - INTERVAL '50 hours' означает "свежее, чем 50 часов назад"

        filter_sql = """
            WHERE 
                -- Зеленые (обязательные технические)
                first_name IS NOT NULL AND first_name != ''
                AND last_name IS NOT NULL AND last_name != ''
                AND domain IS NOT NULL AND domain != ''
                AND sex IS NOT NULL
                AND has_mobile IS NOT NULL
                AND has_photo IS NOT NULL
                AND friends_count IS NOT NULL
                AND followers_count IS NOT NULL
                AND platform IS NOT NULL
                AND last_seen IS NOT NULL

                -- Желтые (Гео и Личные данные)
                AND city_title IS NOT NULL AND city_title != ''
                AND country_id IS NOT NULL
                AND bdate IS NOT NULL
                AND status IS NOT NULL AND status != ''

                -- Красные (Самый жесткий фильтр)
                AND site IS NOT NULL AND site != ''

                -- Временной фильтр (Свежие данные)
                AND updated_at > NOW() - INTERVAL '50 hours'
        """

        # 2. Считаем, сколько таких идеальных пользователей
        print("📊 2. Анализирую количество подходящих пользователей...")
        count_query = f"SELECT COUNT(*) FROM users {filter_sql}"
        cur.execute(count_query)
        count = cur.fetchone()[0]

        print(f"   🔎 Найдено пользователей: {count:,}")

        if count == 0:
            print("   ⚠️ Никто не прошел фильтр. Попробуйте убрать условие по 'site' или 'status'.")
        else:
            # 3. Перенос
            print(f"🚀 3. Копирую {count:,} пользователей в new_users...")
            insert_query = f"""
                INSERT INTO new_users 
                SELECT * FROM users 
                {filter_sql}
                ON CONFLICT (id) DO NOTHING;
            """
            cur.execute(insert_query)
            inserted = cur.rowcount
            conn.commit()
            print(f"✅ Успешно скопировано: {inserted:,}")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    filter_full_profiles()