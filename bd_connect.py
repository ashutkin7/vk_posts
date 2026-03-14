import psycopg2


def get_connection():
    """Устанавливает подключение и сразу переключает контекст на нужную схему."""
    try:
        # Указываем схему по умолчанию через параметр options
        # -c search_path=имя_схемы
        connection = psycopg2.connect(
            host="176.108.251.88",
            port="5432",
            user="neuro",
            password="neuro",
            database="neuro",
            options="-c search_path=clustering_article"
        )

        connection.autocommit = True
        return connection

    except Exception as _ex:
        print("❌ Ошибка подключения к базе:", _ex)
        return None


if __name__ == "__main__":
    conn = get_connection()
    if conn:
        with conn.cursor() as cursor:
            # Проверяем текущую схему
            cursor.execute("SHOW search_path;")
            current_schema = cursor.fetchone()[0]
            print(f"✅ Подключено! Текущая схема по умолчанию: {current_schema}")

            # Проверка доступа к таблице из readme
            try:
                cursor.execute("SELECT COUNT(*) FROM users;")
                print(f"📊 Таблица 'users' доступна в этой схеме.")
            except:
                print("⚠️ Схема выбрана, но таблицы еще не созданы.")

        conn.close()