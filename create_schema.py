from bd_connect import get_connection


def create_structure():
    conn = get_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            # 1. Создание отдельной схемы для статьи по кластеризации
            cursor.execute("CREATE SCHEMA IF NOT EXISTS clustering_article;")
            print("✅ Схема 'clustering_article' готова.")

            # Устанавливаем эту схему по умолчанию для всех последующих команд в сессии
            cursor.execute("SET search_path TO clustering_article;")

            # 2. SQL-запрос на создание всех таблиц
            sql_create_tables = """
            -- 1. Таблица групп
            CREATE TABLE IF NOT EXISTS groups (
                id BIGINT PRIMARY KEY,
                name TEXT,
                screen_name TEXT,
                type TEXT,
                is_closed SMALLINT,
                deactivated TEXT,
                members_count INT,
                description TEXT,
                status TEXT,
                site TEXT,
                verified BOOLEAN,
                age_limits SMALLINT,
                city_id INT,
                country_id INT,
                start_date DATE,
                market_enabled BOOLEAN,
                counters_photos INT,
                counters_videos INT,
                counters_topics INT,
                counters_docs INT,
                cover_url TEXT,
                photo_max_url TEXT,
                scraped_at TIMESTAMP
            );

            -- 2. Таблица пользователей
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                maiden_name TEXT,
                screen_name TEXT,
                sex SMALLINT,
                bdate TEXT,
                city_id INT,
                country_id INT,
                home_town TEXT,
                relation SMALLINT,
                relation_partner_id BIGINT,
                about TEXT,
                activities TEXT,
                interests TEXT,
                music TEXT,
                movies TEXT,
                tv TEXT,
                books TEXT,
                games TEXT,
                quotes TEXT,
                personal_political SMALLINT,
                personal_alcohol SMALLINT,
                personal_smoking SMALLINT,
                personal_religion TEXT,
                personal_people_main SMALLINT,
                personal_life_main SMALLINT,
                occupation_type TEXT,
                occupation_name TEXT,
                site TEXT,
                status TEXT,
                verified BOOLEAN,
                followers_count INT,
                friends_count INT,
                pages_count INT,
                last_seen TIMESTAMP,
                platform INT,
                photo_max_url TEXT,
                is_closed BOOLEAN,
                deactivated TEXT,
                career_json JSONB,
                universities_json JSONB,
                schools_json JSONB,
                relatives_json JSONB,
                scraped_at TIMESTAMP
            );

            -- 3. Таблица подписок (с внешними ключами)
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
                group_id BIGINT REFERENCES groups(id) ON DELETE CASCADE,
                scraped_at TIMESTAMP
            );

            -- 4. Таблица постов (с составным первичным ключом)
            CREATE TABLE IF NOT EXISTS posts (
                id BIGINT,
                owner_id BIGINT,
                owner_type TEXT,
                from_id BIGINT,
                text TEXT,
                date TIMESTAMP,
                post_type TEXT,
                is_pinned BOOLEAN,
                marked_as_ads BOOLEAN,
                likes_count INT,
                views_count INT,
                reposts_count INT,
                comments_count INT,
                is_copy BOOLEAN,
                copy_owner_id BIGINT,
                copy_post_id BIGINT,
                copy_text TEXT,
                has_photo BOOLEAN,
                has_video BOOLEAN,
                has_audio BOOLEAN,
                has_link BOOLEAN,
                attachments JSONB,
                scraped_at TIMESTAMP,
                PRIMARY KEY (owner_id, id)
            );

            -- 5. Таблица комментариев
            CREATE TABLE IF NOT EXISTS comments (
                id BIGINT PRIMARY KEY,
                owner_id BIGINT,
                post_id BIGINT,
                from_id BIGINT,
                text TEXT,
                date TIMESTAMP,
                reply_to_user BIGINT,
                reply_to_comment BIGINT,
                likes_count INT,
                scraped_at TIMESTAMP
            );
            """

            # Выполняем SQL-скрипт
            cursor.execute(sql_create_tables)
            print("✅ Все таблицы (groups, users, subscriptions, posts, comments) успешно созданы.")

    except Exception as _ex:
        print("❌ Ошибка при создании структуры БД:", _ex)
    finally:
        conn.close()
        print("🔒 Подключение закрыто.")


if __name__ == "__main__":
    create_structure()