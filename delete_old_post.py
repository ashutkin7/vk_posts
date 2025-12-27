import psycopg2
from config import user, password, db_name, host

DB_CONFIG = {
    "dbname": db_name,
    "user": user,
    "password": password,
    "host": host
}


def delete_old_posts(months=6):
    """
    Удаляет посты, которые старше, чем указанное количество месяцев.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print(f"🧹 Начинаю очистку постов старше {months} месяцев...")

        # SQL-запрос на удаление
        # NOW() - текущее время
        # INTERVAL '%s month' - вычитаем нужное кол-во месяцев
        query = """
        DELETE FROM posts 
        WHERE date < NOW() - INTERVAL '%s month';
        """

        # Выполняем запрос
        cur.execute(query % int(months))

        # Получаем количество удаленных строк
        deleted_count = cur.rowcount

        # Фиксируем изменения в базе
        conn.commit()

        print(f"✅ Успешно удалено постов: {deleted_count}")

        # --- ОПЦИОНАЛЬНО: Очистка сиротливых комментариев ---
        # Если у тебя нет FOREIGN KEY с ON DELETE CASCADE,
        # комментарии к удаленным постам останутся в базе "мусором".
        # Можно удалить и их:
        if deleted_count > 0:
            print("🧹 Удаляю комментарии, привязанные к удаленным постам...")
            cur.execute("""
                DELETE FROM comments 
                WHERE post_id NOT IN (SELECT id FROM posts);
            """)
            deleted_comments = cur.rowcount
            conn.commit()
            print(f"✅ Удалено сиротливых комментариев: {deleted_comments}")

    except Exception as e:
        print(f"❌ Ошибка при удалении: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    # Удаляем всё, что старше полгода
    delete_old_posts(months=6)