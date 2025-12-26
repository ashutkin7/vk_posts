import psycopg2
from config import user, password, db_name, host

# --- НАСТРОЙКИ ПОДКЛЮЧЕНИЯ ---
DB_CONFIG = {
    "dbname": db_name,
    "user": user,
    "password": password,
    "host": host
}

# Список ID для удаления
non_food_ids = [76742095, 107210348, 124477391, 200408423, 223055180]


def delete_groups_from_db():
    conn = None
    try:
        # 1. Подключение
        print("🔌 Подключаюсь к базе данных...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Проверка, не пуст ли список
        if not non_food_ids:
            print("⚠️ Список ID пуст, удалять нечего.")
            return

        # 2. Формирование запроса
        # Используем конструкцию IN %s. Важно передать список как кортеж (tuple)
        query = "DELETE FROM groups WHERE id IN %s"

        print(f"🗑 Удаляю {len(non_food_ids)} групп...")

        # 3. Выполнение
        cur.execute(query, (tuple(non_food_ids),))
        deleted_count = cur.rowcount  # Сколько строк реально удалилось

        # 4. Фиксация изменений
        conn.commit()

        print(f"✅ Готово! Удалено записей из таблицы groups: {deleted_count}")
        if deleted_count < len(non_food_ids):
            print(f"ℹ️ (Запрошено на удаление {len(non_food_ids)}, но некоторых ID в базе не было).")

    except Exception as e:
        print(f"❌ Ошибка при удалении: {e}")
        if conn:
            conn.rollback()  # Откат изменений в случае ошибки
    finally:
        if conn:
            cur.close()
            conn.close()
            print("🔌 Соединение закрыто.")


if __name__ == "__main__":
    delete_groups_from_db()
