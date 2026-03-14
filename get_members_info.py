import csv
import os
import psycopg2
from psycopg2.extras import RealDictCursor  # Для работы с именами колонок

# Импортируем вашу функцию подключения
from bd_connect import get_connection


def print_general_stats(conn):
    """Выводит общую статистику по базе."""
    print("\n📊 --- ОБЩАЯ СТАТИСТИКА ---")

    # Используем RealDictCursor, чтобы row['column_name'] работал
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. Общее количество пользователей
        cur.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()['count']
        print(f"👥 Всего пользователей в базе: {total_users}")

        # 2. Количество подписок (связей)
        cur.execute("SELECT COUNT(*) FROM subscriptions")
        total_subs = cur.fetchone()['count']
        print(f"🔗 Всего связей (подписок): {total_subs}")

        # 3. Распределение по полу
        print("\n🚹🚺 Распределение по полу:")
        cur.execute("SELECT sex, COUNT(*) as cnt FROM users GROUP BY sex ORDER BY cnt DESC")
        sex_rows = cur.fetchall()
        sex_map = {1: "Женский", 2: "Мужской", 0: "Не указан"}
        for row in sex_rows:
            sex_name = sex_map.get(row['sex'], "Неизвестно")
            print(f"   - {sex_name}: {row['cnt']}")

        # 4. Топ-5 Городов
        print("\n🏙️ Топ-5 городов:")
        cur.execute("""
            SELECT city_title, COUNT(*) as cnt 
            FROM users 
            WHERE city_title IS NOT NULL 
            GROUP BY city_title 
            ORDER BY cnt DESC 
            LIMIT 5
        """)
        for row in cur.fetchall():
            print(f"   - {row['city_title']}: {row['cnt']}")


def print_last_users(conn, limit=10):
    """Выводит информацию о последних добавленных пользователях."""
    print(f"\n🆕 --- ПОСЛЕДНИЕ {limit} ДОБАВЛЕННЫХ ПОЛЬЗОВАТЕЛЕЙ ---")

    query = """
        SELECT id, first_name, last_name, city_title, bdate, status 
        FROM users 
        ORDER BY last_seen DESC NULLS LAST 
        LIMIT %s
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (limit,))
        rows = cur.fetchall()

        print(f"{'ID':<12} | {'Имя Фамилия':<30} | {'Город':<20} | {'Дата рожд.'}")
        print("-" * 80)

        for row in rows:
            full_name = f"{row['first_name']} {row['last_name']}"
            city = row['city_title'] if row['city_title'] else "Нет данных"
            bdate = str(row['bdate']) if row['bdate'] else "-"
            print(f"{row['id']:<12} | {full_name:<30} | {city:<20} | {bdate}")


def export_csv(conn):
    """Экспортирует данные в CSV файл."""
    filename = "users_export.csv"
    print(f"\n💾 Экспорт данных в файл {filename}...")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, first_name, last_name, city_title, has_mobile, site FROM users LIMIT 1000")
        rows = cur.fetchall()

        if not rows:
            print("Нет данных для экспорта.")
            return

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            # Используем DictWriter, так как у нас список словарей
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    print("✅ Экспорт завершен.")


def main():
    print("🔌 Подключение к БД...")
    conn = get_connection()

    if conn is None:
        return

    try:
        # Запуск функций анализа
        print_general_stats(conn)
        print_last_users(conn, limit=10)

        # Раскомментируйте для экспорта
        # export_csv(conn)

    except Exception as e:
        print(f"🛑 Ошибка при выполнении запросов: {e}")
    finally:
        conn.close()
        print("\n🏁 Проверка завершена. Соединение закрыто.")


if __name__ == "__main__":
    main()