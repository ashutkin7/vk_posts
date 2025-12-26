import asyncio
import asyncpg
import csv
import os

# --- ИМПОРТ КОНФИГУРАЦИИ ---
try:
    from config import user, password, db_name, host
except ImportError:
    print("❌ Ошибка: Не найден файл config.py")
    exit(1)

# Формируем строку подключения
DB_DSN = f"postgresql://{user}:{password}@{host}/{db_name}"


async def print_general_stats(pool):
    """Выводит общую статистику по базе."""
    print("\n📊 --- ОБЩАЯ СТАТИСТИКА ---")

    async with pool.acquire() as conn:
        # 1. Общее количество пользователей
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
        print(f"👥 Всего пользователей в базе: {total_users}")

        # 2. Количество подписок (связей)
        total_subs = await conn.fetchval("SELECT COUNT(*) FROM subscriptions")
        print(f"🔗 Всего связей (подписок): {total_subs}")

        # 3. Распределение по полу
        print("\n🚹🚺 Распределение по полу:")
        sex_rows = await conn.fetch("SELECT sex, COUNT(*) as cnt FROM users GROUP BY sex ORDER BY cnt DESC")
        sex_map = {1: "Женский", 2: "Мужской", 0: "Не указан"}
        for row in sex_rows:
            sex_name = sex_map.get(row['sex'], "Неизвестно")
            print(f"   - {sex_name}: {row['cnt']}")

        # 4. Топ-5 Городов
        print("\n🏙️ Топ-5 городов:")
        city_rows = await conn.fetch("""
            SELECT city_title, COUNT(*) as cnt 
            FROM users 
            WHERE city_title IS NOT NULL 
            GROUP BY city_title 
            ORDER BY cnt DESC 
            LIMIT 5
        """)
        for row in city_rows:
            print(f"   - {row['city_title']}: {row['cnt']}")


async def print_last_users(pool, limit=10):
    """Выводит информацию о последних добавленных пользователях."""
    print(f"\n🆕 --- ПОСЛЕДНИЕ {limit} ДОБАВЛЕННЫХ ПОЛЬЗОВАТЕЛЕЙ ---")

    query = """
        SELECT id, first_name, last_name, city_title, bdate, status 
        FROM users 
        ORDER BY last_seen DESC NULLS LAST 
        LIMIT $1
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, limit)

        # Заголовок таблицы
        print(f"{'ID':<12} | {'Имя Фамилия':<30} | {'Город':<20} | {'Дата рожд.'}")
        print("-" * 80)

        for row in rows:
            full_name = f"{row['first_name']} {row['last_name']}"
            city = row['city_title'] if row['city_title'] else "Нет данных"
            bdate = str(row['bdate']) if row['bdate'] else "-"

            print(f"{row['id']:<12} | {full_name:<30} | {city:<20} | {bdate}")


async def export_csv(pool):
    """Экспортирует данные в CSV файл (опционально)."""
    filename = "users_export.csv"
    print(f"\n💾 Экспорт данных в файл {filename}...")

    async with pool.acquire() as conn:
        # Получаем данные с курсором для экономии памяти
        rows = await conn.fetch("SELECT id, first_name, last_name, city_title, has_mobile, site FROM users LIMIT 1000")

        if not rows:
            print("Нет данных для экспорта.")
            return

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Заголовки (берем из ключей первой записи)
            writer.writerow(rows[0].keys())
            # Данные
            for row in rows:
                writer.writerow(row.values())

    print("✅ Экспорт завершен.")


async def main():
    print(f"🔌 Подключение к БД {db_name}...")
    try:
        pool = await asyncpg.create_pool(DB_DSN)
    except Exception as e:
        print(f"🛑 Ошибка подключения: {e}")
        return

    # Запуск функций анализа
    await print_general_stats(pool)
    await print_last_users(pool, limit=10)

    # Раскомментируйте строчку ниже, если хотите сохранить данные в файл
    # await export_csv(pool)

    await pool.close()
    print("\n🏁 Проверка завершена.")


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())