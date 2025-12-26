import asyncio
import aiohttp
import asyncpg
import os

# --- ИМПОРТ КОНФИГУРАЦИИ ---
try:
    from Tokens import access_token
    from config import user, password, db_name, host
except ImportError:
    print("❌ Ошибка: Не найдены файлы config.py или Tokens.py")
    exit(1)

# --- НАСТРОЙКИ ---
ACCESS_TOKEN = access_token
DB_DSN = f"postgresql://{user}:{password}@{host}/{db_name}"

# Ограничение скорости (3-5 одновременных запросов безопасно для сервисного ключа)
CONCURRENT_REQUESTS = 4


async def get_all_group_ids(pool: asyncpg.Pool) -> list:
    """Получает все ID групп из базы данных."""
    query = "SELECT id FROM groups ORDER BY id"
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [row['id'] for row in rows]
    except Exception as e:
        print(f"❌ Ошибка чтения БД: {e}")
        return []


async def check_group_access(session: aiohttp.ClientSession, group_id: int, semaphore: asyncio.Semaphore) -> int | None:
    """
    Проверяет доступ к участникам группы.
    Возвращает group_id, если доступ ЗАКРЫТ.
    Возвращает None, если доступ ОТКРЫТ.
    """
    url = "https://api.vk.com/method/groups.getMembers"

    # Запрашиваем 0 участников. Нам важен только факт успешного ответа или ошибки.
    params = {
        "group_id": group_id,
        "count": 0,
        "access_token": ACCESS_TOKEN,
        "v": "5.131"
    }

    async with semaphore:
        try:
            async with session.get(url, params=params) as resp:
                data = await resp.json()

                # Искусственная задержка, чтобы не превысить лимит (3 запроса в секунду)
                await asyncio.sleep(0.35)

                if 'error' in data:
                    error_code = data['error']['error_code']
                    error_msg = data['error']['error_msg']

                    # Коды ошибок доступа:
                    # 15 = Access denied (группа закрыта или вы в ЧС)
                    # 30 = This profile is private
                    # 203 = Access to group denied
                    if error_code in [15, 30, 203]:
                        print(f"🔒 Группа {group_id}: Доступ закрыт ({error_msg})")
                        return group_id
                    elif error_code == 6:
                        print(f"⚠️ Группа {group_id}: Слишком много запросов! (Rate Limit)")
                        # Можно добавить логику повтора, но пока просто вернем как есть
                        return None
                    else:
                        print(f"❓ Группа {group_id}: Странная ошибка {error_code} - {error_msg}")
                        return group_id  # Считаем недоступной

                # Если ответ успешный ('response' in data)
                print(f"✅ Группа {group_id}: Доступ открыт")
                return None

        except Exception as e:
            print(f"❌ Сетевая ошибка {group_id}: {e}")
            return None


async def main():
    print(f"🔌 Подключение к БД: {db_name}...")

    try:
        pool = await asyncpg.create_pool(DB_DSN)
    except Exception as e:
        print(f"🛑 Ошибка подключения: {e}")
        return

    # 1. Берем все группы из БД
    all_groups = await get_all_group_ids(pool)
    print(f"📋 Всего групп в базе для проверки: {len(all_groups)}")

    if not all_groups:
        await pool.close()
        return

    # 2. Проверяем доступ
    closed_groups_list = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for gid in all_groups:
            task = check_group_access(session, gid, semaphore)
            tasks.append(task)

        # Запускаем и ждем результаты
        results = await asyncio.gather(*tasks)

        # Фильтруем None (доступные) и оставляем только ID недоступных
        closed_groups_list = [gid for gid in results if gid is not None]

    # 3. Вывод результата
    print("\n" + "=" * 50)
    print(f"🚫 МАССИВ ГРУПП, ОТКУДА НЕЛЬЗЯ ПОЛУЧИТЬ ПОДПИСЧИКОВ ({len(closed_groups_list)} шт):")
    print("=" * 50)
    print(closed_groups_list)
    print("=" * 50)

    # Опционально: можно сохранить этот массив в файл
    # with open("banned_groups.txt", "w") as f:
    #     f.write(str(closed_groups_list))

    await pool.close()


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())