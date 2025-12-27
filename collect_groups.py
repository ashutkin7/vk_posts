import vk
import time
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
# Импорты конфигов (убедись, что файлы существуют)
from Tokens import access_token
from config import user, password, db_name, host

# --- КОНФИГУРАЦИЯ ---
ACCESS_TOKEN = access_token
DB_CONFIG = {
    "dbname": db_name,
    "user": user,
    "password": password,
    "host": host
}

# Твой список ID групп
GROUP_IDS = [81846282, 34273295, 170221583, 70148184, 227835995, 191819870, 83521634, 70803593, 81442958, 136333481, 184973499, 230688957, 225061063, 104763609, 67131627, 59715826, 48236789, 219246838, 163901702, 60410126, 12538150, 122855719, 61899062, 66332986, 76532043, 159045989, 73408869, 37067111, 89373035, 154493292, 108427660, 11991443, 220252566, 168393141, 114612681, 126994890, 71952907, 21142038, 225851929, 127525404, 219073069, 172524137, 99482223, 12845709, 131461996, 62272176, 173230769, 226065079, 101493480, 20349674, 203627249, 190978808, 144999176, 138312461, 142568208, 78060310, 105720611, 79999780, 226960172, 223247159, 126221119, 77925185, 43076442, 72078171, 169827212, 211698594, 91505577, 138163113, 157512627, 154649529, 61918157, 50535375, 178158547, 183096280, 215481310, 116913122, 155663336, 10746870, 158209030, 36017175, 228197400, 175107101, 135160883, 58221641, 158741643, 86250640, 186524835, 23266468, 122477745, 158729403, 100269268, 152683760, 225469693, 195634436, 230350099, 56519960, 198176029, 220089666, 53290319, 127688018, 68302164, 116112735, 141213024, 116481406, 64296329, 155524537, 135755197, 133588423, 87260650, 17321503, 93892129, 65338914, 154361383, 183680554, 91156015, 52110908, 226704966, 109631066, 129646177, 154809962, 40601221, 49065607, 88790680, 57476778, 227178159, 138069703, 66043597, 82142959, 82781966, 115906329, 25790249, 61910836, 194744127, 96814938, 144949088, 65398647, 84539264, 114896770, 143456143, 53768104, 101003192, 170184635, 79898562, 21952469, 167174115]


# Инициализация API
api = vk.API(access_token=access_token, v='5.131')


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def split_list(lst, n):
    """Разбивает список на куски по n элементов"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def parse_vk_date(date_str):
    """
    VK отдает start_date как 'YYYYMMDD' (строка) или timestamp.
    Postgres DATE требует 'YYYY-MM-DD'.
    """
    if not date_str:
        return None

    # Если это число (timestamp)
    if isinstance(date_str, int):
        return datetime.fromtimestamp(date_str).strftime('%Y-%m-%d')

    # Если строка YYYYMMDD
    if len(str(date_str)) == 8:
        try:
            d = str(date_str)
            return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
        except:
            return None
    return None


def get_max_cover_url(cover_obj):
    """Достает ссылку на самое большое изображение обложки"""
    if not cover_obj or not cover_obj.get('enabled'):
        return None
    images = cover_obj.get('images', [])
    if not images:
        return None
    # Сортируем по ширине и берем последнее (самое большое)
    try:
        sorted_imgs = sorted(images, key=lambda x: x.get('width', 0))
        return sorted_imgs[-1].get('url')
    except:
        return None


def prepare_group_data(group):
    """
    Подготавливает один объект группы для вставки в SQL.
    Извлекает вложенные поля, counters и т.д.
    """

    # Счетчики
    counters = group.get('counters', {})

    # Обложка
    cover_url = get_max_cover_url(group.get('cover'))

    # Дата основания
    start_date = parse_vk_date(group.get('start_date'))

    return (
        group['id'],
        group.get('name', '')[:255],  # Обрезаем на всякий случай
        group.get('screen_name'),
        group.get('type'),
        group.get('is_closed'),
        group.get('deactivated'),  # banned, deleted или None
        group.get('members_count', 0),
        group.get('description', ''),
        group.get('status', ''),
        group.get('site', ''),
        bool(group.get('verified', 0)),  # Превращаем 0/1 в False/True
        group.get('age_limits', 1),
        group.get('city', {}).get('id'),  # Извлекаем ID
        group.get('country', {}).get('id'),  # Извлекаем ID
        start_date,
        bool(group.get('market', {}).get('enabled', 0)),  # Рынок
        counters.get('photos', 0),
        counters.get('videos', 0),
        counters.get('topics', 0),
        counters.get('docs', 0),
        cover_url,
        group.get('photo_200') or group.get('photo_max'),  # Аватарка
        datetime.now()  # scraped_at
    )


# --- ФУНКЦИИ VK ---
def get_groups_from_vk(group_ids):
    """
    Получает расширенную информацию о группах для новой структуры БД.
    """
    all_groups = []

    # Поля, которые нужно запросить
    fields = (
        "members_count,"
        "description,"
        "status,"
        "site,"
        "verified,"
        "age_limits,"
        "city,"
        "country,"
        "start_date,"
        "market,"
        "counters,"
        "cover"
    )

    batches = split_list(group_ids, 500)

    for batch in batches:
        print(f"🔄 Запрашиваю инфо о {len(batch)} группах...")
        try:
            response = api.groups.getById(
                group_ids=",".join(map(str, batch)),
                fields=fields
            )

            # Обрабатываем каждую группу сразу
            processed_batch = [prepare_group_data(g) for g in response]
            all_groups.extend(processed_batch)

            time.sleep(0.35)

        except Exception as e:
            print(f"⚠️ Ошибка при запросе к VK: {e}")

    return all_groups


# --- ФУНКЦИИ БД ---
def upsert_groups(groups_data):
    """
    Вставляет данные в таблицу groups со всеми новыми полями.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # SQL запрос с новыми полями
    insert_query = """
    INSERT INTO groups (
        id, name, screen_name, type, is_closed, deactivated, 
        members_count, description, status, site, verified,
        age_limits, city_id, country_id, start_date, market_enabled,
        counters_photos, counters_videos, counters_topics, counters_docs,
        cover_url, photo_max_url, scraped_at
    ) VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        screen_name = EXCLUDED.screen_name,
        type = EXCLUDED.type,
        is_closed = EXCLUDED.is_closed,
        deactivated = EXCLUDED.deactivated,
        members_count = EXCLUDED.members_count,
        description = EXCLUDED.description,
        status = EXCLUDED.status,
        site = EXCLUDED.site,
        verified = EXCLUDED.verified,
        age_limits = EXCLUDED.age_limits,
        city_id = EXCLUDED.city_id,
        country_id = EXCLUDED.country_id,
        start_date = EXCLUDED.start_date,
        market_enabled = EXCLUDED.market_enabled,
        counters_photos = EXCLUDED.counters_photos,
        counters_videos = EXCLUDED.counters_videos,
        counters_topics = EXCLUDED.counters_topics,
        counters_docs = EXCLUDED.counters_docs,
        cover_url = EXCLUDED.cover_url,
        photo_max_url = EXCLUDED.photo_max_url,
        scraped_at = EXCLUDED.scraped_at;
    """

    try:
        execute_values(cursor, insert_query, groups_data)
        conn.commit()
        print(f"✅ Успешно сохранено/обновлено групп: {len(groups_data)}")
    except Exception as e:
        print(f"❌ Ошибка записи в БД: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# --- ГЛАВНЫЙ ЗАПУСК ---
if __name__ == "__main__":

    # Пример получения ID из БД (если нужно будет в будущем):
    def get_ids_from_db():
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM groups") # или другая логика
        ids = [row[0] for row in cur.fetchall()]
        conn.close()
        return ids

    target_ids = get_ids_from_db()

    print(target_ids)

    if not target_ids:
        print("Список ID пуст.")
    else:
        print(f"🚀 Начинаю обработку {len(target_ids)} групп.")

        # 1. Получаем и обрабатываем данные
        processed_data = get_groups_from_vk(target_ids)

        # 2. Сохраняем в БД
        if processed_data:
            upsert_groups(processed_data)
        else:
            print("Данные не получены.")