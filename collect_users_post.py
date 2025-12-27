import vk
import time
import json
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from config import user, password, db_name, host, token

# --- КОНФИГУРАЦИЯ ---
DB_CONFIG = {
    "dbname": db_name,
    "user": user,
    "password": password,
    "host": host
}

# --- НАСТРОЙКИ ---
DAYS_TO_SCRAPE = 183  # Полгода
MAX_USERS_PER_BATCH = 25  # Максимальный размер пачки (начнем с него)
POSTS_PER_USER = 100  # Постов с юзера

BATCH_SIZE = 5000  # Буфер БД
VK_SLEEP = 0.35

STATE_FILE = "user_posts_state.json"
DEBUG = True


class VkUserPostScraper:
    def __init__(self, token):
        self.api = vk.API(access_token=token, v='5.131', timeout=60)
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.posts_buffer = []
        self.last_processed_id = self.load_state()

        self.cutoff_date = datetime.now() - timedelta(days=DAYS_TO_SCRAPE)
        print(f"📅 Дата отсечения постов: {self.cutoff_date.strftime('%Y-%m-%d')}")

    def log(self, message):
        if DEBUG:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    data = json.load(f)
                    return data.get("last_processed_id", 0)
            except:
                pass
        return 0

    def save_state(self, last_id):
        with open(STATE_FILE, "w") as f:
            json.dump({"last_processed_id": last_id}, f)

    def get_users_chunk(self, limit=1000):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT id FROM users 
                WHERE id > {self.last_processed_id}
                  AND is_closed = FALSE 
                  AND deactivated IS NULL 
                ORDER BY id ASC
                LIMIT {limit}
            """)
            return [row[0] for row in cur.fetchall()]

    def flush_buffer(self):
        if not self.posts_buffer:
            return

        unique_map = {}
        for p in self.posts_buffer:
            unique_map[(p[0], p[1])] = p
        unique_posts_list = list(unique_map.values())

        try:
            with self.conn.cursor() as cur:
                query = """
                    INSERT INTO posts (
                        id, owner_id, owner_type, from_id, text, date, post_type,
                        is_pinned, marked_as_ads,
                        likes_count, views_count, reposts_count, comments_count,
                        is_copy, copy_owner_id, copy_post_id, copy_text,
                        has_photo, has_video, has_audio, has_link, 
                        attachments, scraped_at
                    ) VALUES %s
                    ON CONFLICT (owner_id, id) DO UPDATE SET
                        text = EXCLUDED.text,
                        likes_count = EXCLUDED.likes_count,
                        views_count = EXCLUDED.views_count,
                        reposts_count = EXCLUDED.reposts_count,
                        comments_count = EXCLUDED.comments_count,
                        attachments = EXCLUDED.attachments,
                        scraped_at = EXCLUDED.scraped_at;
                """
                execute_values(cur, query, unique_posts_list)
                self.conn.commit()
                self.posts_buffer.clear()
        except Exception as e:
            self.log(f"❌ Ошибка записи в БД: {e}")
            self.conn.rollback()
            self.posts_buffer.clear()

    def process_users_batch(self, user_ids):
        """Возвращает True если успех, False если ошибка API"""
        ids_str = ",".join(map(str, user_ids))

        code = f"""
            var user_ids = [{ids_str}];
            var all_posts = [];
            var i = 0;

            while (i < user_ids.length) {{
                var uid = user_ids[i];
                var resp = API.wall.get({{
                    "owner_id": uid, 
                    "count": {POSTS_PER_USER}, 
                    "extended": 0
                }});

                if (resp) {{
                    if (resp.items) {{
                        all_posts = all_posts + resp.items;
                    }}
                }}
                i = i + 1;
            }}
            return all_posts;
        """

        try:
            items = self.api.execute(code=code)

            # Если items is None, это не всегда ошибка, но VKScript обычно возвращает []
            if items is None:
                items = []

            saved_local = 0

            for p in items:
                if not isinstance(p, dict): continue

                post_ts = p.get('date', 0)
                post_date = datetime.fromtimestamp(post_ts)

                if post_date < self.cutoff_date:
                    continue

                    # Парсинг вложений и полей (как раньше)
                attachments = p.get('attachments', [])
                has_photo = any(a.get('type') == 'photo' for a in attachments)
                has_video = any(a.get('type') == 'video' for a in attachments)
                has_audio = any(a.get('type') == 'audio' for a in attachments)
                has_link = any(a.get('type') == 'link' for a in attachments)

                try:
                    atts_json = json.dumps(attachments, ensure_ascii=False)
                except:
                    atts_json = '[]'

                copy_history = p.get('copy_history', [])
                is_copy = len(copy_history) > 0
                copy_owner_id = copy_history[0].get('owner_id') if is_copy else None
                copy_post_id = copy_history[0].get('id') if is_copy else None
                copy_text = copy_history[0].get('text') if is_copy else None

                post_tuple = (
                    p.get('id'),
                    p.get('owner_id'),
                    'user',
                    p.get('from_id'),
                    p.get('text', ''),
                    post_date,
                    p.get('post_type', 'post'),
                    bool(p.get('is_pinned', 0)),
                    bool(p.get('marked_as_ads', 0)),
                    p.get('likes', {}).get('count', 0),
                    p.get('views', {}).get('count', 0),
                    p.get('reposts', {}).get('count', 0),
                    p.get('comments', {}).get('count', 0),
                    is_copy, copy_owner_id, copy_post_id, copy_text,
                    has_photo, has_video, has_audio, has_link,
                    atts_json, datetime.now()
                )
                self.posts_buffer.append(post_tuple)
                saved_local += 1

            if len(self.posts_buffer) >= BATCH_SIZE:
                self.flush_buffer()

            print(f"\r   👤 Пакет {len(user_ids)} чел. | Свежих: {saved_local}", end="")
            return True  # УСПЕХ

        except Exception as e:
            # Ловим ошибку и возвращаем False, чтобы внешний цикл снизил скорость
            # self.log(f"\nСбой батча: {e}")
            return False

    def run(self):
        print("🚀 Начинаем сбор постов пользователей...")

        # Динамический размер батча
        current_batch_size = MAX_USERS_PER_BATCH

        while True:
            # Берем большую пачку ID (например, 2000)
            chunk_size = 2000
            target_ids = self.get_users_chunk(limit=chunk_size)

            if not target_ids:
                print("\n🏁 Все пользователи обработаны!")
                break

            print(f"\n📚 Загружена очередь из {len(target_ids)} пользователей (ID от {target_ids[0]})")

            # Внутренний цикл с ручным управлением индексом
            i = 0
            while i < len(target_ids):
                # Формируем срез текущего размера
                batch_ids = target_ids[i: i + current_batch_size]

                # Пытаемся обработать
                success = self.process_users_batch(batch_ids)

                if success:
                    # Если успех - двигаем индекс вперед
                    i += len(batch_ids)

                    # Сохраняем состояние (последний успешный ID)
                    self.last_processed_id = batch_ids[-1]
                    self.save_state(self.last_processed_id)

                    # Если все прошло хорошо, потихоньку увеличиваем скорость (до максимума)
                    if current_batch_size < MAX_USERS_PER_BATCH:
                        current_batch_size += 1

                    time.sleep(VK_SLEEP)

                else:
                    # Если ошибка (500, Timeout и т.д.)
                    print(f"\n⚠️ Сбой на пачке {len(batch_ids)} пользователей. Снижаю нагрузку...")

                    if current_batch_size > 5:
                        current_batch_size = 5  # Резко снижаем до 5
                    elif current_batch_size > 1:
                        current_batch_size = 1  # Снижаем до 1
                    else:
                        # Если даже 1 пользователь валит скрипт (битый профиль)
                        print(f"❌ Пользователь ID {batch_ids[0]} вызывает ошибку API. Пропускаем.")
                        i += 1
                        self.last_processed_id = batch_ids[0]
                        self.save_state(self.last_processed_id)

                    time.sleep(1)  # Даем серверу передохнуть

            # Сброс остатков перед новой большой загрузкой
            self.flush_buffer()


if __name__ == "__main__":
    scraper = VkUserPostScraper(token)
    try:
        scraper.run()
    except KeyboardInterrupt:
        print("\n🛑 Скрипт остановлен.")
    finally:
        if scraper.conn:
            scraper.conn.close()