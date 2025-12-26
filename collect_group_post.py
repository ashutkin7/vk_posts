import vk
import time
import json
import os
import random
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from config import user, password, db_name, host, token
from requests.exceptions import ReadTimeout, ConnectionError

# --- КОНФИГУРАЦИЯ ---
DB_CONFIG = {
    "dbname": db_name,
    "user": user,
    "password": password,
    "host": host
}

# НАСТРОЙКИ СКОРОСТИ
# Пытаемся взять 5 запросов по 100 постов за раз (итого 500).
EXECUTE_BATCH = 5

# --- ИЗМЕНЕНИЕ: ЦЕЛЬ ВСЕГО 500 ПОСТОВ С ГРУППЫ ---
TARGET_POSTS_COUNT = 500

BATCH_SIZE = 500
VK_SLEEP = 1.0
ERROR_SLEEP = 600
STATE_FILE = "posts_parser_state.json"
DEBUG = True


class VkPostScraper:
    def __init__(self, token):
        self.api = vk.API(access_token=token, v='5.131', timeout=120)
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.posts_buffer = []
        self.state = self.load_state()

    def log(self, message):
        if DEBUG:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    return json.load(f)
            except:
                pass
        return {"group_id": None, "offset": 0}

    def save_state(self, group_id, offset):
        with open(STATE_FILE, "w") as f:
            json.dump({"group_id": group_id, "offset": offset}, f)

    def get_groups_to_scrape(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id, name FROM groups 
                WHERE is_active = TRUE 
                  AND deactivated IS NULL
                  AND is_closed = 0
                  AND wall != 0
                ORDER BY members_count DESC
            """)
            return cur.fetchall()

    def flush_buffer(self):
        if not self.posts_buffer:
            return

        # Удаление дубликатов перед записью
        unique_posts_dict = {post[0]: post for post in self.posts_buffer}
        unique_posts_list = list(unique_posts_dict.values())

        try:
            with self.conn.cursor() as cur:
                query = """
                    INSERT INTO group_posts (
                        id, group_id, text, published_at, post_type,
                        likes_count, views_count, reposts_count, comments_count,
                        has_photo, has_video, attachments
                    ) VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        text = EXCLUDED.text,
                        likes_count = EXCLUDED.likes_count,
                        views_count = EXCLUDED.views_count,
                        reposts_count = EXCLUDED.reposts_count,
                        comments_count = EXCLUDED.comments_count,
                        attachments = EXCLUDED.attachments;
                """
                execute_values(cur, query, unique_posts_list)
                self.conn.commit()
                self.posts_buffer.clear()
        except Exception as e:
            self.log(f"❌ Ошибка БД: {e}")
            self.conn.rollback()
            self.posts_buffer.clear()

    def sleep_long(self, reason):
        self.log(f"🛑 {reason}")
        self.log(f"💤 Спим {int(ERROR_SLEEP / 60)} минут...")
        time.sleep(ERROR_SLEEP)
        self.log("🔔 Просыпаемся.")

    def parse_posts_from_group(self, group_id, group_name, start_offset=0):
        print(f"\n🚀 Группа: {group_name} (ID: {group_id}) | Цель: {TARGET_POSTS_COUNT} постов")
        if start_offset > 0:
            print(f"⏩ Продолжаем с позиции: {start_offset}")

        offset = start_offset
        current_execute_batch = EXECUTE_BATCH

        while offset < TARGET_POSTS_COUNT:
            # Вычисляем сколько осталось
            remaining = TARGET_POSTS_COUNT - offset
            needed_calls = (remaining + 99) // 100
            actual_batch = min(current_execute_batch, needed_calls)

            # Стандартный VKSCRIPT без лишних наворотов
            code = f"""
                var group_id = -{group_id}; 
                var start_offset = {offset};
                var posts = [];
                var i = 0;
                var batch_size = {actual_batch};

                var result_status = "ok";
                var error_msg = "";

                while (i < batch_size) {{
                    var current_offset = start_offset + (i * 100);

                    var resp = API.wall.get({{
                        "owner_id": group_id, 
                        "count": 100, 
                        "offset": current_offset,
                        "extended": 0
                    }});

                    if (!resp) {{
                        result_status = "fail";
                        error_msg = "API returned null";
                        i = 999; 
                    }} else {{
                        if (!resp.items) {{
                            result_status = "fail";
                            error_msg = "Response has no items";
                            i = 999;
                        }} else {{
                            posts = posts + resp.items;
                            if (resp.items.length == 0) {{
                                i = 999; 
                            }}
                        }}
                    }}
                    i = i + 1;
                }}

                return {{
                    "items": posts,
                    "status": result_status,
                    "error_msg": error_msg
                }};
            """

            try:
                response = self.api.execute(code=code, timeout=120)

                if not isinstance(response, dict):
                    self.sleep_long("Ответ не словарь.")
                    continue

                posts = response.get('items', [])
                status = response.get('status', 'ok')

                # Если успешно получили посты
                if posts:
                    # Если работали на пониженной скорости, пробуем аккуратно поднять
                    if current_execute_batch < EXECUTE_BATCH:
                        current_execute_batch += 1

                    for p in posts:
                        if not isinstance(p, dict): continue

                        has_photo = False
                        has_video = False
                        attachments = p.get('attachments', [])

                        try:
                            for att in attachments:
                                at_type = att.get('type')
                                if at_type == 'photo':
                                    has_photo = True
                                elif at_type == 'video':
                                    has_video = True
                        except:
                            pass

                        try:
                            atts_json = json.dumps(attachments, ensure_ascii=False)
                        except:
                            atts_json = '[]'

                        pub_date = datetime.fromtimestamp(p.get('date', 0))

                        post_tuple = (
                            p.get('id'), group_id, p.get('text', ''), pub_date,
                            p.get('post_type', 'post'),
                            p.get('likes', {}).get('count', 0),
                            p.get('views', {}).get('count', 0),
                            p.get('reposts', {}).get('count', 0),
                            p.get('comments', {}).get('count', 0),
                            has_photo, has_video, atts_json
                        )
                        self.posts_buffer.append(post_tuple)

                    items_received = len(posts)
                    offset += items_received

                    print(f"\r   📝 Постов: {offset} / {TARGET_POSTS_COUNT} (Batch: {actual_batch})", end="")

                    if len(self.posts_buffer) >= BATCH_SIZE:
                        self.flush_buffer()
                        self.save_state(group_id, offset)

                # Обработка сбоя внутри execute (редко, но бывает)
                if status == 'fail':
                    raise Exception(f"VKScript Fail: {response.get('error_msg')}")

                # Конец группы
                if not posts and status == 'ok':
                    self.log("\n🏁 Посты закончились.")
                    self.save_state(group_id, TARGET_POSTS_COUNT)
                    break

                time.sleep(VK_SLEEP + random.random())

            # --- ОБРАБОТКА ОШИБОК И ПОНИЖЕНИЕ СКОРОСТИ (500 -> 300 -> 100) ---
            except Exception as e:
                error_str = str(e)
                # Ловим коды ошибок: 13 (Too big), 12 (Runtime), 500 (Internal Server) или VKScript Fail
                is_overload = "500" in error_str or "Internal Server Error" in error_str or "VKScript Fail" in error_str

                # Проверка на ошибку VKAPI (13 или 12)
                if isinstance(e, vk.exceptions.VkAPIError):
                    if e.code == 13 or e.code == 12:
                        is_overload = True

                if is_overload:
                    self.log(f"\n⚠️ Сбой при получении {actual_batch * 100} постов.")

                    # Логика понижения:
                    if current_execute_batch > 3:
                        current_execute_batch = 3
                        self.log("📉 Снижаем нагрузку: пробуем 300 постов.")
                        time.sleep(2)
                        continue  # Пробуем снова с 300

                    elif current_execute_batch > 1:
                        current_execute_batch = 1
                        self.log("📉 Снижаем нагрузку: пробуем 100 постов.")
                        time.sleep(2)
                        continue  # Пробуем снова с 100

                    else:
                        # Если даже 100 не работает
                        self.log("❌ Даже 100 постов вызывают ошибку. Пропускаем этот блок.")
                        offset += 100
                        continue

                elif "Read timed out" in error_str:
                    self.log("\n🔌 Тайм-аут. Снижаем нагрузку до минимума.")
                    current_execute_batch = 1
                    time.sleep(5)

                else:
                    self.sleep_long(f"Критическая ошибка: {e}")

    def run(self):
        groups = self.get_groups_to_scrape()
        print(f"Всего групп с открытой стеной: {len(groups)}")

        last_group_id = self.state.get('group_id')
        last_offset = self.state.get('offset', 0)
        found = False
        if last_group_id is None: found = True

        for g_id, g_name in groups:
            if not found:
                if g_id == last_group_id:
                    found = True
                    if last_offset < TARGET_POSTS_COUNT:
                        self.parse_posts_from_group(g_id, g_name, start_offset=last_offset)
                    else:
                        print(f"⏩ Группа {g_name} уже собрана.")
                continue

            self.parse_posts_from_group(g_id, g_name, start_offset=0)
            self.flush_buffer()
            self.save_state(g_id, TARGET_POSTS_COUNT)


if __name__ == "__main__":
    scraper = VkPostScraper(token)
    try:
        scraper.run()
    except KeyboardInterrupt:
        print("\n🛑 Стоп.")
    finally:
        if scraper.conn:
            scraper.conn.close()