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

user = 'postgres'
password = '1'
host = 'localhost'
db_name = 'vk_posts'

# --- КОНФИГУРАЦИЯ ---
DB_CONFIG = {
    "dbname": db_name,
    "user": user,
    "password": password,
    "host": host
}

BATCH_SIZE = 5000
VK_SLEEP = 1.0
ERROR_SLEEP = 600  # 10 минут сна при ошибках
STATE_FILE = "parser_state.json"
DEBUG = True


class VkScraper:
    def __init__(self, token):
        # Увеличиваем timeout до 120 секунд, так как 10 запросов внутри execute могут выполняться долго
        self.api = vk.API(access_token=token, v='5.131', timeout=120)
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.users_buffer = []
        self.subs_buffer = []
        self.total_saved = 0
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
                SELECT id, name, members_count FROM groups 
                WHERE is_active = TRUE 
                  AND deactivated IS NULL
                ORDER BY members_count DESC
            """)
            return cur.fetchall()

    def flush_buffer(self):
        if not self.users_buffer:
            return
        try:
            with self.conn.cursor() as cur:
                if self.users_buffer:
                    query_users = """
                        INSERT INTO users (
                            id, first_name, last_name, domain, sex, bdate, 
                            city_title, country_id, has_mobile, has_photo, 
                            site, status, followers_count, platform, last_seen
                        ) VALUES %s
                        ON CONFLICT (id) DO UPDATE SET
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            followers_count = EXCLUDED.followers_count,
                            last_seen = EXCLUDED.last_seen,
                            updated_at = NOW();
                    """
                    execute_values(cur, query_users, self.users_buffer)

                if self.subs_buffer:
                    query_subs = """
                        INSERT INTO subscriptions (group_id, user_id, scraped_at)
                        VALUES %s
                        ON CONFLICT (group_id, user_id) DO UPDATE SET
                            scraped_at = NOW();
                    """
                    execute_values(cur, query_subs, self.subs_buffer)

                self.conn.commit()
                self.users_buffer.clear()
                self.subs_buffer.clear()
        except Exception as e:
            self.log(f"❌ Ошибка БД: {e}")
            self.conn.rollback()
            self.users_buffer.clear()
            self.subs_buffer.clear()

    def sleep_long(self, reason):
        """Функция длительного отдыха (10 минут)"""
        self.log(f"🛑 {reason}")
        self.log(f"💤 Уходим в спячку на {int(ERROR_SLEEP / 60)} минут...")
        time.sleep(ERROR_SLEEP)
        self.log("🔔 Просыпаемся и пробуем снова.")

    def parse_users_from_group(self, group_id, group_name, members_count, start_offset=0):
        print(f"\n🚀 Группа: {group_name} (ID: {group_id}) | Подписчиков: {members_count}")
        if start_offset > 0:
            print(f"⏩ Продолжаем с позиции: {start_offset}")

        offset = start_offset

        # --- МАКСИМАЛЬНОЕ УСКОРЕНИЕ ---
        # 10 запросов * 1000 юзеров = 10 000 за один HTTP запрос
        EXECUTE_BATCH = 10

        while True:
            code = f"""
                var group_id = {group_id};
                var start_offset = {offset};
                var members = [];
                var i = 0;
                var batch_size = {EXECUTE_BATCH};

                // Статус результата: 'ok' или 'fail'
                var result_status = "ok";
                var error_msg = "";
                var failed_index = -1;

                while (i < batch_size) {{
                    var current_offset = start_offset + (i * 1000);

                    var resp = API.groups.getMembers({{
                        "group_id": group_id, 
                        "count": 1000, 
                        "offset": current_offset, 
                        "fields": "sex, bdate, city, country, has_mobile, photo_max_orig, site, status, followers_count, last_seen, domain"
                    }});

                    // 1. Проверка на полный провал (null)
                    if (!resp) {{
                        result_status = "fail";
                        error_msg = "API returned null";
                        failed_index = i;
                        // Прерываем цикл
                        i = 999; 
                    }} else {{
                        // 2. Проверка структуры ответа
                        if (!resp.items) {{
                            result_status = "fail";
                            error_msg = "Response has no items";
                            failed_index = i;
                            i = 999;
                        }} else {{
                            // Всё ок, добавляем
                            members = members + resp.items;

                            // 3. Проверка на конец списка
                            if (resp.items.length == 0) {{
                                i = 999; 
                            }}
                        }}
                    }}
                    i = i + 1;
                }}

                return {{
                    "items": members,
                    "status": result_status,
                    "error_msg": error_msg,
                    "failed_at_index": failed_index
                }};
            """

            try:
                # self.log(f"Запрос execute (offset={offset})...")

                # Timeout 120 сек, так как 10 запросов могут выполняться долго
                response = self.api.execute(code=code, timeout=120)

                # Проверка: пришел ли корректный JSON
                if not isinstance(response, dict):
                    self.sleep_long("Ответ от execute не является словарем (Сбой API).")
                    continue

                members = response.get('items', [])
                status = response.get('status', 'ok')

                # --- ЛОГИКА ОБРАБОТКИ РЕЗУЛЬТАТА ---

                # Сохраняем то, что успели получить
                if members:
                    for u in members:
                        if not isinstance(u, dict): continue

                        bdate = u.get('bdate')
                        valid_bdate = None
                        if bdate and isinstance(bdate, str) and len(bdate.split('.')) == 3:
                            try:
                                valid_bdate = datetime.strptime(bdate, "%d.%m.%Y").date()
                            except:
                                valid_bdate = None

                        city = u.get('city', {}).get('title') if isinstance(u.get('city'), dict) else None
                        country = u.get('country', {}).get('id') if isinstance(u.get('country'), dict) else None

                        last_seen_time = None
                        platform = None
                        if isinstance(u.get('last_seen'), dict):
                            last_seen_time = datetime.fromtimestamp(u['last_seen'].get('time', 0))
                            platform = u['last_seen'].get('platform')

                        user_tuple = (
                            u.get('id'), u.get('first_name'), u.get('last_name'), u.get('domain'),
                            u.get('sex'), valid_bdate, city, country,
                            bool(u.get('has_mobile')), bool(u.get('photo_max_orig')),
                            u.get('site'), u.get('status'), u.get('followers_count', 0),
                            platform, last_seen_time
                        )
                        self.users_buffer.append(user_tuple)
                        self.subs_buffer.append((group_id, u.get('id'), datetime.now()))

                    # Сдвигаем оффсет на реальное кол-во полученных
                    saved_count = len(members)
                    offset += saved_count

                    print(f"\r   ⚡ Обработано: {offset} / {members_count}", end="")

                    # Сброс в БД
                    if len(self.users_buffer) >= BATCH_SIZE:
                        self.flush_buffer()
                        self.save_state(group_id, offset)

                # Проверяем ошибки
                if status == 'fail':
                    error_msg = response.get('error_msg')
                    failed_index = response.get('failed_at_index')
                    self.log(f"\n⚠️ Сбой внутри execute на шаге {failed_index}: {error_msg}")
                    self.sleep_long("Получена ошибка из VKScript.")
                    continue

                    # Конец группы
                if not members and status == 'ok':
                    if offset < members_count:
                        self.sleep_long("Получен пустой список, но группа не кончилась.")
                        continue
                    else:
                        self.log("🏁 Конец группы.")
                        self.save_state(group_id, members_count)
                        break

                time.sleep(VK_SLEEP + random.random())

            except (ReadTimeout, ConnectionError) as e:
                self.sleep_long(f"Ошибка сети: {e}")

            except vk.exceptions.VkAPIError as e:
                # Глобальные ошибки API
                self.sleep_long(f"Ошибка VK API (Код {e.code}): {e}")

            except Exception as e:
                self.sleep_long(f"Критическая ошибка скрипта: {e}")

    def run(self):
        groups = self.get_groups_to_scrape()
        print(f"Всего групп в очереди: {len(groups)}")

        last_group_id = self.state.get('group_id')
        last_offset = self.state.get('offset', 0)
        found = False
        if last_group_id is None: found = True

        for g_id, g_name, m_count in groups:
            if not found:
                if g_id == last_group_id:
                    found = True
                    self.parse_users_from_group(g_id, g_name, m_count, start_offset=last_offset)
                continue

            self.parse_users_from_group(g_id, g_name, m_count, start_offset=0)
            self.flush_buffer()


if __name__ == "__main__":
    scraper = VkScraper(token)
    try:
        scraper.run()
    except KeyboardInterrupt:
        print("\n🛑 Стоп.")
    finally:
        if scraper.conn:
            scraper.conn.close()