import vk
import time
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

BATCH_SIZE = 10000
VK_SLEEP = 0.35  # Пауза между запросами


class VkScraper:
    def __init__(self, token):
        self.api = vk.API(access_token=token, v='5.131', timeout=60)
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.users_buffer = []
        self.subs_buffer = []
        self.total_saved = 0

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
        if not self.users_buffer and not self.subs_buffer:
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
                self.total_saved += len(self.users_buffer)
                self.users_buffer.clear()
                self.subs_buffer.clear()

        except Exception as e:
            print(f"❌ Ошибка БД: {e}")
            self.conn.rollback()
            raise e

    def parse_users_from_group(self, group_id, group_name, members_count):
        print(f"\n🚀 Группа: {group_name} (ID: {group_id}) | Подписчиков: {members_count}")

        # --- ИСПРАВЛЕНИЕ: Снижаем до 6 (6000 за раз) ---
        # Это безопасно для response size limits
        EXECUTE_BATCH = 6

        code_template = """
            var group_id = %s;
            var offset = %s;
            var members = [];
            var i = 0;

            // Цикл теперь выполняется 6 раз
            while (i < %s) {
                var resp = API.groups.getMembers({
                    "group_id": group_id, 
                    "count": 1000, 
                    "offset": offset + (i * 1000), 
                    "fields": "sex, bdate, city, country, has_mobile, photo_max_orig, site, status, followers_count, last_seen, domain"
                });

                if (resp.items.length == 0) {
                    return members;
                }

                members = members + resp.items;
                i = i + 1;
            }
            return members;
        """

        offset = 0
        retry_count = 0

        while True:
            try:
                code = code_template % (group_id, offset, EXECUTE_BATCH)

                members = self.api.execute(code=code, timeout=60)
                retry_count = 0

                if not members:
                    break

                for u in members:
                    bdate = u.get('bdate')
                    valid_bdate = None
                    if bdate and len(bdate.split('.')) == 3:
                        try:
                            valid_bdate = datetime.strptime(bdate, "%d.%m.%Y").date()
                        except:
                            valid_bdate = None

                    city = u.get('city', {}).get('title')
                    country = u.get('country', {}).get('id')
                    last_seen_time = None
                    platform = None
                    if 'last_seen' in u:
                        last_seen_time = datetime.fromtimestamp(u['last_seen']['time'])
                        platform = u['last_seen'].get('platform')

                    user_tuple = (
                        u['id'],
                        u.get('first_name'),
                        u.get('last_name'),
                        u.get('domain'),
                        u.get('sex'),
                        valid_bdate,
                        city,
                        country,
                        bool(u.get('has_mobile')),
                        bool(u.get('photo_max_orig')),
                        u.get('site'),
                        u.get('status'),
                        u.get('followers_count', 0),
                        platform,
                        last_seen_time
                    )

                    self.users_buffer.append(user_tuple)
                    self.subs_buffer.append((group_id, u['id'], datetime.now()))

                # Сдвигаем оффсет на размер пачки (6000)
                offset += (EXECUTE_BATCH * 1000)

                print(f"\r   ⚡ Обработано: {offset} / {members_count} (Saved: {self.total_saved})", end="")

                if len(self.users_buffer) >= BATCH_SIZE:
                    self.flush_buffer()

                time.sleep(VK_SLEEP)

            except (ReadTimeout, ConnectionError) as net_err:
                retry_count += 1
                print(f"\n   🔌 Ошибка сети (попытка {retry_count}/3): {net_err}")
                if retry_count >= 3:
                    print("   ❌ Слишком много ошибок сети. Пропускаем остаток группы.")
                    break
                time.sleep(5)

            except vk.exceptions.VkAPIError as e:
                print(f"\n⚠️ Ошибка VK API: {e}")

                # Код 13 = Response size too big (если вдруг опять случится, хотя на 6k не должно)
                if e.code == 13:
                    print("   📦 Ответ всё еще слишком большой. Нужно уменьшать EXECUTE_BATCH вручную.")
                    break
                elif e.code == 6:
                    time.sleep(2)
                elif e.code in [15, 30]:
                    print("   🔒 Приватная группа, пропускаем.")
                    break
                else:
                    print(f"   ⚠️ Ошибка API (код {e.code}), идем к следующей группе.")
                    break

    def run(self):
        groups = self.get_groups_to_scrape()
        print(f"Всего групп в очереди: {len(groups)}")

        try:
            for g_id, g_name, m_count in groups:
                self.parse_users_from_group(g_id, g_name, m_count)
                self.flush_buffer()

        except KeyboardInterrupt:
            print("\n🛑 Стоп.")
        except Exception as e:
            print(f"\n❌ Ошибка: {e}")
        finally:
            print("\n🧹 Финальное сохранение...")
            self.flush_buffer()
            if self.conn:
                self.conn.close()
            print("✅ Готово.")


if __name__ == "__main__":
    scraper = VkScraper(token)
    scraper.run()