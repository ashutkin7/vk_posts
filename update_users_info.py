import vk
import time
import json
import os
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

# МАКСИМАЛЬНАЯ СКОРОСТЬ
# VK API позволяет 25 обращений внутри execute.
# users.get принимает пачку ID.
# 25 запросов * 100 пользователей = 2500 пользователей за один сетевой запрос.
USERS_PER_EXECUTE = 2500
SUB_BATCH_SIZE = 100  # Пачка внутри VKScript

STATE_FILE = "full_info_state.json"
DEBUG = True


class VkUserEnricher:
    def __init__(self, token):
        # Таймаут побольше, так как 2500 юзеров - это много данных
        self.api = vk.API(access_token=token, v='5.131', timeout=120)
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.users_buffer = []

        # Загружаем состояние сразу в переменную
        self.current_last_id = self.load_state_id()

    def log(self, message):
        if DEBUG:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def load_state_id(self):
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

    def setup_database(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users_detailed (
                    id BigInt PRIMARY KEY,
                    first_name Text,
                    last_name Text,
                    maiden_name Text,
                    screen_name Text,
                    sex SmallInt,
                    bdate Text,
                    city_id Int,
                    country_id Int,
                    home_town Text,
                    relation SmallInt,
                    relation_partner_id BigInt,
                    about Text,
                    activities Text,
                    interests Text,
                    music Text,
                    movies Text,
                    tv Text,
                    books Text,
                    games Text,
                    quotes Text,
                    personal_political SmallInt,
                    personal_alcohol SmallInt,
                    personal_smoking SmallInt,
                    personal_religion Text,
                    personal_people_main SmallInt,
                    personal_life_main SmallInt,
                    occupation_type Text,
                    occupation_name Text,
                    site Text,
                    status Text,
                    verified Boolean,
                    followers_count Int,
                    friends_count Int,
                    pages_count Int,
                    last_seen Timestamp,
                    platform Int,
                    photo_max_url Text,
                    is_closed Boolean,
                    deactivated Text,
                    career_json JSONB,
                    universities_json JSONB,
                    schools_json JSONB,
                    relatives_json JSONB,
                    scraped_at Timestamp
                );
            """)
            self.conn.commit()
            print("🛠 Таблица users_detailed проверена/создана.")

    def get_next_users_chunk(self, last_id, limit=10000):
        """Берет следующую партию ID из базы"""
        # self.log(f"📥 Запрос к БД: ID > {last_id}, лимит {limit}...")
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT id FROM new_users 
                WHERE id > {last_id}
                ORDER BY id ASC
                LIMIT {limit}
            """)
            return [row[0] for row in cur.fetchall()]

    def flush_buffer(self):
        if not self.users_buffer:
            return

        try:
            with self.conn.cursor() as cur:
                query = """
                    INSERT INTO users_detailed (
                        id, first_name, last_name, maiden_name, screen_name, sex, bdate,
                        city_id, country_id, home_town, relation, relation_partner_id,
                        about, activities, interests, music, movies, tv, books, games, quotes,
                        personal_political, personal_alcohol, personal_smoking, personal_religion,
                        personal_people_main, personal_life_main,
                        occupation_type, occupation_name, site, status, verified,
                        followers_count, friends_count, pages_count, last_seen, platform,
                        photo_max_url, is_closed, deactivated,
                        career_json, universities_json, schools_json, relatives_json, scraped_at
                    ) VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        status = EXCLUDED.status,
                        last_seen = EXCLUDED.last_seen,
                        followers_count = EXCLUDED.followers_count,
                        scraped_at = EXCLUDED.scraped_at;
                """
                execute_values(cur, query, self.users_buffer)
                self.conn.commit()
                self.users_buffer.clear()
        except Exception as e:
            self.log(f"❌ Ошибка БД: {e}")
            self.conn.rollback()
            self.users_buffer.clear()

    def process_execute_batch(self, user_ids_chunk):
        """Обрабатывает 2500 пользователей одним запросом"""
        ids_str = ",".join(map(str, user_ids_chunk))

        # Список всех полей
        fields = (
            "sex,bdate,city,country,home_town,relation,about,activities,interests,"
            "music,movies,tv,books,games,quotes,personal,occupation,site,status,"
            "verified,followers_count,counters,last_seen,photo_max_orig,domain,"
            "maiden_name,career,education,schools,relatives"
        )

        # VK Script: делит 2500 на кусочки по 100 и делает 25 запросов внутри
        code = f"""
            var user_ids = [{ids_str}];
            var fields = "{fields}";
            var data = [];
            var i = 0;

            // Пока не пройдем все ID
            while (i < user_ids.length) {{
                // Берем срез по 100 ID (безопасный лимит для users.get)
                var batch = user_ids.slice(i, i + {SUB_BATCH_SIZE});

                var resp = API.users.get({{
                    "user_ids": batch, 
                    "fields": fields
                }});

                data = data + resp;
                i = i + {SUB_BATCH_SIZE};
            }}
            return data;
        """

        try:
            # self.log(f"🚀 Запрос к VK API для {len(user_ids_chunk)} пользователей...")
            response = self.api.execute(code=code, timeout=120)

            if not isinstance(response, list):
                self.log(f"⚠️ Ответ не список: {response}")
                return False

            for u in response:
                if not isinstance(u, dict): continue

                uid = u.get('id')

                # Извлечение вложенных данных
                city_id = u.get('city', {}).get('id') if isinstance(u.get('city'), dict) else None
                country_id = u.get('country', {}).get('id') if isinstance(u.get('country'), dict) else None

                rel_partner = u.get('relation_partner')
                rel_partner_id = rel_partner.get('id') if isinstance(rel_partner, dict) else None

                personal = u.get('personal', {})
                occup = u.get('occupation', {})
                counters = u.get('counters', {})

                ls = u.get('last_seen', {})
                ls_time = datetime.fromtimestamp(ls.get('time')) if ls.get('time') else None
                ls_plat = ls.get('platform')

                # JSON dumps с защитой от ошибок
                try:
                    career = json.dumps(u.get('career', []), ensure_ascii=False)
                except:
                    career = '[]'

                try:
                    universities = json.dumps(u.get('universities', []), ensure_ascii=False)
                except:
                    universities = '[]'

                try:
                    schools = json.dumps(u.get('schools', []), ensure_ascii=False)
                except:
                    schools = '[]'

                try:
                    relatives = json.dumps(u.get('relatives', []), ensure_ascii=False)
                except:
                    relatives = '[]'

                row = (
                    uid,
                    u.get('first_name'),
                    u.get('last_name'),
                    u.get('maiden_name'),
                    u.get('domain'),
                    u.get('sex'),
                    u.get('bdate'),
                    city_id,
                    country_id,
                    u.get('home_town'),
                    u.get('relation'),
                    rel_partner_id,
                    u.get('about'),
                    u.get('activities'),
                    u.get('interests'),
                    u.get('music'),
                    u.get('movies'),
                    u.get('tv'),
                    u.get('books'),
                    u.get('games'),
                    u.get('quotes'),
                    personal.get('political'),
                    personal.get('alcohol'),
                    personal.get('smoking'),
                    personal.get('religion'),
                    personal.get('people_main'),
                    personal.get('life_main'),
                    occup.get('type'),
                    occup.get('name'),
                    u.get('site'),
                    u.get('status'),
                    bool(u.get('verified')),
                    u.get('followers_count'),
                    u.get('friends_count'),
                    counters.get('pages'),
                    ls_time,
                    ls_plat,
                    u.get('photo_max_orig'),
                    bool(u.get('is_closed')),
                    u.get('deactivated'),
                    career,
                    universities,
                    schools,
                    relatives,
                    datetime.now()
                )
                self.users_buffer.append(row)

            # Сохраняем в БД то, что получили
            self.flush_buffer()
            return True

        except Exception as e:
            self.log(f"❌ Ошибка при обработке batch: {e}")
            return False

    def run(self):
        self.setup_database()

        print(f"▶️ Старт с ID: {self.current_last_id}")

        while True:
            # 1. Получаем большую пачку ID из базы (например, 10,000)
            # Чтобы не дергать БД каждые 2 секунды
            target_ids = self.get_next_users_chunk(self.current_last_id, limit=10000)

            if not target_ids:
                print("\n🎉 Все пользователи обработаны!")
                break

            print(f"📚 Загружено {len(target_ids)} ID из базы. Обрабатываю...")

            # 2. Дробим эту пачку на куски по 2500 для execute
            for i in range(0, len(target_ids), USERS_PER_EXECUTE):
                batch = target_ids[i: i + USERS_PER_EXECUTE]

                # Обрабатываем
                success = self.process_execute_batch(batch)

                if success:
                    # ВАЖНО: Обновляем ID, чтобы не зациклиться
                    # Берем последний ID из текущего батча
                    last_id_in_batch = batch[-1]
                    self.current_last_id = last_id_in_batch
                    self.save_state(last_id_in_batch)

                    print(f"\r   👤 Обработано: {len(batch)} | Last ID: {last_id_in_batch}", end="")
                    time.sleep(0.5)
                else:
                    print(f"\n⚠️ Пропуск батча из-за ошибки (ID {batch[0]}-{batch[-1]})")
                    time.sleep(2)


if __name__ == "__main__":
    enricher = VkUserEnricher(token)
    try:
        enricher.run()
    except KeyboardInterrupt:
        print("\n🛑 Стоп.")
    finally:
        if enricher.conn:
            enricher.conn.close()