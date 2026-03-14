import vk
import time
import json
import os
import random
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from config import user, password, db_name, host, token
from bd_connect import get_connection
# --- КОНФИГУРАЦИЯ ---


# --- НАСТРОЙКИ ---
DAYS_TO_SCRAPE = 183  # Полгода
MAX_POSTS_LIMIT = 50000  # Страховка

EXECUTE_BATCH = 5  # 5 запросов по 100 = 500 постов за раз
BATCH_SIZE = 500  # Размер буфера записи в БД
VK_SLEEP = 1  # Пауза

STATE_FILE = "posts_parser_state.json"
DEBUG = True


class VkPostScraper:
    def __init__(self, token):
        self.api = vk.API(access_token=token, v='5.131', timeout=60)
        self.conn = get_connection()
        self.posts_buffer = []
        self.state = self.load_state()

        self.cutoff_date = datetime.now() - timedelta(days=DAYS_TO_SCRAPE)
        print(f"📅 Дата отсечения: {self.cutoff_date.strftime('%Y-%m-%d')}")

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

    def get_groups_from_db(self):
        with self.conn.cursor() as cur:
            # Выбираем только активные группы (у которых deactivated IS NULL)
            cur.execute("""
                SELECT id, name FROM groups 
                WHERE deactivated IS NULL
                ORDER BY id ASC
            """)
            return cur.fetchall()

    def flush_buffer(self):
        if not self.posts_buffer:
            return

        unique_map = {}
        for p in self.posts_buffer:
            unique_map[(p[1], p[0])] = p

        unique_posts_list = list(unique_map.values())

        try:
            with self.conn.cursor() as cur:
                # Обновленный запрос с photo_urls и video_urls
                query = """
                    INSERT INTO posts (
                        id, owner_id, owner_type, from_id, text, date, post_type,
                        is_pinned, marked_as_ads,
                        likes_count, views_count, reposts_count, comments_count,
                        is_copy, copy_owner_id, copy_post_id, copy_text,
                        has_photo, has_video, has_audio, has_link, 
                        photo_urls, video_urls,
                        attachments, scraped_at
                    ) VALUES %s
                    ON CONFLICT (owner_id, id) DO UPDATE SET
                        text = EXCLUDED.text,
                        post_type = EXCLUDED.post_type,
                        is_pinned = EXCLUDED.is_pinned,
                        marked_as_ads = EXCLUDED.marked_as_ads,
                        likes_count = EXCLUDED.likes_count,
                        views_count = EXCLUDED.views_count,
                        reposts_count = EXCLUDED.reposts_count,
                        comments_count = EXCLUDED.comments_count,
                        is_copy = EXCLUDED.is_copy,
                        copy_owner_id = EXCLUDED.copy_owner_id,
                        copy_post_id = EXCLUDED.copy_post_id,
                        copy_text = EXCLUDED.copy_text,
                        has_photo = EXCLUDED.has_photo,
                        has_video = EXCLUDED.has_video,
                        has_audio = EXCLUDED.has_audio,
                        has_link = EXCLUDED.has_link,
                        photo_urls = EXCLUDED.photo_urls,
                        video_urls = EXCLUDED.video_urls,
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

    def parse_posts_from_group(self, group_id, group_name, start_offset=0):
        print(f"\n🚀 Группа: {group_name} (ID: {group_id})")

        owner_id = -group_id
        offset = start_offset
        stop_scraping = False
        saved_count_local = 0
        current_execute_batch = EXECUTE_BATCH

        real_total_count = None

        while not stop_scraping and offset < MAX_POSTS_LIMIT:
            if real_total_count is not None and offset >= real_total_count:
                print(f"\r   🏁 Достигнут конец ленты ({offset} >= {real_total_count}).")
                break

            code = f"""
                var group_id = {owner_id}; 
                var start_offset = {offset};
                var posts = [];
                var i = 0;
                var batch_size = {current_execute_batch};
                var total_count = 0;

                while (i < batch_size) {{
                    var current_offset = start_offset + (i * 100);
                    var resp = API.wall.get({{
                        "owner_id": group_id, 
                        "count": 100, 
                        "offset": current_offset,
                        "extended": 0
                    }});

                    if (resp) {{
                        posts = posts + resp.items;
                        total_count = resp.count;
                    }}
                    i = i + 1;
                }}
                return {{
                    "items": posts,
                    "total_count": total_count
                }};
            """

            try:
                response_data = self.api.execute(code=code)

                if not response_data:
                    break

                response_items = response_data.get('items', [])
                batch_total = response_data.get('total_count', 0)

                if batch_total > 0:
                    real_total_count = batch_total

                if len(response_items) == 0:
                    break

                last_dt = None
                added_in_batch = 0

                for p in response_items:
                    if not isinstance(p, dict): continue

                    post_ts = p.get('date', 0)
                    post_date = datetime.fromtimestamp(post_ts)
                    is_pinned = bool(p.get('is_pinned', 0))

                    if post_date < self.cutoff_date:
                        if is_pinned:
                            continue
                        else:
                            stop_scraping = True
                            break

                    # --- ИЗВЛЕЧЕНИЕ ПРЯМЫХ ССЫЛОК НА МЕДИА ---
                    attachments = p.get('attachments', [])

                    photo_urls_list = []
                    video_urls_list = []
                    has_photo = False
                    has_video = False
                    has_audio = False
                    has_link = False

                    for att in attachments:
                        att_type = att.get('type')

                        if att_type == 'photo':
                            has_photo = True
                            sizes = att.get('photo', {}).get('sizes', [])
                            if sizes:
                                # Берем картинку максимальной ширины
                                best_size = max(sizes, key=lambda x: x.get('width', 0))
                                photo_urls_list.append(best_size.get('url'))

                        elif att_type == 'video':
                            has_video = True
                            video = att.get('video', {})
                            v_owner = video.get('owner_id')
                            v_id = video.get('id')
                            if v_owner and v_id:
                                video_urls_list.append(f"https://vk.com/video{v_owner}_{v_id}")

                        elif att_type == 'audio':
                            has_audio = True
                        elif att_type == 'link':
                            has_link = True

                    photos_str = ", ".join(photo_urls_list) if photo_urls_list else None
                    videos_str = ", ".join(video_urls_list) if video_urls_list else None

                    try:
                        atts_json = json.dumps(attachments, ensure_ascii=False)
                    except:
                        atts_json = '[]'

                    copy_history = p.get('copy_history', [])
                    is_copy = len(copy_history) > 0
                    copy_owner_id = copy_history[0].get('owner_id') if is_copy else None
                    copy_post_id = copy_history[0].get('id') if is_copy else None
                    copy_text = copy_history[0].get('text') if is_copy else None

                    # Формируем кортеж с новыми полями
                    post_tuple = (
                        p.get('id'), owner_id, 'group', p.get('from_id'),
                        p.get('text', ''), post_date, p.get('post_type', 'post'),
                        is_pinned, bool(p.get('marked_as_ads', 0)),
                        p.get('likes', {}).get('count', 0),
                        p.get('views', {}).get('count', 0),
                        p.get('reposts', {}).get('count', 0),
                        p.get('comments', {}).get('count', 0),
                        is_copy, copy_owner_id, copy_post_id, copy_text,
                        has_photo, has_video, has_audio, has_link,
                        photos_str, videos_str,  # Новые поля
                        atts_json, datetime.now()
                    )

                    self.posts_buffer.append(post_tuple)
                    saved_count_local += 1
                    added_in_batch += 1
                    last_dt = post_date

                offset += len(response_items)

                total_str = str(real_total_count) if real_total_count else "?"
                date_str = last_dt.strftime('%Y-%m-%d') if last_dt else "?"

                print(f"\r   ⚡ Прогресс: {offset}/{total_str} | Сохранено: {saved_count_local} | Посл.дата: {date_str}",
                      end="")

                if len(self.posts_buffer) >= BATCH_SIZE or stop_scraping:
                    self.flush_buffer()
                    if not stop_scraping:
                        self.save_state(group_id, offset)

                if len(response_items) > 0 and added_in_batch == 0 and not stop_scraping:
                    if real_total_count and offset > real_total_count + 100:
                        print("\n⚠️ Обнаружен цикл на закрепленном посте. Принудительный выход.")
                        break

                time.sleep(VK_SLEEP)

            except Exception as e:
                print(f"\n⚠️ Ошибка: {e}")
                if current_execute_batch == 5:
                    current_execute_batch = 3
                    time.sleep(2)
                    continue
                elif current_execute_batch == 3:
                    current_execute_batch = 1
                    time.sleep(2)
                    continue
                else:
                    offset += 100
                    time.sleep(5)
                    continue

        self.flush_buffer()
        print(f"\n✅ Группа {group_name} готова. Сохранено: {saved_count_local}")
        self.save_state(None, 0)

    def run(self):
        groups = self.get_groups_from_db()
        print(f"Всего групп: {len(groups)}")

        last_group_id = self.state.get('group_id')
        last_offset = self.state.get('offset', 0)
        found_start = False if last_group_id else True

        for g_id, g_name in groups:
            if not found_start:
                if g_id == last_group_id:
                    found_start = True
                    self.parse_posts_from_group(g_id, g_name, start_offset=last_offset)
                continue

            self.parse_posts_from_group(g_id, g_name, start_offset=0)


if __name__ == "__main__":
    scraper = VkPostScraper(token)
    try:
        scraper.run()
    except KeyboardInterrupt:
        print("\n🛑 Скрипт остановлен.")
    finally:
        if scraper.conn:
            scraper.conn.close()