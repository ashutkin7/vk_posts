## Как получить данные из БД (Python + Pandas)

### 1. Установка зависимостей

Bash

```
pip install pandas sqlalchemy psycopg2-binary
```

### 2. Шаблон кода для выгрузки данных

Создайте файл (например, `analysis.py` или используйте Jupyter Notebook) и используйте следующий код:

Python

```
import pandas as pd
from sqlalchemy import create_engine
import json

# --- КОНФИГУРАЦИЯ ПОДКЛЮЧЕНИЯ ---
DB_USER = 'PostDb'       # Ваше имя пользователя
DB_PASS = 'PostDb'       # Ваш пароль
DB_HOST = '85.198.86.85' # IP сервера
DB_PORT = '5432'
DB_NAME = 'PostDb'

# Создаем строку подключения PostgreSQL
connection_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Инициализируем движок (создается пул соединений)
engine = create_engine(connection_str)

print("✅ Подключение к БД установлено.")

# ------------------------------------------------------------------
# СЦЕНАРИЙ 1: Получение пользователей для демографического анализа
# ------------------------------------------------------------------
def get_users_dataframe(limit=1000):
    """
    Выгружает пользователей, исключая удаленные/забаненные аккаунты.
    """
    query = """
    SELECT
        id, first_name, last_name, sex, bdate, city_title,
        followers_count, last_seen
    FROM users
    WHERE deactivated IS NULL
    LIMIT %s;
    """
    # Pandas сам выполняет SQL и возвращает удобный DataFrame
    df = pd.read_sql(query, engine, params=(limit,))
    return df

# ------------------------------------------------------------------
# СЦЕНАРИЙ 2: Получение текстов постов для NLP (анализ текста)
# ------------------------------------------------------------------
def get_posts_dataframe(limit=5000):
    """
    Выгружает посты, где есть текст длиннее 10 символов.
    """
    query = """
    SELECT
        owner_id,
        id as post_id,
        text,
        likes_count,
        views_count,
        date,
        marked_as_ads
    FROM posts
    WHERE text IS NOT NULL
      AND LENGTH(text) > 10
    ORDER BY date DESC
    LIMIT %s;
    """
    df = pd.read_sql(query, engine, params=(limit,))
    return df

# ------------------------------------------------------------------
# ПРИМЕР ИСПОЛЬЗОВАНИЯ
# ------------------------------------------------------------------
if __name__ == "__main__":

    # 1. Загружаем посты
    print("⏳ Выгружаю посты...")
    df_posts = get_posts_dataframe(2000)

    print(f"📊 Загружено {len(df_posts)} постов.")
    print(df_posts[['text', 'likes_count']].head()) # Вывод первых 5 строк

    # Пример простой аналитики: Среднее кол-во лайков
    avg_likes = df_posts['likes_count'].mean()
    print(f"❤️ Среднее кол-во лайков: {avg_likes:.2f}")

    # 2. Загружаем пользователей
    print("\n⏳ Выгружаю пользователей...")
    df_users = get_users_dataframe(1000)

    # Пример: Распределение по полу
    # 1 - Женский, 2 - Мужской
    gender_counts = df_users['sex'].value_counts()
    print("\nРаспределение по полу:")
    print(gender_counts)
```

## Cтруктура базы данных  
### 1. Таблица `groups` (Группы)  
  
|Поле|Тип данных|Описание|  
|---|---|---|  
|**`id`**|`BigInt` (PK)|ID группы (положительное число, как в API).|  
|**`name`**|`Text`|Название сообщества.|  
|**`screen_name`**|`Text`|Короткий адрес (slug).|  
|**`type`**|`Text`|Тип: `group`, `page`, `event`.|  
|**`is_closed`**|`SmallInt`|0 — открытое, 1 — закрытое, 2 — частное.|  
|**`deactivated`**|`Text`|`deleted`, `banned` или `NULL` (активна).|  
|**`members_count`**|`Int`|Количество участников.|  
|**`description`**|`Text`|Текст описания сообщества.|  
|**`status`**|`Text`|Текст статуса.|  
|**`site`**|`Text`|Ссылка на веб-сайт.|  
|**`verified`**|`Boolean`|Верифицировано ли сообщество (галочка).|  
|**`age_limits`**|`SmallInt`|1 — нет, 2 — 16+, 3 — 18+.|  
|**`city_id`**|`Int`|ID города.|  
|**`country_id`**|`Int`|ID страны.|  
|**`start_date`**|`Date`|Дата основания или начала мероприятия.|  
|**`market_enabled`**|`Boolean`|Включен ли блок товаров.|  
|**`counters_photos`**|`Int`|Количество фото.|  
|**`counters_videos`**|`Int`|Количество видео.|  
|**`counters_topics`**|`Int`|Количество обсуждений.|  
|**`counters_docs`**|`Int`|Количество документов.|  
|**`cover_url`**|`Text`|Ссылка на обложку (максимальное разрешение).|  
|**`photo_max_url`**|`Text`|Ссылка на аватарку (максимальное разрешение).|  
|**`scraped_at`**|`Timestamp`|Время сбора данных.|  
  
---  
  
### 2. Таблица `users` (Пользователи)  
  
|Поле|Тип данных|Описание|  
|---|---|---|  
|**`id`**|`BigInt` (PK)|ID пользователя.|  
|**`first_name`**|`Text`|Имя.|  
|**`last_name`**|`Text`|Фамилия.|  
|**`maiden_name`**|`Text`|Девичья фамилия.|  
|**`screen_name`**|`Text`|Никнейм (domain).|  
|**`sex`**|`SmallInt`|1 — жен, 2 — муж, 0 — не указан.|  
|**`bdate`**|`Text`|День рождения (храним строкой, т.к. может быть "21.9").|  
|**`city_id`**|`Int`|ID города.|  
|**`country_id`**|`Int`|ID страны.|  
|**`home_town`**|`Text`|Родной город (текстовое поле).|  
|**`relation`**|`SmallInt`|Семейное положение (код 1-8).|  
|**`relation_partner_id`**|`BigInt`|ID партнера (если указан).|  
|**`about`**|`Text`|Содержимое поля "О себе".|  
|**`activities`**|`Text`|Деятельность.|  
|**`interests`**|`Text`|Интересы.|  
|**`music`**|`Text`|Любимая музыка.|  
|**`movies`**|`Text`|Любимые фильмы.|  
|**`tv`**|`Text`|Любимые телешоу.|  
|**`books`**|`Text`|Любимые книги.|  
|**`games`**|`Text`|Любимые игры.|  
|**`quotes`**|`Text`|Любимые цитаты.|  
|**`personal_political`**|`SmallInt`|Полит. предпочтения (код 1-9).|  
|**`personal_alcohol`**|`SmallInt`|Отношение к алкоголю (1-5).|  
|**`personal_smoking`**|`SmallInt`|Отношение к курению (1-5).|  
|**`personal_religion`**|`Text`|Мировоззрение.|  
|**`personal_people_main`**|`SmallInt`|Главное в людях (код).|  
|**`personal_life_main`**|`SmallInt`|Главное в жизни (код).|  
|**`occupation_type`**|`Text`|`work`, `school`, `university`.|  
|**`occupation_name`**|`Text`|Название текущего места работы/учебы.|  
|**`site`**|`Text`|Ссылка на сайт.|  
|**`status`**|`Text`|Статус под именем.|  
|**`verified`**|`Boolean`|Верифицирован ли аккаунт.|  
|**`followers_count`**|`Int`|Число подписчиков.|  
|**`friends_count`**|`Int`|Число друзей.|  
|**`pages_count`**|`Int`|Число интересных страниц (подписок).|  
|**`last_seen`**|`Timestamp`|Время последнего захода.|  
|**`platform`**|`Int`|Тип устройства последнего входа (1-7).|  
|**`photo_max_url`**|`Text`|Аватарка.|  
|**`is_closed`**|`Boolean`|Закрыт ли профиль.|  
|**`deactivated`**|`Text`|Статус блокировки.|  
|**`career_json`**|`JSONB`|История карьеры (массив объектов).|  
|**`universities_json`**|`JSONB`|История ВУЗов (массив объектов).|  
|**`schools_json`**|`JSONB`|История школ (массив объектов).|  
|**`relatives_json`**|`JSONB`|Родственники (массив объектов).|  
|**`scraped_at`**|`Timestamp`|Время сбора данных.|  
  
---  
  
### 3. Таблица `subscriptions` (Подписки)  
  
|Поле|Тип данных|Описание|  
|---|---|---|  
|**`user_id`**|`BigInt` (FK)|ID пользователя.|  
|**`group_id`**|`BigInt` (FK)|ID группы.|  
|**`scraped_at`**|`Timestamp`|Дата фиксации подписки.|  
  
---  
  
### 4. Таблица `posts` (Посты)  
  
|Поле|Тип данных|Описание|  
|---|---|---|  
|**`id`**|`BigInt`|ID поста (локальный ID внутри стены).|  
|**`owner_id`**|`BigInt`|**Владелец стены.** Группа (<0) или User (>0).|  
|**`owner_type`**|`Text`|Поле-маркер: `group` или `user` (для быстрых выборок).|  
|**`from_id`**|`BigInt`|Автор поста (может совпадать с owner_id, а может быть другим юзером).|  
|**`text`**|`Text`|Весь текст поста.|  
|**`date`**|`Timestamp`|Дата публикации.|  
|**`post_type`**|`Text`|`post`, `copy`, `reply`, `suggest`.|  
|**`is_pinned`**|`Boolean`|Закреплена ли запись.|  
|**`marked_as_ads`**|`Boolean`|Пометка "Реклама".|  
|**`likes_count`**|`Int`|Количество лайков.|  
|**`views_count`**|`Int`|Количество просмотров.|  
|**`reposts_count`**|`Int`|Количество репостов.|  
|**`comments_count`**|`Int`|Количество комментариев.|  
|**`is_copy`**|`Boolean`|Является ли пост репостом.|  
|**`copy_owner_id`**|`BigInt`|ID владельца оригинального поста (откуда репост).|  
|**`copy_post_id`**|`BigInt`|ID оригинального поста.|  
|**`copy_text`**|`Text`|Текст оригинального поста (из copy_history, если есть).|  
|**`has_photo`**|`Boolean`|Есть ли картинка (флаг).|  
|**`has_video`**|`Boolean`|Есть ли видео (флаг).|  
|**`has_audio`**|`Boolean`|Есть ли аудио (флаг).|  
|**`has_link`**|`Boolean`|Есть ли ссылка (флаг).|  
|**`attachments`**|`JSONB`|Массив медиа-вложений (фото, видео, ссылки, опросы).|  
|**`scraped_at`**|`Timestamp`|Время сбора.|  
  
_Primary Key:_ Составной ключ `(owner_id, id)`.  
  
---  
  
### 5. Таблица `comments` (Комментарии)  
  
|Поле|Тип данных|Описание|  
|---|---|---|  
|**`id`**|`BigInt` (PK)|ID комментария.|  
|**`owner_id`**|`BigInt`|ID владельца стены (Группа или Юзер), где написан коммент.|  
|**`post_id`**|`BigInt`|ID поста, к которому относится комментарий.|  
|**`from_id`**|`BigInt`|Автор комментария.|  
|**`text`**|`Text`|Текст комментария.|  
|**`date`**|`Timestamp`|Дата написания.|  
|**`reply_to_user`**|`BigInt`|ID пользователя, которому отвечают (если есть).|  
|**`reply_to_comment`**|`BigInt`|ID родительского комментария (для веток обсуждений).|  
|**`likes_count`**|`Int`|Количество лайков на комментарии.|  
|**`scraped_at`**|`Timestamp`|Время сбора.|