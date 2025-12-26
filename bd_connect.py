import psycopg2

try:
    connection = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="1",
        database="vk_posts"
    )

    connection.autocommit = True

    with connection.cursor() as cursor:
        cursor.execute("SELECT version();")
        print(f"Подключено! Версия: {cursor.fetchone()}")

    if connection:
        connection.close()

except Exception as _ex:
    print("Ошибка подключения к базе", _ex)