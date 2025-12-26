import psycopg2
from psycopg2.extras import RealDictCursor
from collections import Counter
from config import user, password, db_name, host

# --- НАСТРОЙКИ ---
DB_CONFIG = {
    "dbname": db_name,
    "user": user,
    "password": password,
    "host": host
}

# Словари для расшифровки кодов VK
WALL_TYPES = {
    0: "Выключена (0)",
    1: "Открытая (1)",
    2: "Ограниченная (2)",
    3: "Закрытая (3)"
}

PRIVACY_TYPES = {
    0: "Открытое (0)",
    1: "Закрытое (1)",
    2: "Частное (2)"
}


class GroupManager:
    def __init__(self):
        self.conn = None
        self.groups = []

    def connect(self):
        """Подключение и загрузка данных в память"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            cur = self.conn.cursor(cursor_factory=RealDictCursor)

            print("⏳ Загружаю данные из базы...")
            # Загружаем сразу всё, так как групп не миллионы (памяти хватит)
            query = "SELECT * FROM groups ORDER BY members_count DESC;"
            cur.execute(query)
            self.groups = cur.fetchall()
            print(f"✅ Загружено групп: {len(self.groups)}")

        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")
            self.groups = []

    def show_statistics(self):
        """Вывод красивой сводной статистики"""
        if not self.groups:
            print("Нет данных для анализа.")
            return

        total_groups = len(self.groups)
        total_members = sum(g['members_count'] for g in self.groups)
        avg_members = total_members // total_groups if total_groups > 0 else 0

        # Подсчеты через Counter
        type_cnt = Counter(g['type'] for g in self.groups)
        wall_cnt = Counter(g['wall'] for g in self.groups)
        privacy_cnt = Counter(g['is_closed'] for g in self.groups)

        # Подсчет статусов (актив/бан)
        status_cnt = Counter()
        for g in self.groups:
            if g['deactivated']:
                status_cnt[f"Deactivated: {g['deactivated']}"] += 1
            else:
                status_cnt["Active"] += 1

        print("\n" + "=" * 30 + " СТАТИСТИКА " + "=" * 30)
        print(f"📊 Всего групп:      {total_groups}")
        print(f"👥 Общая аудитория:  {total_members:,}".replace(",", " "))
        print(f"📉 Среднее кол-во:   {avg_members:,}".replace(",", " "))
        print("-" * 72)

        # Вывод колонок
        print(f"{'ТИП СООБЩЕСТВА':<25} | {'ТИП СТЕНЫ':<25} | {'СТАТУС':<20}")
        print("-" * 72)

        # Собираем ключи для итерации (максимальное кол-во строк)
        row_count = max(len(type_cnt), len(wall_cnt), len(status_cnt))

        types_list = list(type_cnt.items())
        walls_list = list(wall_cnt.items())
        statuses_list = list(status_cnt.items())

        for i in range(row_count):
            # Тип
            t_str = ""
            if i < len(types_list):
                k, v = types_list[i]
                t_str = f"{k}: {v}"

            # Стена
            w_str = ""
            if i < len(walls_list):
                k, v = walls_list[i]
                readable_wall = WALL_TYPES.get(k, str(k))
                w_str = f"{readable_wall}: {v}"

            # Статус
            s_str = ""
            if i < len(statuses_list):
                k, v = statuses_list[i]
                s_str = f"{k}: {v}"

            print(f"{t_str:<25} | {w_str:<25} | {s_str:<20}")
        print("=" * 72 + "\n")

    def show_list(self, limit=20):
        """Вывод списка групп (Топ N)"""
        print(f"\n📋 ТОП-{limit} ГРУПП ПО ПОДПИСЧИКАМ:")
        print("-" * 80)
        print(f"{'ID':<12} | {'Имя':<30} | {'Подписчики':<10} | {'Стена':<15}")
        print("-" * 80)

        for g in self.groups[:limit]:
            # Обработка длинных названий
            name = g['name']
            if len(name) > 28:
                name = name[:25] + "..."

            # Расшифровка стены
            wall_text = WALL_TYPES.get(g['wall'], str(g['wall']))

            # Пометка удаленных
            if g['deactivated']:
                name = f"💀 {name}"

            print(f"{g['id']:<12} | {name:<30} | {g['members_count']:<10} | {wall_text:<15}")
        print("-" * 80 + "\n")

    def close(self):
        if self.conn:
            self.conn.close()


# --- МЕНЮ ---
def main():
    manager = GroupManager()
    manager.connect()

    while True:
        print("Выберите действие:")
        print("1. 📊 Показать общую статистику")
        print("2. 📋 Показать список групп (Топ-50)")
        print("3. 🔄 Перезагрузить данные из БД")
        print("0. 🚪 Выход")

        choice = input("Ваш выбор: ")

        if choice == "1":
            manager.show_statistics()
        elif choice == "2":
            manager.show_list(50)
        elif choice == "3":
            manager.connect()
        elif choice == "0":
            manager.close()
            print("Пока!")
            break
        else:
            print("Неверный ввод, попробуйте еще раз.")


if __name__ == "__main__":
    main()