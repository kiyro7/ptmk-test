import os
import sys
from datetime import date
from dotenv import load_dotenv
import psycopg2
import random
import string
from time import time
from io import StringIO

GENDERS = ["Male", "Female"]


class Database:
    """Класс для работы с PostgreSQL"""

    def __init__(self):
        load_dotenv()
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        self.cursor = self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()


class Employee:
    """Модель сотрудника"""

    def __init__(self, full_name: str, birth_date: str, gender: str):
        self.full_name = full_name.strip()
        try:
            self.birth_date = date.fromisoformat(birth_date)
        except Exception as e:
            print(f"Неверная дата рождения: {e}")
            exit(-1)
        if gender not in GENDERS:
            print(f"Неверный пол: {gender}, выберите один из двух вариантов - {GENDERS}")
            exit(-1)
        self.gender = gender

    def get_age(self) -> int:
        today = date.today()
        return today.year - self.birth_date.year - (
                (today.month, today.day) < (self.birth_date.month, self.birth_date.day)
        )

    def to_tuple(self):
        return self.full_name, self.birth_date, self.gender


class EmployeeApp:
    TABLE_NAME = "employees"

    def __init__(self):
        self.db = Database()

    def first_mode(self):
        self.db.cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            full_name TEXT NOT NULL,
            birth_date DATE NOT NULL,
            gender TEXT NOT NULL
        );
        """)
        self.db.commit()
        print("Таблица создана")
        self.db.close()

    def second_mode(self, full_name, birth_date, gender):
        employee = Employee(full_name, birth_date, gender)
        self.db.cursor.execute(
            f"INSERT INTO {self.TABLE_NAME} (full_name, birth_date, gender) VALUES (%s, %s, %s)",
            employee.to_tuple()
        )
        self.db.commit()
        print(f"Сотрудник {employee.full_name} добавлен: {employee.get_age()} лет, пол {employee.gender}")
        self.db.close()

    def third_mode(self):
        print("Получаем сотрудников")
        self.db.cursor.execute(
            f"SELECT full_name, birth_date, gender FROM {self.TABLE_NAME} "
            f"GROUP BY full_name, birth_date, gender "
            f"ORDER BY full_name ASC "
            f"LIMIT 100"
        )
        rows = self.db.cursor.fetchall()
        print("Первые 100 сотрудников:")
        if not rows:
            print("Нет данных")
        for row in rows:
            emp = Employee(row[0], row[1].isoformat(), row[2])
            print(row[0], row[1], row[2], emp.get_age(), "лет", sep="\t")
        self.db.close()

    # def fourth_mode(self):
    #     print("Создаём сотрудников")
    #     start = time()
    #     employees = []
    #     total_rows = 1_000_000
    #     special_rows = 100
    #     normal_rows = total_rows - special_rows
    #
    #     for _ in range(normal_rows):
    #         fio = self._random_fio()
    #         birth = self._random_birth()
    #         gender = random.choice(GENDERS)
    #         employees.append(Employee(fio, birth, gender))
    #
    #     for _ in range(special_rows):
    #         fio = "F" + self._random_str(6) + " " + self._random_str(6) + " " + self._random_str(6)
    #         birth = self._random_birth()
    #         employees.append(Employee(fio, birth, "Male"))
    #
    #     self._batch_insert(employees)
    #     print(f"Заполнение БД завершено, заняло {round(time() - start, 3)} секунд")
    #     self.db.close()

    def fourth_mode(self):
        print("Создаём сотрудников")
        start = time()
        self.db.cursor.execute(f"TRUNCATE TABLE {self.TABLE_NAME};")
        self.db.commit()

        total_rows = 1_000_000
        special_rows = 100
        normal_rows = total_rows - special_rows

        buffer = StringIO()

        for _ in range(normal_rows):
            fio = self._random_fio()
            birth = self._random_birth()
            gender = random.choice(GENDERS)
            buffer.write(f"{fio},{birth},{gender}\n")

        for _ in range(special_rows):
            fio = "F" + self._random_str(6) + " " + self._random_str(6) + " " + self._random_str(6)
            birth = self._random_birth()
            buffer.write(f"{fio},{birth},Male\n")

        buffer.seek(0)
        self._copy_from_buffer(buffer)

        print(f"Заполнение БД завершено, заняло {round(time() - start, 3)} секунд")
        self.db.close()

    def _copy_from_buffer(self, buffer):
        self.db.cursor.copy_from(
            file=buffer,
            table=self.TABLE_NAME,
            sep=",",
            columns=("full_name", "birth_date", "gender")
        )
        self.db.commit()

    def _batch_insert(self, employees: list):
        batch = [emp.to_tuple() for emp in employees]
        self.db.cursor.executemany(
            f"INSERT INTO {self.TABLE_NAME} (full_name, birth_date, gender) VALUES (%s, %s, %s)",
            batch
        )
        self.db.commit()

    # def fifth_mode(self):
    #     print("Выполняем поиск сотрудников")
    #     start = time()
    #
    #     self.db.cursor.execute(
    #         f"SELECT full_name, birth_date, gender FROM {self.TABLE_NAME} WHERE gender='Male'"
    #     )
    #     rows_gender = self.db.cursor.fetchall()
    #
    #     self.db.cursor.execute(
    #         f"SELECT full_name, birth_date, gender FROM {self.TABLE_NAME} WHERE full_name LIKE 'F%%'"
    #     )
    #     rows_f = self.db.cursor.fetchall()
    #
    #     result = {
    #         row for row in rows_gender
    #         if row in rows_f
    #     }
    #
    #     duration = time() - start
    #
    #     print(f"Найдено записей: {len(result)}")
    #     print(f"Время выполнения {round(duration, 4)} секунд")
    #
    #     self.db.close()

    def fifth_mode(self):
        print("Выполняем поиск сотрудников")
        start = time()
        self.db.cursor.execute(
            f"SELECT full_name, birth_date, gender FROM {self.TABLE_NAME} "
            f"WHERE gender='Male' AND full_name LIKE 'F%'"
        )
        rows = self.db.cursor.fetchall()
        duration = time() - start

        print(f"Найдено записей: {len(rows)}")
        print(f"Время выполнения {round(duration, 4)} секунд")

        self.db.close()

    @staticmethod
    def _random_str(n=6):
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(n))

    def _random_fio(self):
        return f"{self._random_str().capitalize()} {self._random_str().capitalize()} {self._random_str().capitalize()}"

    @staticmethod
    def _random_birth():
        year = random.randint(1950, 2022)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        return f"{year}-{month:02}-{day:02}"


def main():
    if len(sys.argv) < 2 or not sys.argv[1].isdigit():
        print("Ошибка: укажите режим работы (1–5)")
        return

    mode = int(sys.argv[1])
    app = EmployeeApp()

    if mode == 1:
        app.first_mode()
    elif mode == 2:
        if len(sys.argv) != 5:
            print("Использование: python main.py 2 \"ФИО\" YYYY-MM-DD Gender")
            return
        app.second_mode(sys.argv[2], sys.argv[3], sys.argv[4])
    elif mode == 3:
        app.third_mode()
    elif mode == 4:
        app.fourth_mode()
    elif mode == 5:
        app.fifth_mode()
    else:
        print("Неизвестный режим, допустимые: 1–5")


if __name__ == "__main__":
    main()

"""
python main.py 5
Найдено записей: 19397
Время выполнения (серия запросов): 592.7558 секунд


python main.py 5
Найдено записей: 19397
Время выполнения: 0.0888 секунд

улучшение на 4 порядка
"""
