import os
from dotenv import load_dotenv
import psycopg2


load_dotenv()


def main():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
    """)

    cur.execute("INSERT INTO users (name) VALUES (%s), (%s)", ("Alice", "Bob"))
    conn.commit()

    cur.execute("SELECT * FROM users;")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    cur.execute("DELETE FROM users;")
    conn.commit()

    cur.execute("SELECT * FROM users;")
    rows = cur.fetchall()
    for row in rows:
        print(row)
    else:
        print(0)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
