import psycopg2

def update_rows():
    conn = psycopg2.connect(
        dbname="test_db",
        user="admin",
        password="admin",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
        UPDATE public.my_table
        SET name = %s
        WHERE name = %s;
    """, ('Alice_updated', 'Alice'))

    conn.commit()
    cur.close()
    conn.close()
    print("🔁 Строки с именем 'Alice' обновлены на 'Alice_updated'.")


if __name__ == '__main__':
    update_rows()