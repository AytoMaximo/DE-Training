from flask import Flask, request, render_template_string
import uuid
import datetime
import re
import clickhouse_connect

app = Flask(__name__)
client = clickhouse_connect.get_client(host='localhost', database='default')

# Регулярка для базовой проверки e‑mail
EMAIL_REGEX = re.compile(r'^[^@]+@[^@]+\.[^@]+$')

# Список допустимых стран
ALLOWED_COUNTRIES = {'USA', 'Germany', 'Russia', 'India'}

FORM_HTML = """
<!doctype html>
<title>Форма регистрации</title>
<h2>Регистрация</h2>
<form method="post">
    ФИО: <input name="full_name" required><br>
    Дата рождения: <input name="birth_date" type="date" required><br>
    Баланс: <input name="balance" type="number" step="0.01" required><br>
    Email: <input name="email" type="email" required><br>
    Страна:
    <select name="country" required>
        <option value="">— выберите —</option>
        <option>USA</option>
        <option>Germany</option>
        <option>Russia</option>
        <option>India</option>
    </select><br><br>
    <button type="submit">Отправить</button>
</form>
"""

@app.route("/", methods=["GET", "POST"])
def submit():
    if request.method == "POST":
        # 1. full_name: не пустое
        full_name = request.form.get("full_name", "").strip()
        if not full_name:
            return "ФИО не может быть пустым", 400

        # 2. birth_date: корректный формат и не в будущем
        birth_str = request.form.get("birth_date", "")
        try:
            birth_date = datetime.datetime.strptime(birth_str, "%Y-%m-%d").date()
        except ValueError:
            return "Неверный формат даты рождения", 400
        if birth_date > datetime.date.today():
            return "Дата рождения не может быть позже сегодняшней", 400

        # 3. balance: число ≥ 0
        balance_str = request.form.get("balance", "")
        try:
            balance = float(balance_str)
        except ValueError:
            return "Баланс должен быть числом", 400
        if balance < 0:
            return "Баланс не может быть отрицательным", 400

        # 4. email: базовая проверка формата
        email = request.form.get("email", "").strip()
        if not EMAIL_REGEX.match(email):
            return "Некорректный формат email", 400

        # 5. country: из списка
        country = request.form.get("country", "")
        if country not in ALLOWED_COUNTRIES:
            return "Недопустимая страна", 400

        # 6. проверка уникальности email в ClickHouse
        q = "SELECT count() FROM site_submissions WHERE email = %(email)s"
        res = client.query(q, {"email": email})
        existing = res.result_rows[0][0] if res.result_rows else 0
        if existing > 0:
            return "Этот email уже зарегистрирован", 400

        # Все проверки пройдены — вставляем в базу
        row = [
            str(uuid.uuid4()),
            full_name,
            birth_date,
            balance,
            email,
            country,
            datetime.datetime.now()
        ]
        client.insert(
            table="site_submissions",
            data=[row],
            column_names=[
                "id", "full_name", "birth_date",
                "balance", "email", "country", "submitted_at"
            ]
        )
        return "Данные успешно отправлены!", 200

    # GET — показать форму
    return render_template_string(FORM_HTML)

if __name__ == "__main__":
    # В продакшене debug=False
    app.run(host="0.0.0.0", port=5000, debug=True)
