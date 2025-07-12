from flask import Flask, request, render_template_string
import uuid
import datetime
import clickhouse_connect

app = Flask(__name__)
client = clickhouse_connect.get_client(host='localhost', database='default')

FORM_HTML = """
<form method="post">
    ФИО: <input name="full_name" required><br>
    Дата рождения: <input name="birth_date" type="date" required><br>
    Баланс: <input name="balance" type="number" step="0.01" required><br>
    Email: <input name="email" type="email" required><br>
    Страна: <select name="country" required>
        <option>USA</option>
        <option>Germany</option>
        <option>Russia</option>
        <option>India</option>
    </select><br>
    <input type="submit">
</form>
"""

@app.route("/", methods=["GET", "POST"])
def submit():
    if request.method == "POST":
        row = [
            str(uuid.uuid4()),
            request.form["full_name"],
            datetime.datetime.strptime(request.form["birth_date"], "%Y-%m-%d").date(),
            float(request.form["balance"]),
            request.form["email"],
            request.form["country"],
            datetime.datetime.now()
        ]
        client.insert(
            table='site_submissions',
            data=[row],
            column_names=[
                'id', 'full_name', 'birth_date', 'balance', 'email', 'country', 'submitted_at'
            ]
        )
        return "Данные успешно отправлены!"
    return render_template_string(FORM_HTML)

if __name__ == "__main__":
    app.run(debug=True)