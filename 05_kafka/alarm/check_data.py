import os
import smtplib
import time
from datetime import date
from email.mime.text import MIMEText

from clickhouse_connect import get_client


TARGET_TABLE: str = "customer.sales"
MAX_ATTEMPTS: int = 4
RETRY_DELAY_SECONDS: int = 1
SMTP_TIMEOUT_SECONDS: int = 30


def get_required_env(name: str) -> str:
    value: str | None = os.environ.get(name)
    if value is None or value == "":
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def get_required_int_env(name: str) -> int:
    value: str = get_required_env(name)
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"Environment variable must be an integer: {name}") from error


def query_import_count(target_table: str, import_date: date) -> int:
    query_text: str = f"""
        SELECT count() FROM customer.imports
        WHERE table_name = '{target_table}'
          AND last_import_date = toDate('{import_date}')
    """
    count_value: object = get_client(
        host="localhost",
        port=8123,
        username="user",
        password="strongpassword",
    ).query(query_text).result_rows[0][0]

    if not isinstance(count_value, int):
        raise TypeError(f"ClickHouse count result must be int, got: {type(count_value).__name__}")
    return count_value


def has_import_for_date(target_table: str, import_date: date) -> bool:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        print(f"Attempt {attempt} of {MAX_ATTEMPTS}")
        import_count: int = query_import_count(target_table, import_date)

        if import_count > 0:
            print(f"Data for {import_date} exists in {target_table}.")
            return True

        print(f"Data for {import_date} does not exist in {target_table}.")
        if attempt < MAX_ATTEMPTS:
            print(f"Waiting {RETRY_DELAY_SECONDS} seconds before next attempt.")
            time.sleep(RETRY_DELAY_SECONDS)

    return False


def build_alert_message(subject: str, body: str, smtp_user: str, recipient_email: str) -> MIMEText:
    message: MIMEText = MIMEText(body)
    message["Subject"] = subject
    message["From"] = smtp_user
    message["To"] = recipient_email
    return message


def send_alert(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_token: str,
    recipient_email: str,
    subject: str,
    body: str,
) -> None:
    message: MIMEText = build_alert_message(subject, body, smtp_user, recipient_email)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=SMTP_TIMEOUT_SECONDS) as server:
            server.starttls()
            server.login(smtp_user, smtp_token)
            server.send_message(message)
    except (OSError, smtplib.SMTPException) as error:
        raise RuntimeError(
            "Failed to send SMTP alert "
            f"(smtp_host={smtp_host}, smtp_port={smtp_port}, "
            f"smtp_user={smtp_user}, recipient_email={recipient_email})"
        ) from error


def main() -> None:
    import_date: date = date.today()
    if has_import_for_date(TARGET_TABLE, import_date):
        return

    recipient_email: str = get_required_env("RECIPIENT_EMAIL")
    smtp_host: str = get_required_env("SMTP_HOST")
    smtp_port: int = get_required_int_env("SMTP_PORT")
    smtp_user: str = get_required_env("SMTP_USER")
    smtp_token: str = get_required_env("SMTP_TOKEN")
    subject: str = f"[ALERT] No data in {TARGET_TABLE} for {import_date}"
    body: str = (
        f"Data in table {TARGET_TABLE} for {import_date} did not appear "
        f"after {MAX_ATTEMPTS} checks."
    )

    send_alert(smtp_host, smtp_port, smtp_user, smtp_token, recipient_email, subject, body)
    print(f"Alert sent to {recipient_email}.")


if __name__ == "__main__":
    main()
