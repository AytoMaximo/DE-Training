import argparse
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_csv(filepath: str, rows: int):
    """
    Генерирует CSV с тремя колонками:
    - timestamp: последовательные временные метки начиная с текущего момента
    - value: случайные целые числа от 0 до 200
    - category: случайная категория из ['A', 'B', 'C']
    """
    start = datetime.now()
    timestamps = [start + timedelta(seconds=i) for i in range(rows)]
    values = np.random.randint(0, 200, size=rows)
    categories = np.random.choice(['A', 'B', 'C'], size=rows)

    df = pd.DataFrame({
        'timestamp': timestamps,
        'value': values,
        'category': categories
    })
    df.to_csv(filepath, index=False)
    print(f"Generated: {filepath} ({rows} rows)")


def main():
    parser = argparse.ArgumentParser(description="Генератор тестовых CSV для pipeline")
    parser.add_argument(
        "--output-dir", "-o",
        default="incoming",
        help="Папка для сохранения сгенерированных CSV"
    )
    parser.add_argument(
        "--rows", "-r",
        type=int,
        default=100,
        help="Количество строк в каждом CSV"
    )
    parser.add_argument(
        "--files", "-f",
        type=int,
        default=1,
        help="Сколько файлов сгенерировать"
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")

    for i in range(1, args.files + 1):
        filename = f"test_{timestamp_str}_{i}.csv" if args.files > 1 else f"test_{timestamp_str}.csv"
        filepath = os.path.join(args.output_dir, filename)
        generate_csv(filepath, args.rows)


if __name__ == "__main__":
    main()