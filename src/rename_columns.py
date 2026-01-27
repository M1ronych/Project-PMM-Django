import pandas as pd

IN_PATH = "data_out/parsed.csv"
OUT_PATH = "data_out/parsed_renamed.csv"

# Завантажуємо
df = pd.read_csv(IN_PATH)

# Карта перейменування
COLUMN_MAP = {
    "c2": "fact_qty",
    "c3": "fact_amount",
    "c4": "plan_qty",
    "c5": "plan_amount",
    "c6": "price",
    "c7": "delta",
}

# Перейменовуєм
df = df.rename(columns=COLUMN_MAP)

# Ті  c*, які залишились нам не потрібні
drop_cols = [c for c in df.columns if c.startswith("c")]
df = df.drop(columns=drop_cols)

# Приводим числа в порядок (прибираемо 999999999)
num_cols = ["fact_qty", "fact_amount", "plan_qty", "plan_amount", "price", "delta"]
for col in num_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

# зберігаєм
df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

print("Saved:", OUT_PATH)
print("Columns:", list(df.columns))
print("Rows:", len(df))
print(df.head(10))
