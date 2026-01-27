import pandas as pd

df = pd.read_csv("data_out/parsed_renamed.csv")

# оставляем только технику
df = df[df["record_type"] == "vehicle"].copy()

# выкидываем техническое поле
df = df.drop(columns=["record_type"])

df.to_csv("data_out/final_vehicle.csv", index=False, encoding="utf-8-sig")

print("Saved: data_out/final_vehicle.csv")
print("Rows:", len(df))
print(df.head(10))
