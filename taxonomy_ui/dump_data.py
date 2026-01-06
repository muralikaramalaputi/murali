import pandas as pd
from sqlalchemy import create_engine, text

# -----------------------------
# DB CONFIG
# -----------------------------
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "taxonomy_poc"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# -----------------------------
# FILE CONFIG
# -----------------------------
FILE_PATH = r"C:\Users\rambh\Downloads\taxo\taxonomy_ui\taxonomy_dump.xlsx"
CHUNK_SIZE = 10000

# -----------------------------
# COLUMN MAPPING
# -----------------------------
COLUMN_MAP = {
    "Portfolio": "portfolio",
    "Profit Center Key": "profit_center_key",
    "Commodity Level 0": "commodity_level_0",
    "Commodity Level 1": "commodity_level_1",
    "Commodity Level 2": "commodity_level_2",
    "Material #": "material_no",
    "Material Description": "material_description",
    "GR Quantity": "gr_quantity",
    "Vendor Name": "vendor_name",
    "Vendor #": "vendor_no",
    "Parent Vendor": "parent_vendor",
    "Vendor Region": "vendor_region",
    "Vendor Country": "vendor_country",
    "Internal/External": "internal_external",
    "BCC": "bcc",
    "Plant Name": "plant_name",
    "Plant Country": "plant_country",
    "Plant Region": "plant_region",
    "Deflation Strategy": "deflation_strategy",
    "Development Plan": "development_plan",
    "MPA": "mpa",
    "Productivity": "productivity",
    "Rebate": "rebate",
    "Single Source": "single_source",
    "Sole Source": "sole_source",
    "Strategic Status": "strategic_status",
    "Payment Terms": "payment_terms",
    "Stock": "stock",
    "GR Amount $ (AOP FX)": "gr_amount_aop_fx",
    "GR Amount $ (Hana FX)": "gr_amount_hana_fx",
    "GR Month": "gr_month",
    "GR Year": "gr_year"
}

# -----------------------------
# INSERT FUNCTION (NO UPSERT)
# -----------------------------
def insert_dataframe(df, table_name):
    cols = list(df.columns)
    insert_cols = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))

    sql = f"""
        INSERT INTO {table_name} ({insert_cols})
        VALUES ({placeholders})
    """

    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.executemany(sql, df.values.tolist())
        conn.commit()
    finally:
        cur.close()
        conn.close()

# -----------------------------
# READ EXCEL
# -----------------------------
print("📖 Reading Excel file...")
df = pd.read_excel(FILE_PATH, engine="openpyxl")
df = df.rename(columns=COLUMN_MAP)

# Keep everything, keep symbols
df = df.fillna("")
for col in df.columns:
    df[col] = df[col].astype(str)

# -----------------------------
# 🔥 FULL REFRESH (TRUNCATE FIRST)
# -----------------------------
print("🧹 Clearing old data...")
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE material_master RESTART IDENTITY;"))


print("✅ Old data removed")

# -----------------------------
# INSERT IN CHUNKS
# -----------------------------
total_rows = len(df)
print(f"📊 Total rows to insert: {total_rows}")

for start in range(0, total_rows, CHUNK_SIZE):
    end = min(start + CHUNK_SIZE, total_rows)
    chunk = df.iloc[start:end]

    insert_dataframe(chunk, "material_master")

    print(f"✅ Inserted rows {start + 1} to {end}")

print("🎉 FULL REFRESH LOAD COMPLETED SUCCESSFULLY")
