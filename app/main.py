import os
import random
import math
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# load env
load_dotenv()
DB_URL = os.getenv("DB_URL")

app = FastAPI(
    title="Training API",
    description="API for training data",
)

engine = create_engine(DB_URL)


@app.get("/")
def root():
    return {"message": "API is running"}


# get all data with limit
@app.get("/data")
def get_data(limit: int = Query(10, le=100)):
    query = text("SELECT * FROM public.data LIMIT :limit")

    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        rows = [dict(row._mapping) for row in result]

    return {"count": len(rows), "data": rows}


# filter by name
@app.get("/search")
def search(name: str):
    query = text("""
        SELECT * FROM public.data
        WHERE name ILIKE :name
        LIMIT 20
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"name": f"%{name}%"})
        rows = [dict(row._mapping) for row in result]

    return rows


# filter by nilai (range)
@app.get("/nilai")
def filter_nilai(min_nilai: float = 0, max_nilai: float = 100):
    query = text("""
        SELECT * FROM public.data
        WHERE nilai BETWEEN :min AND :max
        LIMIT 50
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"min": min_nilai, "max": max_nilai})
        rows = [dict(row._mapping) for row in result]

    return rows


# get all kurs data
@app.get("/api/kurs")
def get_all_kurs(limit: int = Query(100, ge=1, le=500)):
    query = text("""
        SELECT
            LEFT(CAST(bulan_tahun AS TEXT), 7) AS bulan_tahun,
            kurs_jual,
            kurs_beli,
            kurs_tengah
        FROM public.kurs
        ORDER BY bulan_tahun
        LIMIT :limit
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        rows = [dict(row._mapping) for row in result]

    kurs_map = {
        row["bulan_tahun"]: {
            "kurs_jual": row["kurs_jual"],
            "kurs_beli": row["kurs_beli"],
            "kurs_tengah": row["kurs_tengah"],
        }
        for row in rows
    }

    return {
        "status": "success",
        "data": kurs_map,
    }


# get kurs by bulan-tahun, ex: 2024-01
@app.get("/api/kurs/{bulan_tahun}")
def get_kurs(bulan_tahun: str):
    query = text("""
        SELECT
            LEFT(CAST(bulan_tahun AS TEXT), 7) AS bulan_tahun,
            kurs_jual,
            kurs_beli,
            kurs_tengah
        FROM public.kurs
        WHERE LEFT(CAST(bulan_tahun AS TEXT), 7) = :bulan_tahun
        LIMIT 1
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"bulan_tahun": bulan_tahun})
        row = result.fetchone()

    if not row:
        return JSONResponse(
            status_code=404,
            content={"status": "error", "message": "Periode tidak ditemukan"},
        )

    row_data = dict(row._mapping)
    return {
        "status": "success",
        "periode": row_data["bulan_tahun"],
        "mata_uang": "USD/IDR",
        "data": {
            "kurs_jual": row_data["kurs_jual"],
            "kurs_beli": row_data["kurs_beli"],
            "kurs_tengah": row_data["kurs_tengah"],
        },
    }


# realtime data pipeline dummy endpoint
SENSOR_LOCATIONS = ["Jakarta", "Bandung", "Surabaya", "Medan", "Makassar"]


def generate_realtime_row(i: int) -> dict:
    """Generate a single dummy sensor row with realistic imperfections."""
    rng = random.random()

    # ~15% chance missing temperature
    if rng < 0.15:
        temperature = None
    # ~10% chance outlier temperature
    elif rng < 0.25:
        temperature = round(random.uniform(80, 150), 2)
    else:
        temperature = round(random.uniform(20, 35), 2)

    rng2 = random.random()
    # ~10% chance humidity is a string (dirty data)
    if rng2 < 0.10:
        humidity = "N/A"
    # ~5% chance missing
    elif rng2 < 0.15:
        humidity = None
    else:
        humidity = round(random.uniform(40, 95), 1)

    rng3 = random.random()
    # ~10% chance pressure is missing
    if rng3 < 0.10:
        pressure = None
    # ~8% chance outlier pressure
    elif rng3 < 0.18:
        pressure = round(random.uniform(1100, 1300), 1)
    else:
        pressure = round(random.uniform(995, 1025), 1)

    # ~12% chance sensor_id has mixed format
    if random.random() < 0.12:
        sensor_id = f"SEN-{random.randint(1, 9)}_ERROR"
    else:
        sensor_id = f"SEN-{random.randint(1, 20):03d}"

    return {
        "row_id": i + 1,
        "sensor_id": sensor_id,
        "location": random.choice(SENSOR_LOCATIONS),
        "temperature_c": temperature,
        "humidity_pct": humidity,
        "pressure_hpa": pressure,
        "battery_pct": round(random.uniform(5, 100), 1)
        if random.random() > 0.05
        else None,
        "signal_strength": random.choice(["strong", "medium", "weak", "UNKNOWN", None]),
        "reading_count": random.randint(1, 500) if random.random() > 0.08 else "error",
    }


# finance / tax revenue dummy endpoint
JENIS_PAJAK = [
    "PPh Pasal 21",
    "PPh Pasal 22",
    "PPh Pasal 23",
    "PPh Pasal 25",
    "PPh Pasal 29",
    "PPh Final",
    "PPN",
    "PPnBM",
    "PBB",
    "Bea Meterai",
]
PROVINSI = [
    "DKI Jakarta",
    "Jawa Barat",
    "Jawa Tengah",
    "Jawa Timur",
    "Banten",
    "Sumatera Utara",
    "Sumatera Selatan",
    "Kalimantan Timur",
    "Sulawesi Selatan",
    "Bali",
    "Riau",
    "Kepulauan Riau",
    "Papua",
    "Nusa Tenggara Barat",
]
SEKTOR = [
    "Manufaktur",
    "Perdagangan",
    "Jasa Keuangan",
    "Pertambangan",
    "Konstruksi",
    "Pertanian",
    "Teknologi",
    "Transportasi",
    "Kesehatan",
    "Pendidikan",
]
STATUS_PEMBAYARAN = ["Lunas", "Sebagian", "Menunggak", "Dalam Proses", "INVALID", None]


def generate_finance_row(i: int) -> dict:
    """Generate a single dummy tax revenue row with realistic imperfections."""
    # NPWP: ~8% malformed
    if random.random() < 0.08:
        npwp = f"NPWP-{random.randint(10000, 99999)}-ERR"
    else:
        npwp = f"{random.randint(10, 99)}.{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(1, 9)}-{random.randint(100, 999)}.{random.randint(100, 999)}"

    # Tahun pajak: ~5% missing
    tahun_pajak = None if random.random() < 0.05 else random.randint(2019, 2024)

    # Bulan: ~5% out of range (dirty)
    bulan = random.randint(13, 15) if random.random() < 0.05 else random.randint(1, 12)

    # Penerimaan bruto: ~10% missing, ~7% outlier (sangat besar / negatif)
    r = random.random()
    if r < 0.10:
        penerimaan_bruto = None
    elif r < 0.17:
        penerimaan_bruto = round(
            random.uniform(-500_000_000, -1_000_000), 2
        )  # negatif (anomali)
    elif r < 0.22:
        penerimaan_bruto = round(
            random.uniform(50_000_000_000, 200_000_000_000), 2
        )  # outlier besar
    else:
        penerimaan_bruto = round(random.uniform(1_000_000, 5_000_000_000), 2)

    # Restitusi: ~12% missing, ~5% string
    r2 = random.random()
    if r2 < 0.12:
        restitusi = None
    elif r2 < 0.17:
        restitusi = "TBD"
    else:
        restitusi = round(random.uniform(0, 500_000_000), 2)

    # Penerimaan neto: kalkulasi tapi ~8% corrupt jadi string
    if isinstance(penerimaan_bruto, (int, float)) and isinstance(
        restitusi, (int, float)
    ):
        neto = round(penerimaan_bruto - restitusi, 2)
    else:
        neto = None
    if random.random() < 0.08:
        neto = "CALC_ERROR"

    # Jumlah WP: ~6% missing, ~5% float (harusnya integer)
    r3 = random.random()
    if r3 < 0.06:
        jumlah_wp = None
    elif r3 < 0.11:
        jumlah_wp = round(random.uniform(1, 5000), 2)  # harusnya int
    else:
        jumlah_wp = random.randint(10, 50000)

    return {
        "row_id": i + 1,
        "npwp_bendahara": npwp,
        "provinsi": random.choice(PROVINSI),
        "sektor": random.choice(SEKTOR) if random.random() > 0.07 else None,
        "jenis_pajak": random.choice(JENIS_PAJAK),
        "tahun_pajak": tahun_pajak,
        "bulan": bulan,
        "penerimaan_bruto_idr": penerimaan_bruto,
        "restitusi_idr": restitusi,
        "penerimaan_neto_idr": neto,
        "jumlah_wp": jumlah_wp,
        "status_pembayaran": random.choice(STATUS_PEMBAYARAN),
        "kode_kanwil": f"KW-{random.randint(1, 34):02d}"
        if random.random() > 0.06
        else None,
    }


STATIC_FINANCE_DATA = None
FINAL_FINANCE_DATA = None


def get_or_create_static_finance_data():
    global STATIC_FINANCE_DATA
    if STATIC_FINANCE_DATA is None:
        random.seed(42)
        STATIC_FINANCE_DATA = [generate_finance_row(i) for i in range(50)]
        random.seed()  # reset to non-deterministic
    return STATIC_FINANCE_DATA


def get_or_create_final_finance_data():
    """Create and cache a deterministic 1000-row finance dataset once."""
    global FINAL_FINANCE_DATA
    if FINAL_FINANCE_DATA is None:
        random.seed(42)
        FINAL_FINANCE_DATA = [generate_finance_row(i) for i in range(1000)]
        random.seed()  # reset to non-deterministic
    return FINAL_FINANCE_DATA


@app.get("/api/finance-data/static")
def get_finance_data_static():
    """
    Static version of finance/tax revenue data.
    Returns the same 50 rows on every call (seeded, deterministic).
    Useful as a stable dataset for preprocessing exercises.
    """
    rows = get_or_create_static_finance_data()
    return {
        "status": "ok",
        "count": len(rows),
        "data": rows,
    }


@app.get("/api/finance-data")
def get_finance_data():
    """
    Finance / tax revenue pipeline endpoint.
    Returns 50 rows of dummy penerimaan negara (perpajakan) data with intentional imperfections:
    missing values, outliers, negative anomalies, mixed types, malformed IDs, and dirty fields.
    """
    rows = [generate_finance_row(i) for i in range(50)]
    return {
        "status": "ok",
        "count": len(rows),
        "data": rows,
    }


@app.get("/api/finance-data-final")
def get_finance_data_final():
    """
    Final finance dataset endpoint.
    Returns the same cached 1000 rows on every call.
    """
    rows = get_or_create_final_finance_data()
    return {
        "status": "ok",
        "count": len(rows),
        "data": rows,
    }


@app.get("/api/realtime-data")
def get_realtime_data():
    """
    Realtime data pipeline endpoint.
    Returns 10 rows of dummy sensor data with intentional imperfections:
    missing values, outliers, mixed types (text/number), and dirty fields.
    """
    rows = [generate_realtime_row(i) for i in range(10)]
    return {
        "status": "ok",
        "count": len(rows),
        "data": rows,
    }
