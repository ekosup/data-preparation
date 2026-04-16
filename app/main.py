import os
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

    return {
        "count": len(rows),
        "data": rows
    }


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
        result = conn.execute(query, {
            "min": min_nilai,
            "max": max_nilai
        })
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
            content={
                "status": "error",
                "message": "Periode tidak ditemukan"
            }
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
        }
    }
