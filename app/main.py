import os
from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# load env
load_dotenv()
DB_URL = os.getenv("DB_URL")

app = FastAPI(
    title="Training API",
    description="API for training data",
    prefix="/api",
)

engine = create_engine(DB_URL)


@app.get("/")
def root():
    return {"message": "API is running"}


# 🔹 ambil data (limit)
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


# 🔹 filter by name
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


# 🔹 filter by nilai (range)
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
