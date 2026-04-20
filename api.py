from fastapi import FastAPI, HTTPException
import json
import os

app = FastAPI(
    title="API Data Source - Responsi UTS IPBD",
    description="Endpoint untuk menyajikan data hasil scraping dari file JSON."
)

FILE_PATH = "wired_all_categories.json"


@app.get("/articles")
async def ambil_semua_berita():

    if not os.path.exists(FILE_PATH):
        raise HTTPException(
            status_code=404,
            detail="File JSON hasil scraping belum tersedia. Jalankan scraper terlebih dahulu."
        )

    try:
        with open(FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not data or "articles" not in data:
            raise HTTPException(
                status_code=500,
                detail="Format data tidak valid atau kosong."
            )

        return data

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail="File JSON corrupt / tidak bisa dibaca."
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Terjadi kesalahan: {str(e)}"
        )


@app.get("/status")
async def cek_status():

    if os.path.exists(FILE_PATH):
        try:
            with open(FILE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)

            return {
                "status": "Ready",
                "data_source": FILE_PATH,
                "articles_count": data.get("articles_count", 0),
                "message": "Data siap ditarik oleh DAG Airflow"
            }

        except:
            return {
                "status": "Error",
                "message": "File ada tapi tidak bisa dibaca"
            }

    return {
        "status": "Not Ready",
        "message": "Data scraping belum ada"
    }