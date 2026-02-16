from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/run_daily_close")
def run_daily_close(payload: dict):
    return {
        "status": "ok",
        "message": "APIは正常に動いています"
    }
