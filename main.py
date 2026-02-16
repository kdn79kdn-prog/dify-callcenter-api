from fastapi import FastAPI

app = FastAPI(
    title="dify-callcenter-api",
    docs_url="/docs",      # ← 明示
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

@app.get("/")
def root():
    return {"status": "ok", "message": "root is alive"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/run_daily_close")
def run_daily_close(payload: dict):
    return {"status": "ok", "message": "APIは正常に動いています"}
