from fastapi import FastAPI

app = FastAPI(title="Apollo 67")

@app.get("/healthz")
def health_check():
    return {"status": "ok"}

@app.get("/")
def root():
    return {"app": "Apollo 67", "message": "Backend running"}
