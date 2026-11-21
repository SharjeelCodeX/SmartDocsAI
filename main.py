
from fastapi import FastAPI

app = FastAPI(title="SmartDocs AI")

@app.get("/")
def home():
    return {
        "message": "SmartDocs AI API is running!",
        "status": "success"
    }