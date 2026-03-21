import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from broker.server.api import app as api_app

app = FastAPI(
    title="Message Broker",
    description="Брокер сообщений с функциональностью pub/sub, очередями сообщений",
    version="0.1.0",
)

# Добавляем CORS для возможности обращения к API из браузера
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Включаем API в основной сервер
app.mount("/api/v1", api_app)

@app.get("/")
async def root():
    return {
        "message": "Message Broker API",
        "documentation": "/api/v1/docs",
        "redoc": "/api/v1/redoc"
    }

def run_server():
    """Функция для запуска сервера"""
    uvicorn.run("broker.server.server:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    run_server()
