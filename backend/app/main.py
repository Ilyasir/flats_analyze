from app.users.routers import router as users_router
from fastapi import FastAPI

app = FastAPI()

app.include_router(users_router)


@app.get("/health")
async def health_check():
    return {"status": "ok"}
