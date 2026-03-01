from app.flats.routers import router as flats_router
from app.users.admin_routers import router as admin_router
from app.users.routers import router as users_router
from fastapi import FastAPI

app = FastAPI()

app.include_router(users_router)
app.include_router(admin_router)
app.include_router(flats_router)


@app.get("/health")
async def health_check():
    return {"status": "ok"}
