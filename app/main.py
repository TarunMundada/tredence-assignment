from fastapi import FastAPI
from app.api.endpoints import router as endpoints_router

app = FastAPI(title="Data Quality Workflow Engine")
app.include_router(endpoints_router)