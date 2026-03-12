from __future__ import annotations

from pathlib import Path

import logging

from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.routes import router as api_router
from app.core.config import settings
from app.db.base import Base
from app.db.migrate import run_migrations
from app.db.models import Employee
from app.db.session import engine, get_db
from app.services.maintenance import start_maintenance


Base.metadata.create_all(bind=engine)
run_migrations(engine)

app = FastAPI(title=settings.app_name)
app.include_router(api_router)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

logger = logging.getLogger("timeclock")


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    # Starlette's default 500 response is plain text ("Internal Server Error"),
    # which makes the kiosk/admin JS (`res.json()`) throw:
    #   Unexpected token 'I', "Internal S"... is not valid JSON
    # Keep API responses JSON, and keep non-API routes readable.
    logger.exception("Unhandled exception: %s %s", request.method, request.url.path)
    if request.url.path.startswith("/api/"):
        return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})
    return HTMLResponse(content="Internal Server Error", status_code=500)


@app.on_event("startup")
def _startup() -> None:
    # Daily 03:00 local backup + retention cleanup (MVP).
    start_maintenance()


@app.get("/", response_class=HTMLResponse)
def kiosk_screen(request: Request, db: Session = Depends(get_db)):
    employees = (
        db.execute(select(Employee).where(Employee.is_active.is_(True), Employee.termination_date.is_(None)).order_by(Employee.name))
        .scalars()
        .all()
    )
    resp = templates.TemplateResponse(
        "kiosk.html",
        {
            "request": request,
            "employees": employees,
            "device_id": settings.kiosk_device_id,
        },
    )
    # Prevent iPad/Safari from reusing stale HTML (important for kiosk/admin UI tweaks).
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.get("/sw.js")
def service_worker() -> FileResponse:
    resp = FileResponse(
        path=str(STATIC_DIR / "sw.js"),
        media_type="application/javascript",
    )
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Service-Worker-Allowed"] = "/"
    return resp
