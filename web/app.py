"""決算分析リアルタイムビューア - FastAPIアプリケーション"""
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from web.routers import viewer

app = FastAPI(title="決算分析リアルタイムビューア")

# 静的ファイル配信
WEB_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")

# テンプレート
templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))

# ルーター登録
app.include_router(viewer.router)
