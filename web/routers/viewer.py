"""決算分析リアルタイムビューア - ビューアルーター"""
from datetime import date
from pathlib import Path
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from web.services.financial_service import get_viewer_data, get_detail_data, get_available_dates, get_financial_history

router = APIRouter()

WEB_DIR = Path(__file__).parent.parent
templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """メイン画面（フルHTML）"""
    today = date.today().isoformat()
    available_dates = get_available_dates()
    rows = get_viewer_data(today)
    return templates.TemplateResponse(
        "viewer.html",
        {
            "request": request,
            "rows": rows,
            "target_date": today,
            "available_dates": available_dates,
            "types": ["earnings", "revision", "dividend", "other"],
            "sort": "time",
            "order": "desc",
        },
    )


@router.get("/viewer/table", response_class=HTMLResponse)
async def viewer_table(
    request: Request,
    date: str = Query(default=None),
    types: str = Query(default=None),
    sort: str = Query(default="time"),
    order: str = Query(default="desc"),
):
    """htmx用テーブル本体パーシャル"""
    target_date = date or __import__("datetime").date.today().isoformat()
    type_list = types.split(",") if types else None
    rows = get_viewer_data(target_date, types=type_list, sort=sort, order=order)
    return templates.TemplateResponse(
        "partials/table_body.html",
        {
            "request": request,
            "rows": rows,
            "target_date": target_date,
            "sort": sort,
            "order": order,
        },
    )


@router.get("/viewer/detail/{ticker}/{date}", response_class=HTMLResponse)
async def viewer_detail(request: Request, ticker: str, date: str):
    """展開行パーシャル"""
    detail = get_detail_data(ticker, date)
    return templates.TemplateResponse(
        "partials/detail_row.html",
        {
            "request": request,
            "detail": detail,
            "ticker": ticker,
            "target_date": date,
        },
    )


@router.get("/viewer/financial-detail/{ticker}", response_class=HTMLResponse)
async def financial_detail(request: Request, ticker: str):
    """業績詳細パネル（htmxパーシャル）"""
    data = get_financial_history(ticker)
    return templates.TemplateResponse(
        "partials/financial_detail.html",
        {"request": request, "data": data},
    )
