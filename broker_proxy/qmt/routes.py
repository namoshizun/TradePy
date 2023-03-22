from fastapi import APIRouter


router = APIRouter()


@router.get("/positions")
async def get_positions():
    return None


@router.get("/orders")
async def get_orders():
    return None


@router.get("/account")
async def get_account_info():
    return None
