from fastapi import APIRouter

from app.services.categories_service import load_categories

router = APIRouter(prefix="/categories", tags=["categories"])


@router.get("")
def list_categories() -> dict:
    return load_categories()
