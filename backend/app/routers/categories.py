from fastapi import APIRouter

from app.services.marketplace_categories_service import list_categories_for_api

router = APIRouter(prefix="/categories", tags=["categories"])


@router.get("")
def list_categories() -> dict:
    """Built-in Marketplace categories (slug + label) for settings UI."""
    return {"categories": list_categories_for_api()}
