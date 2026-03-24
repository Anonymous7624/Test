from fastapi import APIRouter, Depends, Query
from pymongo.database import Database

from app.database import get_db
from app.deps import get_current_user
from app.domain import User
from app.repositories.listing_repository import ListingRepository
from app.schemas import ListingOut

router = APIRouter(prefix="/listings", tags=["listings"])


@router.get("", response_model=list[ListingOut])
def list_listings(
    profitable_only: bool | None = Query(default=None),
    category: str | None = Query(default=None),
    user: User = Depends(get_current_user),
    db: Database = Depends(get_db),
) -> list[ListingOut]:
    repo = ListingRepository(db)
    rows = repo.list_filtered(
        user_id=user.id,
        profitable_only=profitable_only,
        category_slug=category,
    )
    return [ListingOut.model_validate(r) for r in rows]
