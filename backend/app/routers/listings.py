from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_user
from app.models import User
from app.repositories.listing_repository import ListingRepository
from app.schemas import ListingOut

router = APIRouter(prefix="/listings", tags=["listings"])


@router.get("", response_model=list[ListingOut])
def list_listings(
    profitable_only: bool | None = Query(default=None),
    category: str | None = Query(default=None),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[ListingOut]:
    _ = user
    repo = ListingRepository(db)
    rows = repo.list_filtered(profitable_only=profitable_only, category_slug=category)
    return [ListingOut.model_validate(r) for r in rows]
