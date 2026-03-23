from sqlalchemy.orm import Session

from app.config import settings
from app.models import User, UserRole
from app.repositories.user_repository import UserRepository
from app.services.auth_service import hash_password


def seed_default_admin(db: Session) -> None:
    repo = UserRepository(db)
    existing = repo.get_by_username(settings.admin_username)
    if existing:
        return
    repo.create(
        settings.admin_username,
        hash_password(settings.admin_password),
        role=UserRole.admin.value,
    )
