from pymongo.database import Database

from app.config import settings
from app.models import UserRole
from app.repositories.user_repository import UserRepository
from app.services.auth_service import hash_password


def seed_default_admin(db: Database) -> None:
    repo = UserRepository(db)
    existing = repo.get_by_username(settings.admin_username)
    if existing:
        return
    repo.create(
        settings.admin_username,
        hash_password(settings.admin_password),
        role=UserRole.admin.value,
    )
