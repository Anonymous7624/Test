from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models import Listing, User, UserRole, UserSettings


class UserRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_by_username(self, username: str) -> User | None:
        return self.db.scalar(select(User).where(User.username == username))

    def get_by_id(self, user_id: int) -> User | None:
        return self.db.get(User, user_id)

    def create(self, username: str, password_hash: str, role: str = UserRole.user.value) -> User:
        user = User(username=username, password_hash=password_hash, role=role)
        self.db.add(user)
        self.db.flush()
        settings = UserSettings(user_id=user.id)
        self.db.add(settings)
        self.db.commit()
        self.db.refresh(user)
        return user

    def list_all(self) -> list[User]:
        return list(self.db.scalars(select(User).order_by(User.id)))

    def delete(self, user: User) -> None:
        self.db.execute(delete(Listing).where(Listing.user_id == user.id))
        self.db.delete(user)
        self.db.commit()
