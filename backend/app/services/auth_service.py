from datetime import datetime, timedelta, timezone

import bcrypt
from jose import jwt
from pymongo.database import Database

from app.config import settings
from app.domain import User
from app.repositories.user_repository import UserRepository


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except ValueError:
        return False


def create_access_token(subject: str, user_id: int, role: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
    payload = {"sub": subject, "uid": user_id, "role": role, "exp": expire}
    return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)


def decode_token(token: str) -> dict:
    return jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])


def authenticate_user(db: Database, username: str, password: str) -> User | None:
    repo = UserRepository(db)
    user = repo.get_by_username(username)
    if not user or not verify_password(password, user.password_hash):
        return None
    return user
