from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import require_admin
from app.models import User, UserRole
from app.repositories.user_repository import UserRepository
from app.schemas import AdminUserCreate, AdminUserOut, AdminUserUpdate
from app.services.auth_service import hash_password

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/users", response_model=list[AdminUserOut])
def list_users(
    _: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> list[AdminUserOut]:
    repo = UserRepository(db)
    return [AdminUserOut.model_validate(u) for u in repo.list_all()]


@router.post("/users", response_model=AdminUserOut)
def create_user(
    body: AdminUserCreate,
    _: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> AdminUserOut:
    repo = UserRepository(db)
    if repo.get_by_username(body.username):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username taken")
    user = repo.create(body.username, hash_password(body.password), role=body.role)
    return AdminUserOut.model_validate(user)


@router.patch("/users/{user_id}", response_model=AdminUserOut)
def update_user(
    user_id: int,
    body: AdminUserUpdate,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> AdminUserOut:
    repo = UserRepository(db)
    user = repo.get_by_id(user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    if body.role is not None:
        user.role = body.role
    if body.password is not None:
        user.password_hash = hash_password(body.password)
    db.add(user)
    db.commit()
    db.refresh(user)
    _ = admin
    return AdminUserOut.model_validate(user)


@router.delete("/users/{user_id}")
def delete_user(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    if user_id == admin.id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete self")
    repo = UserRepository(db)
    user = repo.get_by_id(user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    if user.role == UserRole.admin.value:
        # prevent removing last admin in production — soft check: allow if another admin exists
        others = [u for u in repo.list_all() if u.id != user_id and u.role == UserRole.admin.value]
        if not others:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete last admin")
    repo.delete(user)
    return {"ok": True}
