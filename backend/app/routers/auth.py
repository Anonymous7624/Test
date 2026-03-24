from fastapi import APIRouter, Depends, HTTPException, status
from pymongo.database import Database

from app.database import get_db
from app.deps import get_current_user
from app.domain import User
from app.repositories.user_repository import UserRepository
from app.schemas import DeleteAccountRequest, LoginRequest, LoginResponse, UserPublic
from app.services.auth_service import authenticate_user, create_access_token, verify_password

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
def login(body: LoginRequest, db: Database = Depends(get_db)) -> LoginResponse:
    user = authenticate_user(db, body.username, body.password)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    token = create_access_token(user.username, user.id, user.role)
    return LoginResponse(
        access_token=token,
        user=UserPublic.model_validate(user),
    )


@router.get("/me", response_model=UserPublic)
def me(user: User = Depends(get_current_user)) -> UserPublic:
    return UserPublic.model_validate(user)


@router.post("/delete-account")
def delete_account(
    body: DeleteAccountRequest,
    user: User = Depends(get_current_user),
    db: Database = Depends(get_db),
) -> dict:
    if not verify_password(body.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid password")
    repo = UserRepository(db)
    repo.delete(user)
    return {"ok": True}
