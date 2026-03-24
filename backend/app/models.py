import enum


class UserRole(str, enum.Enum):
    admin = "admin"
    user = "user"


class AlertStatus(str, enum.Enum):
    none = "none"
    pending = "pending"
    sent = "sent"
    skipped = "skipped"
