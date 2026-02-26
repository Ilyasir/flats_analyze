from app.base import BaseRepository
from app.users.models import User


class UserRepository(BaseRepository):
    model = User
