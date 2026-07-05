from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DB
from infrastructure.contracts.user_profile_repository import UserProfileRepository

class MongoUserProfileRepository(UserProfileRepository):

    def __init__(self) -> None:
        self._client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        self._col    = self._client[MONGO_DB]["user_profiles"]

    def find_by_id(self, usuario_id: str) -> dict:
        doc = self._col.find_one(
            {"usuarioID": usuario_id},
            {"promedio_de_gastos": 1, "varianza_de_gastos": 1, "_id": 0}
        )
        return doc or {}