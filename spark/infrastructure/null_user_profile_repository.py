from infrastructure.contracts.user_profile_repository import UserProfileRepository

class NullUserProfileRepository(UserProfileRepository):
    """Retorna dict vacío cuando MONGO_ENABLED=false."""
    def find_by_id(self, usuario_id: str) -> dict:
        return {}