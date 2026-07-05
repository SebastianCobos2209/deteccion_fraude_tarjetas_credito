from abc import ABC, abstractmethod
from typing import Optional

class UserProfileRepository(ABC):
    @abstractmethod
    def find_by_id(self, usuario_id: str) -> Optional[dict]:
        """Retorna el perfil del usuario o None si no existe."""
        ...