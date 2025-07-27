from enum import Enum
from dataclasses import dataclass
from secret_handling.manager import SecretManager

class SecretType(Enum):
    TOKEN = "token"
    USERNAME = "username"
    PASSWORD = "password"
    DB_CONNECTION = "db_connection"
    FILE_PATH = "file_path"
    URL = "url"

@dataclass
class Secret:
    keys: dict[str, SecretType] 

    def get_required_keys(self, type: SecretType) -> list[str]:
        manager = SecretManager()
        for k, v in self.keys.items():
            if v == type:
                return manager.resolve(key=k)
        return None
    
