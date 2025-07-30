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

    def get_required_key(self, type: SecretType) -> str:
        manager = SecretManager()
        for k, v in self.keys.items():
            if v == type:
                val = manager.resolve(key=k)
                if not val:
                    raise ValueError(f"Environment variable '{k}' for secret type '{type}' is not set.")
                return val
        raise ValueError(f"No secret key found for type '{type}'")
    
