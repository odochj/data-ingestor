import os
from typing import Optional

class SecretManager:
    def __init__(self, backend: str = "env"):
        self.backend = backend

    def get(self, key: str) -> Optional[str]:
        if self.backend == "env":
            return os.getenv(key)
        raise NotImplementedError(f"Backend '{self.backend}' not supported.")

    def resolve(self, key: str) -> str:
        return self.get(key)


