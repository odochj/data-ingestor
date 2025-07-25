from sources.source import Source, User, SourceType
from secret_handling.secret import Secret, SecretType
from tags.tag_registry import Transactions

SOURCES = [
     Source(
        name = "MockBank",
        tag = Transactions,
        user = User.JAMES,
        source_type = SourceType.CSV,
        secrets=Secret(keys = {
             "MOCK_BANK_PATH":SecretType.FILE_PATH
        }),
        key_columns= [
            "date",
            "value",
            "description"
        ],
    ),
]