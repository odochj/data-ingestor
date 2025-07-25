from readers.csv_reader import csv_reader
from sources.source import Source, SourceType
from secret_handling.secret import SecretType

path = SecretType.FILE_PATH

class ReaderFactory:
    @staticmethod
    def get_reader(source: Source):
        secrets = source.secrets
        if source.source_type == SourceType.CSV:
            return csv_reader(secrets.get_required_keys(path))

        else:
            raise ValueError(f"Unsupported source type: {source.source_type}")
