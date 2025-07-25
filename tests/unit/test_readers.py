from data_ingestor.readers.pdf_reader import PDFReader
from pathlib import Path

directory = Path('/Users/jamesodoch/Documents/svour/financial_monitor/data/reciepts/sainsburys')

files = list(directory.rglob('*'))

