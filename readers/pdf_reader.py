# import pdfplumber
# import polars as pl
# from pathlib import Path
# from typing import List

# #TODO: fix scaffold

# # class PDFReader:

# #     def extract_tables_from_pdf(pdf_path: str | Path) -> List[pl.DataFrame]:
# #         """
# #         Extracts all tables from all pages of a PDF and returns them as a list of Polars DataFrames.
        
# #         Args:
# #             pdf_path (str | Path): Path to the PDF file.

# #         Returns:
# #             List[pl.DataFrame]: A list of Polars DataFrames, one for each table found.
# #         """
# #         pdf_path = Path(pdf_path)
# #         dataframes = []

# #         with pdfplumber.open(pdf_path) as pdf:
# #             for page_number, page in enumerate(pdf.pages, start=1):
# #                 tables = page.extract_tables()
# #                 for table_number, table in enumerate(tables, start=1):
# #                     if not table or len(table) < 2:
# #                         continue  # Skip empty or malformed tables

# #                     headers = table[0]
# #                     rows = table[1:]

# #                     # Ensure headers are strings
# #                     headers = [str(h).strip() if h is not None else f"col_{i}" for i, h in enumerate(headers)]

# #                     # Create Polars DataFrame
# #                     try:
# #                         df = pl.DataFrame(rows, schema=headers)
# #                         dataframes.append(df)
# #                     except Exception as e:
# #                         print(f"[WARN] Failed to convert table on page {page_number}, table {table_number}: {e}")

# #         return dataframes
