import duckdb
import polars as pl
from pathlib import Path
from dotenv import load_dotenv
from data_ingestor.secrets.ingestion_service import IngestionService 


# Load environment variables
load_dotenv()

def test_amex(tmp_path):
    # Setup test CSV folder
    csv_folder = tmp_path / "Amex"
    csv_folder.mkdir()
    sample_csv = csv_folder / "sample.csv"
    source = "Amex"
    holder = "test"
     
    sample_csv.write_text("Date,Description,Amount\n2024-01-01,Maeme,100\n2024-01-02,Tugu,200\n2024-01-03,Mapendiz,300")

    # Setup DuckDB test file path
    db_path = tmp_path / "test.duckdb"
    table_name = "test_transactions"


    # Run the ingestion
    service = IngestionService(
        csv_folder=csv_folder,
        db_path=str(db_path),
        table_name=table_name,
        source=source,
        holder=holder,
        write_mode="merge"
    )
    service.run()

    # Verify results in DuckDB
    con = duckdb.connect(str(db_path))
    result = con.execute(f"SELECT * FROM {table_name}").fetch_df()
    con.close()

    assert result.shape[0] == 3
    assert "date" in result.columns
    assert "amount" in result.columns
    assert result["amount"].tolist() == [100, 200, 300]


import duckdb
import polars as pl
from pathlib import Path
from dotenv import load_dotenv
from data_ingestor.secrets.ingestion_service import IngestionService 



def test_monzo(tmp_path):
    # Setup test CSV folder
    csv_folder = tmp_path / "Monzo"
    csv_folder.mkdir()
    sample_csv = csv_folder / "sample.csv"
    source = "Monzo"
    holder = "test"
     
    sample_csv.write_text(
    """Transaction ID,Date,Time,Type,Name,Emoji,Category,Amount,Currency,Local amount,Local currency,Notes
txn_001,2024-05-01,09:30,Card Payment,Coffee Shop,☕,Food & Drink,-4.50,USD,-4.50,USD,Morning coffee
txn_002,2024-05-01,12:45,Card Payment,Bookstore,📚,Shopping,-15.99,USD,-15.99,USD,Bought a novel
txn_003,2024-05-02,18:10,ATM Withdrawal,ATM,🏧,Cash,-100.00,USD,-100.00,USD,Weekend cash
"""
    )

    # Setup DuckDB test file path
    db_path = tmp_path / "test.duckdb"
    table_name = "test_transactions"


    # Run the ingestion
    service = IngestionService(
        csv_folder=csv_folder,
        db_path=str(db_path),
        table_name=table_name,
        source=source,
        holder=holder,
        write_mode="merge"
    )
    service.run()

    # Verify results in DuckDB
    con = duckdb.connect(str(db_path))
    result = con.execute(f"SELECT * FROM {table_name}").fetch_df()
    con.close()

    assert result.shape[0] == 3
    assert "date" in result.columns
    assert "amount" in result.columns
    assert result["transaction_id"].tolist() == ["txn_001", "txn_002", "txn_003"]

def test_barclays(tmp_path):
    # Setup test CSV folder
    csv_folder = tmp_path / "Monzo"
    csv_folder.mkdir()
    sample_csv = csv_folder / "sample.csv"
    source = "Barclays"
    holder = "test"
     
    sample_csv.write_text(
    """Number,Date,Account,Amount,Subcategory,Memo
1001,2024-06-01,Checking,-120.50,Utilities,Electricity bill
1002,2024-06-02,Credit Card,-45.00,Entertainment,Movie night
1003,2024-06-03,Savings,500.00,Income,Salary deposit
"""
    )

    # Setup DuckDB test file path
    db_path = tmp_path / "test.duckdb"
    table_name = "test_transactions"


    # Run the ingestion
    service = IngestionService(
        csv_folder=csv_folder,
        db_path=str(db_path),
        table_name=table_name,
        source=source,
        holder=holder,
        write_mode="merge"
    )
    service.run()

    # Verify results in DuckDB
    con = duckdb.connect(str(db_path))
    result = con.execute(f"SELECT * FROM {table_name}").fetch_df()
    con.close()

    assert result.shape[0] == 3
    assert "date" in result.columns
    assert "amount" in result.columns
    assert result["transaction_id"].tolist() == [ 
        "2024-06-01-Electricity bill--120.5-Utilities-Barclays-test",
        "2024-06-02-Movie night--45.0-Entertainment-Barclays-test",
        "2024-06-03-Salary deposit-500.0-Income-Barclays-test"
    ]


