# Data Ingestor

A multi-purpose data ingestion tool, used to write data from a variety of sources to a SQL Database.

A previous iteration was an ETL tool, pre-empting transformations via polars using knowledge of specific source formats inorder to load data in an analytics-ready state

In order to scale this to more formats and use cases, this version will pivot to ELT - with this tool focused solely on Extraction and Loading, with minimal Transformation pre-load.

## Pre Ingestion

Rather than needing intimate knowledge of the data sources needed, making bespoke POLARS transformations each time, we will instead:
1. Tag sources with specific types
2. Create Primary Keys for each source, and test for uniquness

## Ingestion
1. For each Tag, create a table if it doesn't already exist
2. Append all new Primary Keys to an existing "Tag table"
3. For each source, create a table for its specific schema configuration, named with a creation timestamp for versioning (if it doesn't already exist)
4. Append all new source data to appropriate tables

## Tags

Tags will be defined iteratively, as sources become apparent. Each Tag will largely resemble the entities within an ontology. 
The broader structure will just be... the things that I like, interact with, or otherwise have to do that I happen to have data for. Probably typical of most people. 

Tags will be managed via a a registry (of Classes, potentially)  

Tags will include:
- Transactions
- Workouts
- Sleep
- TV and Film
- Music 

```text
root/
├── pipeline.py
├── writers/
│   └── duckdb_writer.py
├── secrets/
│   ├── secret.py
│   └── manager.py
├── tags/
│   ├── tag.py
│   └── tag_registry.py
├── tests/
│   └── e2e.py
├── sources/
│   ├── source.py
│   └── source_registry.py
├── readers/
│   ├── reader_factory.py
│   ├── csv_reader.py
│   ├── pdf_reader.py
│   └── api_reader.py
├── data/
│   ├── mock/
│   │   └── ...
│   ├── receipts/
│   │   └── ...
│   ├── transactions/
│   │   └── ...
│   └── workouts/
│       └── ...
└── test_output.duckdb
```




