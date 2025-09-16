# Data Ingestor

**What if *any* data source could become a Data Product?**

**Data Ingestor** is  multi-purpose data ingestion tool, used to write data from a variety of sources to a SQL Database.

A previous iteration was an ETL tool, pre-empting transformations via POLARS using knowledge of specific source formats in order to load data in an analytics-ready state

In order to scale this to more formats and use cases, this version will pivot to ELT - with this tool focused solely on Extraction and Loading, with the majority of the transformations being handled by the **[Data Transformer](https://github.com/odochj/data-transformer)**

## Overview

Data Ingestor relies on the underlying assumption that a `Source` is being a ingested *for a reason*. If a `Source` does not - or ever ceases to - contain the data points necessary for your use case, the ingestion should fail.

We can also assume that any given use case could involve combining data from multiple `Sources`. As such, we need to be able to `Tag` a `Source` based on its function. These Tags will assert the bare minimum data points required for a `Source` to remain functional downstream.

Lastly, to ensure uniqueness and idempotence, the ingestor must be able to define both `Primary Keys` and `Row Hashes` for a given source prior to ingestion. Both components allow for a `Data Vault`-style ingestion of `Sources` into tables that closely resemble `Hubs` (where `Primary Keys` are stored) and `Satellites` (where source data is stored in its entirety, and `Row Hashes` are used to determine if a given row has changed between runs). If the `Source` schema has changed for any reason, the data will be loaded into a sepaate "`Satellite`", though the "`Hub`" will remain the same.

## Process
1. For each `Tag`, create a new "`Hub`" doesn't already exist
2. Append all new `Primary Keys` to an existing "`Hub`"
3. For each `Source`, create a "`Satellite`" for each unique schema configuration, named with a creation timestamp for versioning (if they don't already exist)
4. Append all new `Source` data to appropriate tables

## Tags

`Tags` will be defined iteratively, as `Sources` and use cases become apparent. Given that I am making this projet to manage my personal data, the broader structure will just be... the things that I like, interact with, or otherwise happen to have data for. Probably typical of most people. 

Tags are defined as a [class](tags/tag.py) and are instantiated in a [registry](tags/tag_registry.py)

Tags will likely include:
- Transactions ✅
- Workouts 
- Sleep
- TV and Film
- Music 


## Project Structure
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
│   ├── bank.csv (mock data)
│   ├── receipts/
│   │   └── ...
│   ├── transactions/
│   │   └── ...
│   └── workouts/
│       └── ...
└── test_output.duckdb
```




