# Logistics DBT ELT Pipeline

This project demonstrates a complete **ELT pipeline** for logistics shipment data using **dbt**, **Snowflake**, and **Apache Airflow**.

##  Project Overview

A mock logistics dataset is used to simulate real-world scenarios like tracking shipments, senders, and receivers. The goal is to design a modern data pipeline that follows the ELT approach.

##  Tech Stack

- **dbt** – Data modeling and transformation  
- **Snowflake** – Cloud Data Warehouse  
- **Apache Airflow** – Workflow orchestration  
- **Python** – DAG creation and scripting  
- **Draw.io** – ER diagram visualization  

##  Project Structure

```
logistics-dbt-elt/
│   .gitignore
│   README.md
├───dags
│   └───dbt_run_dag.py
├───dataset
│   └───Dataset_Generator_for_DTDC.csv
├───dbt_logistics
│   │   dbt_project.yml
│   ├───analyses
│   ├───logs
│   │   └───dbt.log
│   ├───macros
│   │   ├───generate_schema_name.sql
│   │   └───state_normalize.sql
│   ├───models
│   │   ├───src_dbt_schema.yml
│   │   ├───marts
│   │   │   ├───dimensions
│   │   │   │   ├───dim_billing.sql
│   │   │   │   ├───dim_date.sql
│   │   │   │   ├───dim_receiver.sql
│   │   │   │   ├───dim_sender.sql
│   │   │   │   └───dim_shipment_type.sql
│   │   │   └───fact
│   │   │       └───fct_shipment.sql
│   │   └───staging
│   │       └───stg_logistics.sql
│   ├───seeds
│   ├───snapshots
│   │   ├───receiver_snapshot.sql
│   │   └───sender_snapshot.sql
│   └───tests
├───diagram
│   └───er_diagram.drawio
├───images
│   ├───dag.png
│   ├───er_diagram.drawio.png
│   ├───models_run.png
│   └───tests_run.png
├───logs
│   └───dbt.log
└───snowflake_ddl
    ├───ddl.sql
    ├───load_procedure.sql
    └───stages.sql

```

##  dbt Models

- **Staging Layer**: Cleans and prepares raw data from CSV  
- **Marts Layer**:  
  - **dim_sender**  
  - **dim_receiver**  
  - **dim_billing**  
  - **dim_shipment_type**  
  - **dim_date**  
  - **fact_shipment**  

##  Setup Instructions

1. Create a virtual environment and install dependencies:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

2. Configure your dbt profile:  
Edit the `profiles.yml` file to match your Snowflake credentials.

3. Load data into Snowflake:  
Run the SQL scripts in `snowflake_ddl/` to create the schema and load the CSV data.

4. Run dbt commands:

```bash
dbt seed
dbt run
dbt test
```

5. (Optional) Use Airflow to trigger dbt:  
Ensure Airflow is running and DAGs are loaded from the `dags/` folder.

##  Visuals

- `dag.png` – Airflow DAG  
- `er_diagram.drawio.png` – Entity Relationship Diagram  
- `models_run.png` – dbt model lineage  
- `tests_run.png` – dbt test results  

##  Features

- Modular dbt structure with naming conventions  
- Snapshotting sender/receiver info  
- Incremental models for staging tabel to keep historical data
- Custom macros for schema/environment control  
- Reproducible ER diagram  
- dbt tests for nulls, uniqueness, relationships  

##  Future Enhancements

- Add incremental materialization  
- Integrate Great Expectations for advanced validation  
- Automate documentation deployment  
- Add SLA alerts via Airflow  

##  Contact

For issues or contributions, open a GitHub issue or contact via GitHub.
