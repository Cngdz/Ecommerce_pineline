# Ecommerce Data Pipeline

This project implements a data pipeline for processing raw ecommerce data using Python, Pandas, SQLAlchemy, Airflow, and dbt.

## Features

- Loads raw CSV data files into a PostgreSQL database.
- Automatically creates tables and schema for each dataset.
- Workflow orchestration with Apache Airflow.
- Data transformation and modeling with dbt.
- Modular and easy to extend for additional data processing steps.

## Prerequisites

- Python 3.7+
- PostgreSQL database
- Required Python packages: `pandas`, `sqlalchemy`, `psycopg2`
- [Apache Airflow](https://airflow.apache.org/) (for workflow orchestration)
- [dbt](https://www.getdbt.com/) (for data transformation)

## Setup

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd ecommerce_pipeline
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install Airflow:**
   Follow the [official Airflow installation guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html).

4. **Install dbt:**
   Follow the [official dbt installation guide](https://docs.getdbt.com/docs/introduction/installation).

5. **Set environment variables:**
   - `DATABASE_URL`: PostgreSQL connection string (e.g., `postgresql://user:password@localhost:5432/dbname`)
   - `DATA_DIR`: (Optional) Directory containing CSV files (default: `data`)

   Example (Linux/macOS):
   ```bash
   export DATABASE_URL=postgresql://user:password@localhost:5432/dbname
   export DATA_DIR=data
   ```

   Example (Windows):
   ```cmd
   set DATABASE_URL=postgresql://user:password@localhost:5432/dbname
   set DATA_DIR=data
   ```

6. **Prepare your data:**
   - Place your raw CSV files in the `data` directory or the directory specified by `DATA_DIR`.

## Usage

### Load Raw Data

To load raw data into the database, run:

```bash
python scripts/load_raw_data.py
```

This script will:
- Read all `.csv` files in the data directory.
- Create a `raw_data` schema if it does not exist.
- Load each CSV file into a corresponding table in the database.

### Orchestrate with Airflow

- Place your DAGs in the `airflow/dags/` directory.
- Start the Airflow webserver and scheduler:
  ```bash
  airflow db init
  airflow webserver
  airflow scheduler
  ```
- Trigger the pipeline from the Airflow UI.

### Transform Data with dbt

- Configure your dbt project in the `dbt/` directory.
- Set up your `profiles.yml` to connect to your database.
- Run dbt models:
  ```bash
  cd dbt
  dbt run
  ```



## License

MIT License
