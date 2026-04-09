
import snowflake.connector
import pathlib
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


if __name__ == "__main__":
    print(f"SNOWFLAKE_ROLE: {os.getenv('SNOWFLAKE_ROLE')}")
    print(f"SNOWFLAKE_USER: {os.getenv('SNOWFLAKE_USER')}")

    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        authenticator="externalbrowser",
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        database="MODELLING",
        schema="DBT_DEV",
    )

    sql_file_path = pathlib.Path(__file__).parent / "metadata_query.sql"
    output_file = pathlib.Path(__file__).parent / "table_metadata.csv"

    try:
        sql_query = sql_file_path.read_text()
        cur = conn.cursor()
        cur.execute(sql_query)
        df = cur.fetch_pandas_all()
        print(f"Shape: {df.shape}")
        print(df.head())
        df.to_csv(output_file, index=False)
        print(f"\nMetadata extracted to {output_file}")
        print(f"\nNext step: Generate sources.yml file:")
        print(f"  python scripts/sources/2_generate_sources.py")
    except FileNotFoundError:
        print(f"SQL file not found: {sql_file_path}")
        print("Please run script 1a first to generate the metadata query:")
        print("  python scripts/sources/1a_generate_metadata_query.py")
    except Exception as e:
        print(f"Error executing query: {str(e)}")
    finally:
        conn.close()
