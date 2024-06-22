import sqlite3
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("migration.log"),
        logging.StreamHandler()
    ]
)
# SQLite database path
sqlite_db = 'my_database.db'

# PostgreSQL connection parameters
pg_user = os.getenv('DB_USER')
pg_password = os.getenv('DB_PASSWORD')
# migration_db
try:
    # Connect to SQLite
    sqlite_conn = sqlite3.connect(sqlite_db)
    sqlite_cursor = sqlite_conn.cursor()
    logging.info("Connected to SQLite database.")

    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        dbname="migration_db",
        user=pg_user,
        password=pg_password,
        host="localhost",
        port="5432"
    )
    pg_cursor = pg_conn.cursor()
    logging.info("Connected to PostgreSQL database.")
except Exception as e:
    logging.error(f"Error connecting to databases: {e}")
    raise

try:
    # Fetch tables from SQLite
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = sqlite_cursor.fetchall()
    logging.info(f"Found tables in SQLite: {tables}")

    # Function to create table in PostgreSQL
    def create_table_in_pg(table_name, columns):
        try:
            create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({fields})").format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(
                    sql.SQL('{} {}').format(sql.Identifier(col[1]), sql.SQL(col[2])) for col in columns
                )
            )
            pg_cursor.execute(create_table_query)
            pg_conn.commit()
            logging.info(f"Created table {table_name} in PostgreSQL.")
        except Exception as e:
            logging.error(f"Error creating table {table_name} in PostgreSQL: {e}")

    # Function to migrate data from SQLite to PostgreSQL
    def migrate_table(table_name):
        try:
            # Fetch columns from SQLite
            sqlite_cursor.execute(f"PRAGMA table_info({table_name});")
            columns = sqlite_cursor.fetchall()

            # Create table in PostgreSQL
            create_table_in_pg(table_name, columns)

            # Fetch data from SQLite table
            sqlite_cursor.execute(f"SELECT * FROM {table_name};")
            rows = sqlite_cursor.fetchall()

            # Insert data into PostgreSQL table
            insert_query = sql.SQL("INSERT INTO {table} VALUES ({values})").format(
                table=sql.Identifier(table_name),
                values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )
            pg_cursor.executemany(insert_query, rows)
            pg_conn.commit()
            logging.info(f"Migrated table {table_name} from SQLite to PostgreSQL.")
        except Exception as e:
            logging.error(f"Error migrating table {table_name} from SQLite to PostgreSQL: {e}")

    # Migrate each table
    for table in tables:
        table_name = table[0]
        migrate_table(table_name)

except Exception as e:
    logging.error(f"Error during migration: {e}")
finally:
    # Close connections
    sqlite_conn.close()
    pg_conn.close()
    logging.info("Closed database connections.")