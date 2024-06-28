# SQLite to PostgreSQL Migration

## Overview

This project demonstrates the migration of data from an SQLite database to a PostgreSQL database. The script provided performs the following tasks:

1. **Connect to SQLite and PostgreSQL Databases**: The script connects to an SQLite database and a PostgreSQL database using credentials stored in environment variables.
2. **Fetch Tables from SQLite**: It fetches the list of tables present in the SQLite database.
3. **Create Corresponding Tables in PostgreSQL**: For each table in the SQLite database, it creates a corresponding table in the PostgreSQL database.
4. **Migrate Data**: The script migrates data from each table in the SQLite database to the corresponding table in the PostgreSQL database.
5. **Logging**: It logs the process of connecting to the databases, fetching tables, creating tables in PostgreSQL, and migrating data. Any errors encountered during the process are also logged.
