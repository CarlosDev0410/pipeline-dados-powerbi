import os

def get_postgres_connection():
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("DATABASE_URL"),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        port=os.environ.get("PGPORT", 5432)
    )