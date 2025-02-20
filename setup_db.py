"""Simple DB setup script, establishes the schema."""

import os

import dotenv
import psycopg2

dotenv.load_dotenv()


def create_table():
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "github_data"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "postgres"),
    )
    cur = conn.cursor()
    cur.execute(
        """
      CREATE TABLE IF NOT EXISTS repositories (
        repo_id TEXT PRIMARY KEY,
        name_with_owner TEXT NOT NULL,
        stargazer_count INTEGER NOT NULL,
        last_updated TIMESTAMPTZ DEFAULT NOW()
      );
    """
    )
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    create_table()
