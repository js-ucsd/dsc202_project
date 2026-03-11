from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Dataset
    dblp_csv_path: str = "data/raw/dblp-v10.csv"
    dblp_parquet_path: str = "data/raw/dblp-v10.parquet"

    # Postgres
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "papers"
    postgres_user: str = "papers"
    postgres_password: str = "papers"

    # Neo4j
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password123"

    # Qdrant
    qdrant_url: str = "http://localhost:6333"
    qdrant_collection: str = "papers_vectors"

    # Embeddings
    embedding_provider: str = "fastembed"
    fastembed_model: str = "BAAI/bge-small-en-v1.5"

    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} password={self.postgres_password}"
        )


settings = Settings()

