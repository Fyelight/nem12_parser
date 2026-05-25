from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Database
    postgres_url: str = "none"

    # File path for testing
    input_file_path: str = "tests/nem12_csv/nem12_test.csv"
    output_folder_path: str = "sql_chunks"
    state_file_path: str = "state.txt"

    # Dead-Letter Queue Output Files
    dlq_report_csv: str = "corrupted_lines.csv" # For human analytics review
    dlq_replay_csv: str = "replay_failed_records.csv" # raw lines ready to be re-run later after fixing errors

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore" # Safely bypasses non-pipeline settings if present
    )

settings = Settings()
