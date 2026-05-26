nem12 csv convertor to sql insert file(s)

## installation
```
pip install -r requirements.txt
```

## Configuration
```
## .env
POSTGRES_URL=postgresql://postgres:secret@localhost:5432/nem12_db

INPUT_FOLDER_PATH=tests/nem12_csv

OUTPUT_FOLDER_PATH=output/sql_chunks

STATE_FILE_PATH=output/state.json

COMPLETED_FILES_TRACKER=output/processed_files.log

BATCH_SIZE=5000

DLQ_REPORT_CSV=output/corrupted_lines.csv

DLQ_REPLAY_CSV=output/replay_failed_records.csv

## or you can update config/settings.py
```

## Input files
Put the csv in a folder INPUT_FOLDER_PATH

## Run
```
## at root folder .
python main.py
```

## Output files
SQL output chunks in OUTPUT_FOLDER_PATH
STATE_FILE_PATH checkpointer file to continue processing if stopped
COMPLETED_FILES_TRACKER tracks csv files processed
DLQ_REPORT_CSV for error tracking
DLQ_REPLAY_CSV to rerun after fixing error rows


## Rerun reset
```
# Delete the state file. When Python restarts, it will auto-generate a fresh, empty state.
rm state.json

# or update state.json to control what to re-process
{
    "current_file": "./tests/nem12_csv/massive_feed_3.csv",
    "last_byte_position": 0,
    "completed_files": [
        "./tests/nem12_csv/massive_feed_1.csv",
        "./tests/nem12_csv/massive_feed_2.csv"
    ]
}

# (Optional) Wipe your passive history text ledger just to keep things clean
rm processed_files.log
```

