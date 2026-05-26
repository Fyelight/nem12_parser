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

STATE_FILE_PATH=output/state.txt

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
# 1. Delete the completion tracking logs at COMPLETED_FILES_TRACKER so Python forgets it processed the file
rm processed_files.log

# 2. Reset your checkpointer file at STATE_FILE_PATH to byte position 0
echo "0" > state.txt
```

