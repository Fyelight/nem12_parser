import glob
import os
import queue
import sys
from services.csv_to_sql_convertor import generate_sql_file
from utils.datetime_formatter import format_date
from utils.checkpointer import get_last_byte_position, save_byte_position
import config.settings as config

# Configuration
CSV_INPUT = config.settings.input_folder_path
SOURCE_TYPE = config.settings.source_type.lower()
COMPLETED_FILES_TRACKER = config.settings.completed_files_tracker

batch_queue = queue.Queue(maxsize=20)

def main():
    print("==============================")
    print("   NEM12 MULTI-FILE RUNNER    ")
    print("==============================")
    print(f"Target Directory: '{CSV_INPUT}/'")
    print(f"Output Chunks   : '{config.settings.output_folder_path}/'")
    print(f"Batch Aggregation: {config.settings.batch_size} rows per chunk")
    print("--------------------------------------------------")

    # 1. Scan directory patterns for all active CSV files
    search_path = os.path.join(CSV_INPUT, "*.csv")
    csv_files = sorted(glob.glob(search_path))

    if not csv_files:
        print(f"[ABORTED] No CSV files found inside the target directory: '{CSV_INPUT}/'")
        print("Please place your NEM12 feed files inside that folder workspace directory.")
        sys.exit(0)

    print(f"Found {len(csv_files)} target file datasets to process.")

    # 2. Track global system progress across the multi-file stream
    # Check if our state file contains information about which file we were reading during a crash
    last_position = get_last_byte_position()

    # We can read a small companion file or track progress based on completed targets
    completed_set = set()
    if os.path.exists(COMPLETED_FILES_TRACKER):
        with open(COMPLETED_FILES_TRACKER, "r", encoding="utf-8") as log:
            completed_set = set(line.strip() for line in log if line.strip())

    try:
        for index, file_path in enumerate(csv_files, start=1):
            file_name = os.path.basename(file_path)

            # Skip files that have completely finished processing in an earlier run cycle
            if file_path in completed_set:
                print(f"[{index}/{len(csv_files)}] Skipping '{file_name}' (Already processed successfully).")
                continue

            print(f"\n==================================================")
            print(f" PROCESSING FILE [{index}/{len(csv_files)}]: {file_name}")
            print(f"==================================================")

            # Fire your highly optimized concurrent streaming pipeline function for this target file
            success = generate_sql_file(
                source_target=file_path,
                source_type=SOURCE_TYPE,
            )

            if success:
                print(f"[COMPLETED] Clean compilation execution finished for: {file_name}")
                # Log completion metadata so this specific file is never re-read on restart
                with open(COMPLETED_FILES_TRACKER, "a", encoding="utf-8") as log:
                    log.write(f"{file_path}\n")

                # Clear the local byte checkpoint file since the next file starts at position 0
                save_byte_position(0)
            else:
                print(f"[FATAL FAILURE] Ingestion halted on dataset: {file_name}")
                sys.exit(1)

        print("\n==================================================")
        print("[SUCCESS] All discovered CSV datasets fully ingested!")
        print(f"Your optimized chunk packages are waiting in: '{config.settings.output_folder_path}/'")
        print("==================================================")

    except KeyboardInterrupt:
        print("\n[PAUSED] Processing sequence paused by user command safely via terminal console execution rules.")
        sys.exit(0)

if __name__ == "__main__":
    main()
