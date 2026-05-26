import glob
import os
import queue
import sys
import threading
import psycopg

from services.csv_to_sql_convertor import generate_sql_file
from utils.datetime_formatter import format_date
from utils.checkpointer import State
from utils.sql_writer import SQLWriter
import config.settings as config

# Configuration
CSV_INPUT = config.settings.input_folder_path
SQL_OUTPUT = config.settings.output_folder_path
SOURCE_TYPE = config.settings.source_type.lower()
COMPLETED_FILES_TRACKER = config.settings.completed_files_tracker
BATCH_SIZE = config.settings.batch_size
STATE_FILE = config.settings.state_file_path
DLQ_REPORT_CSV = config.settings.dlq_report_csv
DLQ_REPLAY_CSV = config.settings.dlq_replay_csv

def main():
    print("==============================")
    print("   NEM12 MULTI-FILE RUNNER    ")
    print("==============================")
    print(f"Target Directory: '{CSV_INPUT}/'")
    print(f"Output Chunks   : '{SQL_OUTPUT}/'")
    print(f"Batch Aggregation: at least {BATCH_SIZE} rows per chunk or final flush if fewer.")
    print("--------------------------------------------------")

    # 1. Discover all target CSV files in the folder workspace
    search_path = os.path.join(CSV_INPUT, "*.csv")
    csv_files = sorted(glob.glob(search_path))

    if not csv_files:
        print(f"[ABORTED] No CSV files found inside the target directory: '{CSV_INPUT}/'")
        print("Please place your NEM12 feed files inside that folder workspace directory.")
        sys.exit(0)

    required_directories = [
        SQL_OUTPUT,                                      # SQL chunks folder
        os.path.dirname(STATE_FILE),                     # State tracking folder
        os.path.dirname(COMPLETED_FILES_TRACKER),        # Progress ledger log folder
        os.path.dirname(DLQ_REPORT_CSV),                 # DLQ reports folder
        os.path.dirname(DLQ_REPLAY_CSV)                  # DLQ replay queue folder
    ]

    # Dynamically verify and generate folders for every configured output target path
    for directory in required_directories:
        if directory:  # Only attempt to create if a parent directory string actually exists
            os.makedirs(directory, exist_ok=True)

    # 2. Load the structural pipeline checkpoint state
    state = State.load_from_file(STATE_FILE)
    interrupted_file = state.current_file
    last_position = state.last_byte_position

    # Safeguard: If the system crashed mid-file but someone deleted that file during downtime
    if interrupted_file:
        if not os.path.exists(interrupted_file):
            print(f"[WARNING] Disrupted checkpoint target found for '{interrupted_file}', but file is missing. Resetting state.")
            interrupted_file = None
            last_position = 0
            state.reset_current_file()
            state.save_to_file(STATE_FILE)

    # 3. Initialize operational processing assets
    batch_values = []
    batch_queue = queue.Queue(maxsize=20)
    offset_tracker = {"last_seen": last_position}

    try:
        with psycopg.connect(config.settings.postgres_url, autocommit=True, cursor_factory=psycopg.ClientCursor) as conn, \
             open(COMPLETED_FILES_TRACKER, "a", encoding="utf-8") as audit_log:

            cur = conn.cursor()
            writer = SQLWriter(cursor=cur, output_dir=config.settings.output_folder_path)

            # Fast-forward chunk index tracking if recovering mid-stream
            if last_position > 0 and interrupted_file:
                writer.synchronize_chunk_index()

            # --- THREAD-SAFE CONTEXT POINTER ---
            current_active_file = [interrupted_file]

            def threaded_progress_callback(position):
                """The background thread executes this safely to checkpoint progress."""
                if current_active_file[0]:
                    state.current_file = current_active_file[0]
                    state.last_byte_position = position
                    state.save_to_file(STATE_FILE)

            # Launch the background writer worker once
            consumer_thread = threading.Thread(
                target=writer.start_worker_consumer,
                args=(batch_queue, threaded_progress_callback)
            )
            consumer_thread.daemon = True
            consumer_thread.start()

            print(f"Found {len(csv_files)} total datasets. Processing execution queue...")

            # 4. Process the discovery queue sequentially
            for index, file_path in enumerate(csv_files, start=1):
                file_name = os.path.basename(file_path)

                # Skip completely finished targets across state architecture
                if file_path in state.completed_files:
                    print(f"[{index}/{len(csv_files)}] Skipping '{file_name}' (Already processed successfully).")
                    continue

                # Recovery Filter: Fast-forward straight to the file we crashed on
                if interrupted_file and file_path != interrupted_file:
                    print(f"[{index}/{len(csv_files)}] Skipping '{file_name}' (Fast-forwarding to reach checkpoint file).")
                    continue

                # Clear the crash-recovery tracking flag once targets sync up
                if interrupted_file and file_path == interrupted_file:
                    print(f"[RECOVERY] Found interrupted file target: {file_name}. Resuming from byte {last_position}")
                    interrupted_file = None

                print(f"\n==================================================")
                print(f" PROCESSING FILE [{index}/{len(csv_files)}]: {file_name}")
                print(f"==================================================")

                # Set the dynamic file path for our background tracking callback
                current_active_file[0] = file_path

                # Execute your stream conversion pipeline service
                success = generate_sql_file(
                    source_target=file_path,
                    batch_queue=batch_queue,
                    source_type=SOURCE_TYPE,
                    batch_values=batch_values,
                    offset_tracker=offset_tracker,
                    batch_size=BATCH_SIZE
                )

                if success:
                    print(f"[COMPLETED] Clean compilation execution finished for: {file_name}")

                    audit_log.write(f"{file_path}\n")
                    audit_log.flush() # Ensure immediate disk write for audit log

                    # Mutate state class tracking lists and clear current execution settings
                    state.completed_files.append(file_path)
                    offset_tracker["last_seen"] = 0
                    current_active_file[0] = None
                    state.reset_current_file()
                    state.save_to_file(STATE_FILE)
                else:
                    print(f"[FATAL FAILURE] Ingestion halted on dataset: {file_name}")
                    sys.exit(1)

            # 5. Post-Execution Pipeline Cleanup Flushes
            if batch_values:
                print(f"[Flushing] Final lingering batch holding {len(batch_values)} entries...")
                batch_queue.put((batch_values.copy(), offset_tracker["last_seen"]))
                batch_values.clear()

            # Signal the background database consumer thread to wind down gracefully
            print("[Producer] All files processed successfully. Flushing remaining database streams...")
            batch_queue.put(None)
            batch_queue.join()
            consumer_thread.join()

        print("\n==================================================")
        print("[SUCCESS] All discovered CSV datasets fully ingested!")
        print(f"Your chunk packages are waiting in: '{config.settings.output_folder_path}/'")
        print("==================================================")

    except KeyboardInterrupt:
        print("\n[PAUSED] Processing sequence paused by user command safely via terminal console execution rules.")
        sys.exit(0)

if __name__ == "__main__":
    main()
