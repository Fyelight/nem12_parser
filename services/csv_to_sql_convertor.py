import csv
import psycopg
import queue
import threading

from adaptors.nem12_stream_adaptor import NEM12StreamAdapter
from datetime import timedelta
from utils.datetime_formatter import format_date
from utils.checkpointer import get_last_byte_position, save_byte_position
from utils.sql_writer import SQLWriter
import config.settings as config

# Configuration
SQL_OUTPUT = config.settings.output_folder_path
DLQ_REPORT_CSV = config.settings.dlq_report_csv
DLQ_REPLAY_CSV = config.settings.dlq_replay_csv
BATCH_SIZE = config.settings.batch_size
SOURCE_TYPE = config.settings.source_type.lower()

batch_queue = queue.Queue(maxsize=20)

def generate_sql_file(source_target: str, source_type=SOURCE_TYPE):
    last_position = get_last_byte_position()
    file_mode = "a" if last_position > 0 else "w"

    print(f"Initializing adapter pipeline for source [{source_type.upper()}]...")

    adapter = NEM12StreamAdapter(source_type, source_target, checkpoint=last_position)
    raw_stream = adapter.get_stream()

    print(f"Starting pipeline: '{source_target}' -> '{SQL_OUTPUT}' (Mode: {file_mode})")
    print(f"Resuming file stream pointer from byte position: {last_position}")

    current_nmi = None
    interval_length = None
    batch_values = []
    total_rows_processed = 0

    with psycopg.connect(config.settings.postgres_url, autocommit=True, cursor_factory=psycopg.ClientCursor) as conn:
        cur = conn.cursor()

        writer = SQLWriter(cursor=cur, output_dir=SQL_OUTPUT)
        if last_position > 0:
            writer.synchronize_chunk_index() # Re-align index counting after a crash resume

        # Open data handlers, reports, and the clean raw replay queue file handler
        with open(DLQ_REPORT_CSV, "a", encoding="utf-8", newline="") as report_file, \
             open(DLQ_REPLAY_CSV, "a", encoding="utf-8", newline="") as replay_file:

            report_writer = csv.writer(report_file)
            replay_writer = csv.writer(replay_file)

            # --- LAUNCH BACKGROUND WRITER WORKER ---
            # Spin up the background consumer thread task
            offset_tracker = {"last_seen": last_position}
            consumer_thread = threading.Thread(
                target=writer.start_worker_consumer, # ◄ Targets the class worker method!
                args=(batch_queue, save_byte_position) # Passes your queue and checkpointer save function
            )
            consumer_thread.daemon = True
            consumer_thread.start()

            def csv_line_generator():
                for binary_line, current_offset in raw_stream:
                    csv_line_generator.offset = current_offset
                    offset_tracker["last_seen"] = current_offset
                    yield binary_line.decode('utf-8', errors='ignore')

            reader = csv.reader(csv_line_generator())

            for row in reader:
                if not row or len(row) == 0:
                    continue

                line_type = row[0].strip()

                if line_type == "100" or line_type == "900":
                    continue

                # --- 200 ROW: Capture Meter Metadata ---
                elif line_type == "200":
                    current_nmi = row[1].strip()[:10]
                    try:
                        # Extract interval length rules from column index 8
                        interval_length = int(row[8].strip())
                    except (ValueError, IndexError):
                        print(f"ERROR: Corrupt structure in 200 row for meter {current_nmi}. Sent to DLQ.")
                        report_writer.writerow([csv_line_generator.offset, "200_CORRUPT_INTERVAL", row])
                        replay_writer.writerow(row) # Saves pure raw line for easy re-running
                        interval_length = None
                        continue

                    print(f"Reading Meter: {current_nmi}")
                    continue

                # --- 300 ROW: Extract Consumption and Flatten Columns ---
                elif line_type == "300":
                    if not current_nmi:
                        report_writer.writerow([csv_line_generator.offset, "300_MISSING_200_HEADER", row])
                        replay_writer.writerow(row)
                        continue

                    if interval_length is None:
                        report_writer.writerow([csv_line_generator.offset, "300_MISSING_INTERVAL_RULE", row])
                        replay_writer.writerow(row)
                        continue

                    # Determine data validation range limits
                    intervals_per_day = 1440 // interval_length
                    max_data_idx = 2 + intervals_per_day

                    # Guard against truncated or cut lines
                    if len(row) < max_data_idx:
                        report_writer.writerow([csv_line_generator.offset, "300_INCOMPLETE_ROW_LENGTH", row])
                        replay_writer.writerow(row)
                        print(f"WARNING: Row too short for date {row[1]}. Sent to DLQ.")
                        continue

                    # --- FIRST PASS: Scan data slots for gaps or corruption ---
                    row_is_corrupt = False
                    error_reason = ""
                    col_idx = 2

                    while col_idx < max_data_idx:
                        val_str = row[col_idx].strip()

                        if val_str == '':
                            row_is_corrupt = True
                            error_reason = f"MISSING_INTERVAL_AT_COLUMN_{col_idx}"
                            break

                        try:
                            float(val_str) # Test if it's a valid number
                        except ValueError:
                            row_is_corrupt = True
                            error_reason = f"CORRUPT_VALUE_'{val_str}'_AT_COLUMN_{col_idx}"
                            break

                        col_idx += 1

                    # IF CORRUPT: Drop the whole line, log to DLQ records, keep stream moving!
                    if row_is_corrupt:
                        report_writer.writerow([csv_line_generator.offset, error_reason, row])
                        replay_writer.writerow(row) # Ready for re-run later
                        continue

                    # --- SECOND PASS: If row is clean, safe processing execution runs ---
                    date_str = row[1].strip()
                    base_date = format_date(date_str)
                    current_time = base_date

                    col_idx = 2
                    while col_idx < max_data_idx:
                        val_str = row[col_idx].strip()
                        consumption = float(val_str)

                        current_time += timedelta(minutes=interval_length)
                        pg_timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")

                        batch_values.append((current_nmi, pg_timestamp, consumption))
                        total_rows_processed += 1
                        col_idx += 1

                    # Trigger database multi-row batch inserts via checkpointer tracking
                    if len(batch_values) >= BATCH_SIZE:
                        batch_queue.put((batch_values, csv_line_generator.offset))
                        batch_values = []

                elif line_type not in ["200", "300", "400", "500"]:
                    report_writer.writerow([csv_line_generator.offset, f"UNRECOGNIZED_INDICATOR_{line_type}", row])
                    replay_writer.writerow(row)
                    continue

            # Flush remaining query blocks at file summary conclusion
            if batch_values:
                batch_queue.put((batch_values, offset_tracker["last_seen"]))

            # --- CONCURRENT TEARDOWN SEQUENCE ---
            print("[Producer] CSV file streaming finished. Flushing remaining queues...")
            # Send the poison pill termination token to safely shutdown background threads
            batch_queue.put(None)

            # Block application closure until the background consumer finishes writing to disk
            batch_queue.join()
            consumer_thread.join()


    print(f"Pipeline Complete! Successfully parsed {total_rows_processed} entries into '{SQL_OUTPUT}'.")
    print(f"Human Report: '{DLQ_REPORT_CSV}' | Raw Replay Queue File: '{DLQ_REPLAY_CSV}'")
    return True
