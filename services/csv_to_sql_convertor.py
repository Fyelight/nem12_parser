import csv
from datetime import timedelta
from adaptors.nem12_stream_adaptor import NEM12StreamAdapter
from utils.datetime_formatter import format_date
from utils.checkpointer import State
import config.settings as config

# Configuration
SQL_OUTPUT = config.settings.output_folder_path
DLQ_REPORT_CSV = config.settings.dlq_report_csv
DLQ_REPLAY_CSV = config.settings.dlq_replay_csv
SOURCE_TYPE = config.settings.source_type.lower()
BATCH_SIZE = config.settings.batch_size
STATE_FILE = config.settings.state_file_path

def generate_sql_file(source_target: str, batch_queue, source_type=SOURCE_TYPE, batch_values=None, offset_tracker=None, batch_size=BATCH_SIZE):
    if batch_values is None:
        batch_values = []

    # --- CRITICAL RECOVERY GUARD ---
    # Load the state object. Only resume from an offset if this specific file matches the crashed state reference.
    state = State.load_from_file(STATE_FILE)
    if state.current_file == source_target:
        last_position = state.last_byte_position
        print(f"[RECOVERY] Disrupted state match found. Seeking to byte position: {last_position}")
    else:
        last_position = 0

    if offset_tracker is None:
        offset_tracker = {"last_seen": last_position}
    else:
        offset_tracker["last_seen"] = last_position

    file_mode = "a" if last_position > 0 else "w"

    print(f"Initializing adapter pipeline for source [{source_type.upper()}]...")

    adapter = NEM12StreamAdapter(source_type, source_target, checkpoint=last_position)
    raw_stream = adapter.get_stream()

    print(f"Starting pipeline: '{source_target}' -> '{SQL_OUTPUT}' (Mode: {file_mode})")

    current_nmi = None
    interval_length = None
    total_data_processed = 0
    last_200_row = None

    with open(DLQ_REPORT_CSV, "a", encoding="utf-8", newline="") as report_file, \
         open(DLQ_REPLAY_CSV, "a", encoding="utf-8", newline="") as replay_file:

        report_writer = csv.writer(report_file)
        replay_writer = csv.writer(replay_file)

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

            if line_type in ("100", "900"):
                continue

            # --- 200 ROW: Capture Meter Metadata ---
            elif line_type == "200":
                current_nmi = row[1].strip()[:10]
                last_200_row = row  # Store this 200 row for replay context
                try:
                    # Extract interval length rules from column index 8
                    interval_length = int(row[8].strip())
                except (ValueError, IndexError):
                    print(f"ERROR: Corrupt structure in 200 row for meter {current_nmi}. Sent to DLQ.")
                    report_writer.writerow([csv_line_generator.offset, "200_CORRUPT_INTERVAL", row])
                    replay_writer.writerow(row)  # Saves pure raw line for easy re-running
                    last_200_row = None  # Clear context on corruption
                    interval_length = None
                    continue

                print(f"Reading Meter: {current_nmi}")
                continue

            # --- 300 ROW: Extract Consumption and Flatten Columns ---
            elif line_type == "300":
                if not current_nmi:
                    if last_200_row:
                        replay_writer.writerow(last_200_row)  # Include parent 200 row
                    report_writer.writerow([csv_line_generator.offset, "300_MISSING_200_HEADER", row])
                    replay_writer.writerow(row)
                    continue

                if interval_length is None:
                    if last_200_row:
                        replay_writer.writerow(last_200_row)
                    report_writer.writerow([csv_line_generator.offset, "300_MISSING_INTERVAL_RULE", row])
                    replay_writer.writerow(row)
                    continue

                # Determine data validation range limits
                intervals_per_day = 1440 // interval_length
                max_data_idx = 2 + intervals_per_day

                # Guard against truncated or cut lines
                if len(row) < max_data_idx:
                    if last_200_row:
                        replay_writer.writerow(last_200_row)
                    report_writer.writerow([csv_line_generator.offset, "300_INCOMPLETE_ROW_LENGTH", row])
                    replay_writer.writerow(row)
                    print(f"WARNING: Row too short for meter {current_nmi} date {row[1]}. Sent to DLQ.")
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

                    # Test if it's a valid number
                    try:
                        float(val_str)
                    except ValueError:
                        row_is_corrupt = True
                        error_reason = f"CORRUPT_VALUE_'{val_str}'_AT_COLUMN_{col_idx}"
                        break

                    col_idx += 1

                # IF CORRUPT: Drop the whole line, log to DLQ records, keep stream moving!
                if row_is_corrupt:
                    if last_200_row:
                        replay_writer.writerow(last_200_row)  # Include parent 200 row
                    report_writer.writerow([csv_line_generator.offset, error_reason, row])
                    replay_writer.writerow(row)  # Ready for re-run later
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
                    total_data_processed += 1
                    col_idx += 1

                # Trigger database multi-row batch inserts via queue pass off
                if len(batch_values) >= batch_size:
                    batch_queue.put((batch_values.copy(), csv_line_generator.offset))
                    batch_values.clear()

            elif line_type not in ("400", "500"):
                report_writer.writerow([csv_line_generator.offset, f"UNRECOGNIZED_INDICATOR_{line_type}", row])
                replay_writer.writerow(row)
                continue

    print(f"Pipeline Complete! Successfully parsed {total_data_processed} entries into '{SQL_OUTPUT}'.")
    print(f"Human Report: '{DLQ_REPORT_CSV}' | Raw Replay Queue File: '{DLQ_REPLAY_CSV}'")
    return True
