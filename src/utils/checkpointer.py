import os
import csv
import time

STATE_FILE = "nem12_checkpoint.txt"

def get_last_byte_position():
    """Reads the last successfully processed byte position."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0
    return 0

def save_byte_position(position):
    """Atomically saves the progress to avoid state file corruption."""
    temp_state = f"{STATE_FILE}.tmp"
    with open(temp_state, "w") as f:
        f.write(str(position))
    os.replace(temp_state, STATE_FILE) # Atomic swap on OS level

def process_nem12_row(row):
    """
    Your core logic. NEM12 files use Record Indicators (100, 200, 300).
    Handle your database insertions or business math here.
    """
    if not row:
        return

    record_indicator = row[0]

    if record_indicator == "200":
        # This is an interval header row (contains NMI/Meter ID)
        print(f"Reading Meter: {row[1]}")
    elif record_indicator == "300":
        # This contains the actual energy usage intervals
        print(f"Processing intervals for date: {row[1]}")
        # IDEMPOTENCY TIP: Use a compound database key like (MeterID + Date)
        # to ensure re-running a line never creates duplicate data.

def stream_nem12_file(file_path):
    # 1. Get our last saved position
    last_position = get_last_byte_position()
    print(f"Starting pipeline. Resuming from byte: {last_position}")

    # 2. Open file in binary mode ('rb') so f.tell() returns exact byte offsets
    with open(file_path, "rb") as f:
        # Jump directly to where we crashed or left off last time
        f.seek(last_position)

        # Wrap the binary stream in a text decoder for the CSV reader
        # errors='ignore' ensures a single malformed character won't crash the whole terabyte stream
        text_stream = (line.decode('utf-8', errors='ignore') for line in f)
        csv_reader = csv.reader(text_stream)

        for row in csv_reader:
            process_nem12_row(row)

            # 3. Track progress and periodically save state
            # Saving every single row slows down performance. Saving every 1,000 rows is safer.
            current_position = f.tell()

            # For maximum safety, you can save every loop, or batch it
            save_byte_position(current_position)

# --- THE 1-MINUTE CRON / LOOP ---
# This loop simulates checking for new data or restarting after a crash
if __name__ == "__main__":
    TARGET_FILE = "massive_nem12_feed.csv"

    if not os.path.exists(TARGET_FILE):
        # Create empty file if testing
        open(TARGET_FILE, 'a').close()

    while True:
        print("Checking for new data or resuming stream...")
        try:
            stream_nem12_file(TARGET_FILE)
        except Exception as e:
            print(f"Network or System Error encountered: {e}. Retrying safely in 60 seconds...")

        # Wait 1 minute before checking for newly appended rows or retrying a failed stream
        time.sleep(60)
