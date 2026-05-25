import os
import sys

class SQLWriter:
    """
    Handles compiling, securing, and writing multi-row
    PostgreSQL batch inserts into chunked .sql files.
    Includes a built-in multi-threaded consumer loop.
    """
    def __init__(self, cursor, output_dir="sql_chunks"):
        self.cursor = cursor
        self.output_dir = output_dir
        self.chunk_index = 1

        # Ensure the directory exists immediately upon initialization
        os.makedirs(self.output_dir, exist_ok=True)

    def write_batch_chunk(self, values_list):
        """Compiles a batch list and saves it as a brand-new self-contained file chunk."""
        if not values_list:
            return

        # 1. Format the target chunk file name: e.g., sql_chunks/chunk_00001.sql
        target_file_path = os.path.join(self.output_dir, f"chunk_{self.chunk_index:05d}.sql")

        # 2. Build the secure PostgreSQL string blocks
        base_query = 'INSERT INTO meter_readings ("nmi", "timestamp", "consumption") VALUES '
        placeholders = ",".join(["(%s, %s, %s)"] * len(values_list))
        flattened_params = [item for row in values_list for item in row]

        full_query = f'{base_query} {placeholders} ON CONFLICT ("nmi", "timestamp") DO UPDATE SET consumption = EXCLUDED.consumption;\n'

        # Secure client-side serialization via psycopg cursor engine
        safe_sql_bytes = self.cursor.mogrify(full_query, flattened_params)

        # 3. Write out the isolated file chunk context
        with open(target_file_path, "w", encoding="utf-8") as sql_file:
            sql_file.write(safe_sql_bytes.decode('utf-8'))

        # 4. Advance the index counter for the next incoming batch trigger
        self.chunk_index += 1

    def synchronize_chunk_index(self):
        """Scans the folder to resume numbering correctly if restarting after a crash."""
        if os.path.exists(self.output_dir):
            existing_chunks = len([f for f in os.listdir(self.output_dir) if f.endswith('.sql')])
            self.chunk_index = max(1, existing_chunks + 1)

    def start_worker_consumer(self, data_queue, save_checkpoint_func):
        """
        CONCURRENT CONSUMER LOOP: This runs inside your background thread.
        It listens to the memory queue and dumps chunks to disk automatically.
        """
        print("[SQLWriter Worker] Concurrent file-writer engine active.")
        while True:
            # Safely fetch the next data packet from the queue stack
            packet = data_queue.get()

            # Poison Pill Pattern: Shutdown command triggered at End-Of-File
            if packet is None:
                data_queue.task_done()
                break

            values_batch, byte_offset = packet
            try:
                # 1. Call its own internal file-writing method
                self.write_batch_chunk(values_batch)

                # 2. Fire the global system checkpointer save sequence
                save_checkpoint_func(byte_offset)
            except Exception as err:
                print(f"[SQLWriter ERROR] Thread failed dropping chunk block: {err}", file=sys.stderr)
            finally:
                # Notify the queue that the item was successfully processed
                data_queue.task_done()

        print("[SQLWriter Worker] Background loop terminated cleanly.")
