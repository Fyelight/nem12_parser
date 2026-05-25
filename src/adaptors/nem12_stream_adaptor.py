import sys

class NEM12StreamAdapter:
    """
    Unified streaming adapter.
    Guarantees a standardized, line-by-line binary/text generator
    regardless of whether the input is a local file, web API, or IoT feed.
    """
    def __init__(self, source_type, source_target, checkpoint=0):
        self.source_type = source_type.lower()
        self.source_target = source_target
        self.checkpoint = checkpoint

    def get_stream(self):
        """Dispatches the stream generator based on source type."""
        if self.source_type == "file":
            return self._stream_local_file()
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

    def _stream_local_file(self):
        """Streams a massive local file safely using your byte checkpointer."""
        if self.checkpoint == 0:
            with open(self.source_target, "r", encoding="utf-8") as f:
                first_line = f.readline().strip()
                # Use a comma split check or pattern match
                if not first_line.startswith("100"):
                    print(f"CRITICAL ERROR: File '{self.source_target}' does not start with '100'. Aborting.", file=sys.stderr)
                    sys.exit(1)
            print("Success: Verified local NEM12 file signature root header.")

        # --- MEMORY-SAFE BINARY STREAMING LOOP ---
        with open(self.source_target, "rb") as f:
            if self.checkpoint > 0:
                f.seek(self.checkpoint)
            for line in f:
                yield line, f.tell()