import json
import os
from typing import List, Optional
from dataclasses import asdict, dataclass, field

@dataclass
class State:
    current_file: Optional[str] = None
    last_byte_position: int = 0
    completed_files: List[str] = field(default_factory=list)

    def reset_current_file(self):
        """Resets the cursor but keeps historical logs."""
        self.current_file = None
        self.last_byte_position = 0

    def to_json(self) -> str:
        """Converts the internal state directly to a JSON string."""
        return json.dumps(asdict(self), indent=4)

    @classmethod
    def load_from_file(cls, file_path: str) -> "State":
        """Factory method to load state from disk, or return a clean instance."""
        # Convert to an absolute path so threads never lose the folder context
        abs_file_path = os.path.abspath(file_path)

        if os.path.exists(abs_file_path):
            with open(abs_file_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    return cls(**data)
                except (json.JSONDecodeError, TypeError, Exception) as e:
                    print(f"[State Engine Alert] State file corrupted ({e}). Resetting memory configuration.")

        return cls()

    def save_to_file(self, file_path: str):
        """Atomically and thread-safely dumps the state data to a file."""
        # Convert to an absolute path so threads never lose the folder context
        abs_file_path = os.path.abspath(file_path)

        # Append the thread ID to the temp file name so concurrent writes
        # never overwrite or delete each other's temporary files!
        import threading
        temp_file = f"{abs_file_path}.{threading.get_ident()}.tmp"

        try:
            # Write the unique temp file
            with open(temp_file, "w", encoding="utf-8") as f:
                f.write(self.to_json())

            # Atomic swap into the main file destination
            os.replace(temp_file, abs_file_path)
        except Exception as e:
            # Clean up the specific temp file if anything fails mid-write
            if os.path.exists(temp_file):
                os.remove(temp_file)
            print(f"[State Engine Warning] Thread failed to write checkpoint safely: {e}")
