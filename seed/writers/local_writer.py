import json
from pathlib import Path


class LocalWriter:

    def write(self, event):

        entity = event["entity"]
        Path("data").mkdir(exist_ok=True)
        file_path = f"data/{entity}.jsonl"

        with open(file_path, "a") as file:
            file.write(json.dumps(event))
            file.write("\n")