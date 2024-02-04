import os
import json
from utils import handle_error


class DataSaver:
    @staticmethod
    def save_data(data, filename, exchange_name=None):
        if not data or not isinstance(filename, str):
            return

        folder_name = "data_folder"
        filename = f"{filename}.json" if not filename.endswith(".json") else filename
        file_path = os.path.join(folder_name, filename)

        try:
            os.makedirs(folder_name, exist_ok=True)

            write_mode = (
                "a"
                if os.path.isfile(file_path) and os.path.getsize(file_path) > 0
                else "w"
            )
            with open(file_path, write_mode) as file:
                json.dump(data, file, indent=2)
                file.write("\n")
        except Exception as e:
            handle_error(f"Error saving data to {filename}", e)
