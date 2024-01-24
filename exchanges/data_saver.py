import os
import json
from utils import handle_error


class DataSaver:
    @staticmethod
    def save_data(data, filename):
        if data is None or not isinstance(filename, str):
            return
        if not filename.endswith(".json"):
            filename += ".json"
        try:
            folder_name = "data_folder"
            if not os.path.exists(folder_name):
                os.makedirs(folder_name)

            file_path = os.path.join(folder_name, filename)
            if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
                separator = ",\n"
            else:
                separator = ""

            with open(file_path, "a") as file:
                file.write(separator + json.dumps(data, indent=2))
        except Exception as e:
            handle_error(f"Error saving data to {filename}", e)
