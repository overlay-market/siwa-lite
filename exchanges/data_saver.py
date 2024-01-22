import os
import json
from utils import handle_error


class DataSaver:
    @staticmethod
    def save_data(data, filename):
        if data is None:
            return
        try:
            folder_name = "data_folder"
            if not os.path.exists(folder_name):
                os.makedirs(folder_name)

            file_path = os.path.join(folder_name, filename)
            with open(file_path, "a") as file:
                file.write(json.dumps(data, indent=2) + "\n")
        except Exception as e:
            handle_error(f"Error saving data to {filename}", e)
