import os
import json
from utils import handle_error


class DataSaver:
    @staticmethod
    def save_data(data, filename, exchange_name=None):
        # Static method to save data to a file
        if data is None or not isinstance(filename, str):
            # Validate data and filename
            return
        if not filename.endswith(".json"):
            # Ensure the filename has a .json extension
            filename += ".json"
        try:
            folder_name = "data_folder"  # Folder name where data will be saved
            if not os.path.exists(folder_name):
                # Create the folder if it doesn't exist
                os.makedirs(folder_name)

            file_path = os.path.join(folder_name, filename)  # Full path to the file
            if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
                # Check if file exists and is not empty to append data correctly
                separator = ",\n"  # Separator for appending data
            else:
                separator = ""  # No separator needed for new or empty file

            with open(file_path, "a") as file:
                # Open file in append mode and write data
                file.write(
                    separator + json.dumps(data, indent=2)
                )  # Write data with JSON formatting
        except Exception as e:
            # Handle any exceptions during file operations
            handle_error(f"Error saving data to {filename}", e)


# Usage example
# data_saver = DataSaver()
# sample_data = {"key": "value"}
# data_saver.save_data(sample_data, "sample_data")
