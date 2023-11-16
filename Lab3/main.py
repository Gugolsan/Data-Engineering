import os
import json
import glob
import shutil
import csv


def flatten_json(json_obj, prefix=''):
    """
        Recursively flattens a nested JSON object.

        Parameters:
        - json_obj (dict): The input JSON object.
        - prefix (str): The prefix to be added to the flattened keys.

        Returns:
        - dict: The flattened dictionary.
    """
    flat_dict = {}
    for key, value in json_obj.items():
        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, prefix + key + '_'))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, (dict, list)):
                    flat_dict.update(flatten_json(item, prefix + key + str(i) + '_'))
                else:
                    flat_dict[prefix + key + str(i)] = item
        else:
            flat_dict[prefix + key] = value
    return flat_dict


def move_and_flatten_json_files():
    # Get the current working directory
    current_dir = os.getcwd()

    # Define the relative path to the 'data' directory
    relative_data_dir = "data"

    # Construct the absolute path to the 'data' directory
    data_dir = os.path.join(current_dir, relative_data_dir)

    # Use glob to find all JSON files in 'data' and its subdirectories
    json_files = glob.glob(os.path.join(data_dir, '**/*.json'), recursive=True)

    # Move JSON files to the parent directory
    for json_file in json_files:
        file_name = os.path.basename(json_file)
        shutil.move(json_file, os.path.join(os.path.dirname(data_dir), file_name))

    # Delete the 'data' directory and its contents
    shutil.rmtree(data_dir)

    # Use glob again to find all moved JSON files
    moved_json_files = glob.glob(os.path.join(os.path.dirname(data_dir), '*.json'))

    # Iterate over the moved JSON files
    for moved_json_file in moved_json_files:
        # Load the JSON file
        with open(moved_json_file, 'r') as f:
            data = json.load(f)

        # Flatten the JSON data
        flattened_data = flatten_json(data)

        # Create a new JSON file with flattened data
        flattened_json_file = moved_json_file.replace('.json', '_flattened.json')
        with open(flattened_json_file, 'w') as f:
            json.dump(flattened_data, f, indent=2)

        # Remove the original moved JSON file
        os.remove(moved_json_file)

        # Create a new CSV file with flattened data
        csv_file = flattened_json_file.replace('_flattened.json', '_flattened.csv')
        with open(csv_file, 'w', newline='') as csvfile:
            fieldnames = flattened_data.keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header
            writer.writeheader()

            # Write data
            writer.writerow(flattened_data)


if __name__ == "__main__":
    move_and_flatten_json_files()
