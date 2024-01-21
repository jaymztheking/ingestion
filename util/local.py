import os
import logging
import csv

def create_local_csv(records: list, filepath: str, filename: str) -> str:
    if not os.path.isdir(filepath):
        os.makedirs(filepath)
    full_path = os.path.join(filepath, filename)

    with open(full_path, 'w+') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(records)
    return full_path

def removes_files_in_local_folder(folderpath: str) -> bool:
    if os.path.isdir(folderpath):
        for filename in os.listdir(folderpath):
            filepath = os.path.join(folderpath, filename)
            try:
                if os.path.isfile(filepath) or os.path.islink(filepath):
                    os.unlink(filepath)
            except Exception as e:
                logging.error(f'Failed to delete {filepath}: {e}')
                return False
    else:
        logging.warning(f'Folder {folderpath} not found, nothing deleted.')
        return False