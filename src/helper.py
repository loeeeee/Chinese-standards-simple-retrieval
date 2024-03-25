import os
from logger import Logger

def create_folder_if_not_exists(folder_path: str) -> bool:
    # Return if the folder exists
    # It only create one folder at a time

    if not os.path.isfile(folder_path) and not os.path.isdir(folder_path):
        os.mkdir(folder_path)
        Logger.info("Folder does not exists. Creating a new one.")
    else:
        Logger.debug("Folder exists. No further action.")