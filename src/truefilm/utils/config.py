import os
from typing import List, Dict
import logging
from typing import Optional

DEFAULT_CONFIG_FOLDERS_SEARCH_FINAL = ['../../resources/final_ok', '../resources/final_ok', 'resources/final_ok']
DEFAULT_CONFIG_FOLDERS_SEARCH_RAW = ['../../resources/raw_ok', '../resources/raw_ok', 'resources/raw_ok']
logger = logging.getLogger(__name__)

class Config:
    def __init__(self,
                 folders_final=DEFAULT_CONFIG_FOLDERS_SEARCH_FINAL,
                 folders_raw=DEFAULT_CONFIG_FOLDERS_SEARCH_RAW) -> None:
        self.config_folders_final = folders_final
        self.config_folders_raw = folders_raw

    def final_resources_check(self, file_names: List[str]) -> Dict[str, Optional[str]]:
        """
        Checks the presence of the specified final_ok files and returns the path for each given file name
        i.e.
        input: "movies_metadata"
        return:
        {
            "movies_metadata": "../../final_ok/resources/movies_metadata"
        }
        """
        path_dict = {}

        for file_name in file_names:
            path_dict[file_name] = None

        for folder in self.config_folders_final:
            for file_name in file_names:
                if os.path.exists(os.path.join(folder, file_name+'/_SUCCESS')):
                    logger.info("Detected %s in folder %s",file_name, folder)
                    path_dict[file_name] = os.path.join(folder, file_name)

        return path_dict

    def raw_resources_check(self, file_names: List[str]) -> Dict[str, Optional[str]]:
        """
        Checks the presence of the specified raw_ok files and returns the path for each given file name
        i.e.
        input: "movies_metadata"
        return:
        {
            "movies_metadata.csv": "../../raw_ok/resources/movies_metadata.csv"
        }
        """
        path_dict = {}

        for file_name in file_names:
            path_dict[file_name] = None

        for folder in self.config_folders_raw:
            for file_name in file_names:
                if os.path.exists(os.path.join(folder, file_name)):
                    print("Using {} in folder {}".format(file_name, folder))
                    path_dict[file_name] = os.path.join(folder, file_name)

        for key, value in path_dict.items():
            if not value:
                logger.info("Detected %s in folder %s",file_name, folder)

        return path_dict
