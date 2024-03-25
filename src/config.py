import yaml
import json
import os
from logger import Logger

class Config:
    
    main_file_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    file_path = f"{main_file_path}/config.yaml"
    example_file_path = f"{main_file_path}/example_config.yaml"

    config = None
    # Copy config file is no local one exists
    if os.path.isfile(file_path):
        Logger.debug("Config already exists, skips copying.")
        # TODO: Add auto updating

        with open(file_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        Logger.debug("Load local config successfully.")
        Logger.debug(f"Config: {json.dumps(config, indent=2)}")
    else:
        with open(example_file_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        print(json.dumps(config, indent=2))
        Logger.debug("Load example config successfully.")
        Logger.debug(f"Config: {json.dumps(config, indent=2)}")

        with open(file_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(config, f)

        Logger.info("Duplicate config successfully.")

    # Check config
    if not config:
        Logger.error("Config file empty!")
        raise Exception

    # Check if dev mode
    if config["developer mode"]:
        Logger.warning("Start in developer mode! Config file is override by example config file")
        with open(example_file_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        Logger.debug(f"Config: {json.dumps(config, indent=2)}")
    
    config["package root path"] = main_file_path
