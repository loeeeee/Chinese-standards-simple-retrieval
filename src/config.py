import yaml
import json
import os
import sys
from logger import Logger

from argparse import ArgumentParser

class Config:

    # Dynamic path
    main_file_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    current_working_dir = os.getcwd()

    # Add CLI
    arguments = ArgumentParser()

    parser = ArgumentParser()
    parser.add_argument("--config", type=str, default="", help="config file overwrites commandline arguments. if not present, a new one will be created")
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit(0)

    # Dynamic path
    file_path = f"{current_working_dir}/{args.config}" if args.config else f"{main_file_path}/config.yaml"
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

        Logger.info("First time starting script, please modify config.yaml to the requirements.")
        sys.exit() # Exit script

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
