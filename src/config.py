import yaml
import json
import os
import sys
from logger import Logger

from argparse import ArgumentParser

class Config:

    @staticmethod
    def isNotebook() -> bool:
        try:
            shell = get_ipython().__class__.__name__
            if shell == 'ZMQInteractiveShell':
                return True   # Jupyter notebook or qtconsole
            elif shell == 'TerminalInteractiveShell':
                return False  # Terminal running IPython
            else:
                return False  # Other type (?)
        except NameError:
            return False      # Probably standard Python interpreter

    # Dynamic path
    main_file_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    current_working_dir = os.getcwd()

    config_dir = ""
    if not isNotebook():
        # Add CLI
        arguments = ArgumentParser()

        parser = ArgumentParser()
        parser.add_argument("--config", type=str, default="", help="config file overwrites commandline arguments. if not present, a new one will be created")
        
        try:
            args = parser.parse_args()
        except:
            parser.print_help()
            sys.exit(0)
        config_dir = args.config

    # Dynamic path
    file_path = f"{current_working_dir}/{config_dir}" if config_dir else f"{main_file_path}/config.yaml"
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
