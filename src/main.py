import re
import os
import yaml
import json
import helper
from datetime import date
import time
import random
from typing import Literal

# Foreign dependency
import requests
import polars as pl

from loe_simp_app_fw.config import Config
from loe_simp_app_fw.logger import Logger

Config("config.yaml", example_config_path="config-example.yaml", project_root_path=os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
Logger("log", project_root_path=os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Helper
class FinishRetrieval(Exception):
    """Flag for retrieval ending

    Args:
        Exception: When the retrieval finishs, program should raise this flag.
    """
    pass

class BlockedHAHA(Exception):
    """Flag for remote blocking the retrieval, lasting 10 minutes.
    """
    pass

class UnknownError(Exception):
    pass

class UnknownRetrievalTarget(Exception):
    pass


class RetryCounter:

    threshold = 5
    _counter = 0

    @staticmethod
    def count():
        RetryCounter._counter += 1
        Logger.warning(f"Failed to retrieval for {RetryCounter._counter} time")

    @staticmethod
    def reset():
        RetryCounter._count = 0

    @staticmethod
    def shouldContinue():
        return _counter <= threshold


def name_cleaner(raw: str) -> str:
    """Return cleaned name for standards or plans

    Args:
        raw (str): raw name from the scraping

    Returns:
        str: cleaned name
    """
    result = helper.remove_duplicate_space(raw)

    result = result.replace(":", "：")
    return result

# Subprocess

def retrieve_response(parsed_search_words: str, page_number: int, standards_or_plans: Literal["standards", "plans"]) -> str:
    try:
        if standards_or_plans.lower() == "standards":
            response = requests.get(
                    f"https://std.samr.gov.cn/gb/search/gbQueryPage?searchText={parsed_search_words}&ics=&state=&ISSUE_DATE=&sortOrder=asc&pageSize=50&pageNumber={page_number}", 
                    verify=False
                    )
        elif standards_or_plans.lower() == "plans":
            response = requests.get(
                    f"https://std.samr.gov.cn/gb/search/gbProcessInfoPage?searchText={parsed_search_words}&ics=&sortOrder=asc&pageSize=50&pageNumber={page_number}", 
                    verify=False
                    )
        else:
            Logger.error(f"Unknown retrieval target, {standards_or_plans}")
            raise UnknownRetrievalTarget
    except ConnectionError:
        Logger.error("Failed to connect to host server.")
                
    response_parsed = json.loads(response.text)

    try:
        if len(response_parsed["rows"]) == 0: # 0 length return result means finish retrieval
            Logger.info("No more response, finish retrieval.")
            raise FinishRetrieval
    except KeyError:
        if response_parsed["code"]:
            Logger.warning(f"Remote block the access, saying '{response_parsed['code']}-{response_parsed['message']}'")
            raise BlockedHAHA
        else:
            Logger.warning("Unknown parsing error. Outputing raw result...")
            Logger.warning(f"{response.text}")
            raise UnknownError

    return response_parsed

def format_response_standards(response_parsed: dict) -> dict:
    response_formated = []
    for row in response_parsed["rows"]:
        try:
            row_formated = {
                "Website ID": row["id"],
                "Title": name_cleaner(row["C_C_NAME"].replace("<sacinfo>", "").replace("</sacinfo>", "").strip()),
                "ID": row["C_STD_CODE"],
                "Enforcement": row["STD_NATURE"],
                "Enforce Date": date.fromisoformat(row["ACT_DATE"]),
                "Status": row["STATE"],
                "Issue Date": date.fromisoformat(row["ISSUE_DATE"]),
                "Project ID": row["PROJECT_ID"],
                "URL": f"https://std.samr.gov.cn/gb/search/gbDetailed?id={row['id']}"
            }
        except KeyError:
            Logger.warning(f"Key missing for response, insert default values instead {json.dumps(response_parsed['rows'], indent=2)}")
            row_formated = {
                "Website ID": row["id"] if "id" in row else "",
                "Title": name_cleaner(row["C_C_NAME"].replace("<sacinfo>", "").replace("</sacinfo>", "").strip()) if "C_C_NAME" in row else "",
                "ID": row["C_STD_CODE"] if "C_STD_CODE" in row else "",
                "Enforcement": row["STD_NATURE"] if "STD_NATURE" in row else "",
                "Enforce Date": date.fromisoformat(row["ACT_DATE"]) if "ACT_DATE" in row else date.fromisoformat("1949-10-01"),
                "Status": row["STATE"] if "STATE" in row else "" ,
                "Issue Date": date.fromisoformat(row["ISSUE_DATE"]) if "ISSUE_DATE" in row else date.fromisoformat("1949-10-01"),
                "Project ID": row["PROJECT_ID"] if "PROJECT_ID" in row else "",
                "URL": f"https://std.samr.gov.cn/gb/search/gbDetailed?id={row['id']}" if "id" in row else ""
            }

        response_formated.append(row_formated)

    return response_formated

def format_response_plans(response_parsed: dict) -> dict:
    response_formated = []
    for row in response_parsed["rows"]:
        try:
            row_formated = {
                "Website ID": row["id"],
                "Title": name_cleaner(row["C_C_NAME"].replace("<sacinfo>", "").replace("</sacinfo>", "").strip()),
                "ID": row["C_PLAN_CODE"],
                "Status": row["CURRENT_LINK"],
                "Propose Date": date.fromisoformat(row["SEND_DATE"]),
                "New or Update": row["STD_FORM"], # 制修订
                "URL": f"https://std.samr.gov.cn/gb/search/gbDetailed?id={row['id']}"
            }
        except KeyError:
            Logger.warning(f"Key missing for response, insert default values instead {json.dumps(response_parsed['rows'], indent=2)}")
            row_formated = {
                "Website ID": row["id"] if "id" in row else "",
                "Title": name_cleaner(row["C_C_NAME"].replace("<sacinfo>", "").replace("</sacinfo>", "").strip()) if "C_C_NAME" in row else "",
                "ID": row["C_PLAN_CODE"] if "C_PLAN_CODE" in row else "",
                "Status": row["CURRENT_LINK"] if "CURRENT_LINK" in row else "" ,
                "Propose Date": date.fromisoformat(row["SEND_DATE"]) if "SEND_DATE" in row else date.fromisoformat("1949-10-01"),
                "New or Update": row["STD_FORM"] if "STD_FORM" in row else "",
                "URL": f"https://std.samr.gov.cn/gb/search/gbDetailed?id={row['id']}" if "id" in row else ""
            }

        response_formated.append(row_formated)

    return response_formated


def save_result(data: dict, search_keywords: str, standards_or_plans: Literal["standards", "plans"] ):
    # DATA_FRAMEEEEEEEEEEEEEEE
    df = pl.DataFrame(data)

    # Saving results
    SAVE_DIR = os.path.join(Config.config["project root path"], "result", f"{search_keywords.replace(' ', '_')}_{standards_or_plans}")
    Logger.debug(f"Save to {SAVE_DIR}.")

    helper.create_folder_if_not_exists(SAVE_DIR)
    SAVE_FILE_PATH = os.path.join(SAVE_DIR, f"{date.today()}.csv")
    Logger.debug(f"Save to {SAVE_FILE_PATH}.")
    df.write_csv(SAVE_FILE_PATH)
    return

def main():
    # Prelude
    RESULT_PATH = os.path.join(Config.config["project root path"], "result")
    helper.create_folder_if_not_exists(RESULT_PATH)

    # Load JSON
    Logger.info(f"Search for {Config.config['search keywords']}")
    data = []
    search_keywords_url_safe = requests.utils.quote(Config.config["search keywords"])
    standards_or_plans = Config.config["standards or plans"]

    for page_number in range(1, 1500): # 1500 is not a random number. There are in total 68429 stanards, the default page size is 50 per page.
        try:

            try:
                response_parsed = retrieve_response(search_keywords_url_safe, page_number, standards_or_plans)
                RetryCounter.reset()

            except FinishRetrieval:
                break

            except (ConnectionError, UnknownError):
                # Retry
                RetryCounter.count()
                if RetryCounter.shouldContinue():
                    Logger.debug("Fail to retrieve once again.")
                    page_number -= 1
                    continue
                else:
                    Logger.error(f"Failed to retrieval at page number {page_number}, threshold exceeded.")
                    RetryCounter.reset()
                    continue
            except BlockedHAHA:
                # Sleep
                typical_blocking_time_min = 10
                Logger.warning(f"Start sleeping to wait for block lift. (Typically lasting 10 minutes.)")
                for i in typical_blocking_time_min:
                    time.sleep(60.001)
                    Logger.info(f"Minute {i+1}.")

                    # Test if still blocked
                    try:
                        _trial = retrieve_response(search_keywords_url_safe, page_number, standards_or_plans)
                    except:
                        Logger.debug("Retrieval still being blocked.")
                        continue
                    
                    if _trial:
                        # Wake up for further retrieval
                        Logger.info("Retrieval no longer blocked.")
                        break
                
                # Retry
                RetryCounter.count()
                if RetryCounter.shouldContinue():
                    page_number -= 1
                    Logger.debug("Fail to retrieve once again.")
                    continue
                else:
                    Logger.error(f"Failed to retrieval at page number {page_number}, threshold exceeded.")
                    RetryCounter.reset()
                    continue
                    
            # Format response to make it look nice
            if standards_or_plans.lower() == "standards":
                response_formated = format_response_standards(response_parsed)
            elif standards_or_plans.lower() == "plans":
                response_formated = format_response_plans(response_parsed)
            else:
                Logger.error(f"Unknown retrieval target, {standards_or_plans}")
                raise UnknownRetrievalTarget

            # Compose JSON
            data.extend(response_formated)

            # Save file in the middle
            if page_number % 10 == 9:
                if Config.config["search keywords"]:
                    save_result(data, Config.config["search keywords"], standards_or_plans)
                else:
                    save_result(data, "all", standards_or_plans)
            
            # Time to sleep
            time_to_sleep = random.randrange(1024, 16384) / 2048
            time.sleep(time_to_sleep) # Radom sleep to avoid detection
            Logger.debug(f"Sleep for {time_to_sleep}")

            # Cosmatic logs
            if page_number == 20:
                Logger.warning("Large page number!")
            
            if page_number == 50:
                Logger.warning("Very large page number!")

        except KeyboardInterrupt:
            Logger.warning("Keyboard interrupt detected, quit and saving the file.")
            break

    # Save file
    if Config.config["search keywords"]:
        save_result(data, Config.config["search keywords"], standards_or_plans)
    else:
        save_result(data, "all", standards_or_plans)

if __name__ == "__main__":
    main()