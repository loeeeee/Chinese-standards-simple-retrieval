import re
import os
import yaml
import json
import helper
from logger import Logger
from config import Config
import requests
import polars as pl
from datetime import date
import time
import random

class FinishRetrieval(Exception):
    """Flag for retrieval ending

    Args:
        Exception: When the retrieval finishs, program should raise this flag.
    """
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


def retrieve_response(parsed_search_words: str, page_number: int) -> str:
    try:
        response = requests.get(
                f"https://std.samr.gov.cn/gb/search/gbQueryPage?searchText={parsed_search_words}&ics=&state=&ISSUE_DATE=&sortOrder=asc&pageSize=50&pageNumber={page_number}", 
                verify=False
                )
    except ConnectionError:
        Logger.error("Failed to connect to host server.")
                
    response_parsed = json.loads(response.text)

    if len(response_parsed["rows"]) == 0: # 0 length return result means finish retrieval
        Logger.info("No more response, finish retrieval.")
        raise FinishRetrieval

    return response_parsed

def format_response(response_parsed: dict) -> dict:
    response_formated = []
    for row in response_parsed["rows"]:
        row_formated = {
            "DB ID": row["id"],
            "Title": row["C_C_NAME"].replace("<sacinfo>", "").replace("</sacinfo>", "").strip(),
            "ID": row["C_STD_CODE"],
            "Enforcement": row["STD_NATURE"],
            "Enforce Date": date.fromisoformat(row["ACT_DATE"]),
            "Status": row["STATE"],
            "Issue Date": date.fromisoformat(row["ISSUE_DATE"]),
            "Project ID": row["PROJECT_ID"],
            "URL": f"https://std.samr.gov.cn/gb/search/gbDetailed?id={row['id']}"
        }
        response_formated.append(row_formated)

    return response_formated


def save_result(data: dict, search_keywords: str):
    # Formating results to Data Scientist's favourite
    schema = {
        "DB ID": str,
        "Title": str,
        "ID": str,
        "Enforcement": str,
        "Enforce Date": pl.Date,
        "Status": str,
        "Issue Date": pl.Date,
        "Project ID": pl.Int64,
        "URL": str,
    }
    df = pl.DataFrame(data, schema=schema)

    # Saving results
    SAVE_DIR = os.path.join(Config.config["package root path"], "result", search_keywords.replace(" ", "_"))
    Logger.debug(f"Save to {SAVE_DIR}.")

    helper.create_folder_if_not_exists(SAVE_DIR)
    SAVE_FILE_PATH = os.path.join(SAVE_DIR, f"{date.today()}.csv")
    Logger.debug(f"Save to {SAVE_FILE_PATH}.")
    df.write_csv(SAVE_FILE_PATH)
    return

def main():
    RESULT_PATH = os.path.join(Config.config["package root path"], "result")
    helper.create_folder_if_not_exists(RESULT_PATH)

    # Load JSON
    Logger.info(f"Search for {Config.config['search keywords']}")
    data = []
    search_keywords_url_safe = requests.utils.quote(Config.config["search keywords"])

    for page_number in range(1, 1500): # 1500 is not a random number. There are in total 68429 stanards, the default page size is 50 per page.
        try:

            try:
                response_parsed = retrieve_response(search_keywords_url_safe, page_number)
                RetryCounter.reset()

            except FinishRetrieval:
                break

            except ConnectionError:
                RetryCounter.count()
                if RetryCounter.shouldContinue():
                    page_number -= 1
                    continue
                else:
                    Logger.error(f"Failed to retrieval at page number {page_number}, threshold exceeded.")
                    RetryCounter.reset()
                    continue
                    
            # Format response to make it look nice
            response_formated = format_response(response_parsed)

            # Compose JSON
            data.extend(response_formated)

            # Save file in the middle
            if page_number % 10 == 9:
                if Config.config["search keywords"]:
                    save_result(data, Config.config["search keywords"])
                else:
                    save_result(data, "all")
            
            # Time to sleep
            time_to_sleep = 1 + random.randrange(-500, 500) / 1000
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
        save_result(Config.config["search keywords"])
    else:
        save_result("all")

if __name__ == "__main__":
    main()