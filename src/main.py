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

def main():
    RESULT_PATH = os.path.join(Config.config["package root path"], "result")
    helper.create_folder_if_not_exists(RESULT_PATH)

    # Load JSON
    Logger.info(f"Search for {Config.config['search keywords']}")
    data = []
    search_keywords_url_safe = requests.utils.quote(Config.config["search keywords"])
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

    for page_number in range(1, 1500): # 1500 is not a random number. There are in total 68429 stanards, the default page size is 50 per page.
        response = requests.get(f"https://std.samr.gov.cn/gb/search/gbQueryPage?searchText={search_keywords_url_safe}&ics=&state=&ISSUE_DATE=&sortOrder=asc&pageSize=50&pageNumber={page_number}", verify=False)
        response_parsed = json.loads(response.text)
        if len(response_parsed["rows"]) == 0: # 0 length return result means finish retrieval
            break
        
        if page_number == 20:
            Logger.warning("Large page number!")
        
        if page_number == 50:
            Logger.warning("Very large page number!")
        
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

        # Parsing JSON
        data.extend(response_formated)

    df = pl.DataFrame(data, schema=schema)

    SAVE_DIR = os.path.join(Config.config["package root path"], "result", Config.config["search keywords"].replace(" ", "_"))
    helper.create_folder_if_not_exists(SAVE_DIR)
    SAVE_FILE_PATH = os.path.join(SAVE_DIR, f"{date.today()}.csv")
    df.write_csv(SAVE_FILE_PATH)

if __name__ == "__main__":
    main()