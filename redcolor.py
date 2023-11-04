from datetime import datetime
import sqlite3
import requests
import time
import json
import gspread
from tqdm import tqdm
from oauth2client.service_account import ServiceAccountCredentials

URL = 'https://www.oref.org.il/WarningMessages/History/AlertsHistory.json'
URL2 = 'https://www.oref.org.il//Shared/Ajax/GetAlarmsHistory.aspx?lang=he&mode=3' # Taken from here: https://www.oref.org.il/12481-en/Pakar.aspx
#URL2 = 'https://www.oref.org.il//Shared/Ajax/GetAlarmsHistory.aspx?lang=he&fromDate=01.04.2023&toDate=06.10.2023&mode=0'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def create_db():

    conn = sqlite3.connect('alerts.db')
    cursor = conn.cursor()

    # Create a new table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY,
        alertDate TEXT,
        title TEXT,
        data TEXT,
        category INTEGER
    )
    ''')

    # Commit the changes
    conn.commit()

    # Close the connection
    conn.close()

def get_gspread_client(json_keyfile):
    scope = [
        "https://spreadsheets.google.com/feeds", 
        "https://www.googleapis.com/auth/spreadsheets", 
        "https://www.googleapis.com/auth/drive.file", 
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
    return gspread.authorize(creds)

def format_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").strftime('%m/%d/%Y %H:%M:%S')

def format_date_consistently(date_str: str) -> str:
    try:
        return datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S").strftime('%m/%d/%Y %H:%M:%S')
    except ValueError:
        # Handles the case where the hour is a single digit
        return datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S").strftime('%m/%d/%Y %H:%M:%S')

def insert_data_if_not_present(json_data: list, sheet_name: str = 'alerts'):

    client = get_gspread_client("redalerts-402016-d531c404225e.json")

    while True:
        try:
            sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1f7weaMbK1aPfoXh1HAqVjfiUPR-R3p8ZkQVKS5r_8vo/edit").get_worksheet_by_id("271910932")
            break
        except gspread.exceptions.APIError:
            time.sleep(10)
            continue
    
    # Attempt to get all records
    for i in range(4):
        try:
            records = sheet.get_all_records()
            break
        except gspread.exceptions.APIError:
            time.sleep(20)
            if i == 3:
                raise

    new_entries = []
    for entry in json_data:
        #entry["alertDate"] = format_date(entry["alertDate"])
        matching_records = [record for record in records if record['rid'] == entry['rid']]

        if not matching_records:
            #new_entries.append([entry["alertDate"], entry["category_desc"], entry["data"], entry["category"], entry["matrix_id"], entry["rid"]])
            new_entries.append(entry.values())

    # Insert new entries in bulk
    for new_entry in new_entries:

        for i in range(4):
            try:
                sheet.append_row([x for x in new_entry])
                break
            except gspread.exceptions.APIError:
                time.sleep(20)
                if i == 3:
                    raise

        print("Inserting data")

def process_raw_data(raw_data: str):
    """Converts raw data string into a list of dictionaries."""
    entries = raw_data.strip().split("\n")
    structured_data = []
    
    for entry in entries:
        split_entry = entry.split("\t")
        structured_data.append({
            "alertDate": split_entry[0].strip(),
            "title": split_entry[1].strip(),
            "data": split_entry[2].strip(),
            "category": int(split_entry[3].strip())
        })
    
    return structured_data

def collect_old_data():
    while True:

        # Attempt to get the JSON
        while True:
            try:
                response = requests.get(URL2, headers=HEADERS)
                break
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
                print(f"Error: {e}")
                time.sleep(60)
        
        # If the request was successful
        if response.status_code == 200:
            try:
                json_data = response.json()  # assuming the website returns JSON data
            except requests.exceptions.JSONDecodeError:
                print(f"Error: {e}")
                time.sleep(60)
                continue
        else:
            print(f"Failed to fetch data at {time.ctime()}, status code: {response.status_code}")

        insert_data_if_not_present(json_data)

        print(f"Data fetched at {time.ctime()}")
        
        time.sleep(30)

def main():

    while True:

        # Attempt to get the JSON
        while True:
            try:
                response = requests.get(URL, headers=HEADERS)
                break
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
                print(f"Error: {e}")
                time.sleep(5)
        
        # If the request was successful
        if response.status_code == 200:
            json_data = response.json()  # assuming the website returns JSON data
        else:
            print(f"Failed to fetch data at {time.ctime()}, status code: {response.status_code}")

        insert_data_if_not_present(json_data)

        print(f"Data fetched at {time.ctime()}")
        
        time.sleep(10)

if __name__ == "__main__":
    collect_old_data()
    #main()


#raw_data = """
#11-10-23 19:50	ירי רקטות וטילים	גיבתון 	1
#11-10-23 19:50	ירי רקטות וטילים	גן שלמה	1
#11-10-23 19:50	ירי רקטות וטילים	גאליה	1
#11-10-23 19:50	ירי רקטות וטילים	גבעת ברנר	1
#11-10-23 19:50	ירי רקטות וטילים	כפר הנגיד	1
#    """
    #json_data = process_raw_data(raw_data)
    #insert_data_if_not_present(json_data)
