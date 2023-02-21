import time
import requests
import json
import re
import config
from math import ceil
from tqdm import tqdm
from datetime import datetime

# todo: - Health check per get request.
#       - Grouped post requests.

def requestTickets(rowCount, startIndex):
    api_url = "https://helpme.brogangroup.com/api/v3/requests"
    input_data = "&input_data=%7B%22list_info%22%3A%7B%22row_count%22%3A+" + str(rowCount) + "%2C%22start_index%22%3A+" + str(startIndex) + "%2C%22get_total_count%22%3A+true%7D%7D"
    return (requests.get(api_url + config.servicedesk_key + "&format=json" + input_data)).json()

def postTicket(id, req, dep, time, tech, subj, desc):
    api_url = "https://api.powerbi.com/beta/e8f68013-0f30-4098-91cd-6aa91122a893/datasets/4b2d23ee-eadd-4660-992e-0e3152ccb2df/rows?cmpid=pbi-home-body-snn-signin"
    headers = {
        'Content-Type': 'application/json'
    }
    payload = [{
        "id": id,
        "requester": req,
        "department": dep,
        "timestamp": time,
        "technician": tech,
        "subject": subj,
        "description": desc
    }]
    response = requests.post(api_url + config.powerbi_key,headers=headers, data=json.dumps(payload))
    if response.text != "":
        print(response.text)
        return


def getTotalRows():
    return requestTickets(0, 0)["list_info"]["total_count"]

def getHealth():
    ticketInfo = requestTickets(0, 0)
    try:
        code = ticketInfo["response_status"][0]["status_code"]
        status = ticketInfo["response_status"][0]["status"]
        return str(code) + " (" + status + ") "
    except:
        return "Bad"
    
replacements = [
    ("&nbsp;", "\n"),
    (r"\n+", "\n"),
    ("&lt;", "<"),
    ("&gt;", ">"),
    ("&amp;", "&"),
    ("&quot;", "\""),
    ("&apos;", "\'"),
    ("&pound;", "£")
]
def deHTMLify(string):
    for pre, post in replacements:
        string = re.sub(pre, post, string)
    return string

def main():

    # Checks the health of the ServiceDesk API.
    print("API Health: " + getHealth())

    # Establishes how many rows of data ServiceDesk holds and how many API calls will be required.
    totalRows = getTotalRows()
    requiredLoops = ceil(totalRows/100)
    print(str(totalRows) + " total tickets detected. " + str(requiredLoops) + " API calls required.")

    # Performs all API calls and appends the data into a json object.
    for x in tqdm(range(requiredLoops)):
        if x == 0:
            jsonData = requestTickets(100, (x*100)+1)["requests"]
        else:
            jsonData.extend(requestTickets(100, (x*100)+1)["requests"])

    # Counts how many tickets have been fetched.
    ticketCount = len(jsonData)
    print(str(ticketCount) + " tickets retrieved.")

    # Cleans the fetched data by reaplacing HTML entities.
    print("Cleaning Data...")
    for y in tqdm(range(len(jsonData))):
            try: jsonData[y]["short_description"] = deHTMLify(jsonData[y]["short_description"])
            except: pass

    # Writes the json object holding the ticket data into a json file.
    writeFile = open('data.json', 'w')
    writeFile.truncate(0)
    writeFile.write(json.dumps(jsonData, indent=4))
    writeFile.close()

    # Iterates through all tickets and sends a post request to PowerBI holding the ticket data.
    print("Sending post requests to Power BI...")
    print("ETA: " + str(((0.5*ticketCount)/60)+1) + " minutes")
    for z in tqdm(range(len(jsonData))):
        start = time.time()

        d = jsonData[z]
        t = str(datetime.fromtimestamp(int(d["created_time"]["value"]) / 1e3))
        
        try: dep = d["requester"]["department"]["name"]
        except: dep = ""

        try: tech = d["technician"]["name"]
        except: tech = ""

        postTicket(d["id"], d["requester"]["name"], dep, t, tech, d["subject"], d["short_description"])

        # Due to API limitation, pacing per request is introduced @ 2 requests/sec minimum.
        stop = time.time()
        duration = float(stop-start)
        if duration < 0.5:
            time.sleep(0.5-duration)

if __name__ == "__main__":
    main()