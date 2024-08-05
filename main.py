import json
import logging
from datetime import datetime
from time import time, sleep

import requests
import urllib3

from utils import connect_preprod

pipeline_time_start = time()
urllib3.disable_warnings()
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter1 = logging.Formatter('{asctime}: {message} | {lineno}', style='{')

# connect to the database, either preprod or localhost
cursor, db = connect_preprod()

# find initial page
t0 = time()
yesterday = datetime.now()
yesterday = datetime.strftime(yesterday, '%Y-%m-%d')
cursor.execute("""select latest_release from contracts_finder_new order by latest_release DESC limit 1""")
result = cursor.fetchone()
latest_date = result[0].date()
initial_url = f'https://www.contractsfinder.service.gov.uk/Published/Notices/OCDS/Search?publishedTo={yesterday}&publishedFrom={latest_date}'
page_count = 1
record_count = 0


def contractsfinder_request(url) -> None:
    """
    send initial request to public contracts api
    """
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56'}
    request = requests.get(url, headers=headers, verify=False)
    contractsfinder_data = request.json()
    t1 = time()
    request_time = t1 - t0
    logger.info(f'TIME TAKEN FOR REQUEST: {request_time}')
    return contractsfinder_data


def parse_contract(release):
    """
    taking the json data for a single contract release from contractfinder api
    and putting the json into front-end tables
    """
    reqstring = f'https://www.contractsfinder.service.gov.uk/Published/Notice/releases/{release["id"]}.json'
    logger.info(release['id'])
    jresponse = release
    logger.info(jresponse['date'], '<-------------- DATE')
    published_date_str = jresponse['date'].replace('+01:00', 'Z')
    latestrelease = datetime.strptime(published_date_str, '%Y-%m-%dT%H:%M:%SZ')
    ocid = jresponse['ocid']
    release_id = jresponse['id']
    dt_now = datetime.now()
    dt_strf = dt_now.strftime("%Y-%m-%d %H:%M:%S")
    data_dump = json.dumps(jresponse).replace('\n', '').replace('\r', '')
    update_query_string = f"""INSERT INTO contracts_finder_new (uri, ocid, release_id, content, latest_release, date_collected) VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE content=%s, latest_release=%s,  date_collected=%s"""
    # update_query_string = f"""INSERT IGNORE INTO contracts_finder_rework (uri, ocid, release_id, content, latest_release, date_collected) VALUES (%s, %s, %s, %s, %s, %s)"""
    updateq_tuple = (
        reqstring, ocid, release_id, data_dump, latestrelease, dt_strf, data_dump, latestrelease, dt_strf)
    # updateq_tuple = (reqstring, ocid, release_id, data_dump, latestrelease, dt_strf)
    cursor.execute(update_query_string, updateq_tuple)
    db.commit()


def parse_contracts_page(url):
    """
    send request for entire page and return page of contracts. The pack is a json string that lists individual contracts
    has next page functionality
    """
    global page_count
    global record_count

    # send initial request, and collect all of the id's from the results on the first page
    requestj = contractsfinder_request(url)
    releaselist = []
    [releaselist.append(data) for data in requestj['releases']]
    record_count += len(releaselist)
    logger.info(releaselist)

    # parse individual requests in the requestlist
    [parse_contract(release) for release in releaselist]

    # check for next pages
    nextpage = 0
    while nextpage == 0:
        try:
            if requestj['links']['next']:
                logger.info(f"NEXTPAGE FOUND: {requestj['links']['next']}")
                page_count += 1
                logger.info(f'{page_count} <----- current page')
                logger.info(f'{record_count} <------ current records')
                sleep(2)
                requestj = contractsfinder_request(requestj['links']['next'])
                releaselist = []
                [releaselist.append(data) for data in requestj['releases']]
                record_count += len(releaselist)
                logger.info(len(releaselist))
                logger.info(releaselist)
                # parse individual requests in the requestlist
                [parse_contract(release) for release in releaselist]
        except KeyError:
            nextpage = 1
    logger.info('no more pages, closing script')
    return page_count, record_count


def send_message(title, text, hexcolour):
    url = "https://tdworldwide.webhook.office.com/webhookb2/d5d1f4d1-2858-48a6-8156-5abf78a31f9b@7fe14ab6-8f5d-4139-84bf-cd8aed0ee6b9/IncomingWebhook/76b5bd9cd81946338da47e0349ba909d/c5995f3f-7ce7-4f13-8dba-0b4a7fc2c546"
    payload = json.dumps({
        "@type": "MessageCard",
        "themeColor": hexcolour,
        "title": title,
        "text": text,
        "markdown": True
    })
    headers = {
        'Content-Type': 'application/json'
    }
    requests.request("POST", url, headers=headers, data=payload)


# when this is done, we need to check for the presence of a nextpage, if so, repeat process with new link
try:
    parse_contracts_page(url=initial_url)
    pipeline_time_end = time()
    pipeline_time_total = pipeline_time_end - pipeline_time_start
    pass_title = 'PublicContractsPipeline Passed'
    pass_text = f"""
    Total Records Collected: {record_count}
    Pages Parsed: {page_count}
    Time Taken for pipeline: {pipeline_time_total}
    Base URL {initial_url}
    """
    hexcolour_pass = '#00c400'
    send_message(title=pass_title, text=pass_text, hexcolour=hexcolour_pass)
except Exception as e:
    fail_title = 'PublicContractsPipeline Failed'
    fail_text = f'Check AzureDevops\n Error in Code: {e} \nCollected {record_count} records from {page_count} pages \nUsing url {initial_url}'
    hexcolour_fail = '#c40000'
    send_message(title=fail_title, text=fail_text, hexcolour=hexcolour_fail)
    logger.info(f'ERROR: {e}')