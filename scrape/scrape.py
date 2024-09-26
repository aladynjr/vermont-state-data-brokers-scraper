import requests
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import time
import csv
import pandas as pd
import argparse
from requests.exceptions import RequestException, Timeout
from ratelimit import limits, sleep_and_retry

# Load environment variables from .env file in the same directory as the script
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Constants
BASE_URL = "https://bizfilings.vermont.gov"
SEARCH_URL = f"{BASE_URL}/online/DatabrokerInquire/BusinessSearchList"
DEFAULT_OUTPUT_FILE = "vermont_data_brokers.csv"

# Rate limiting: 5 calls per second
@sleep_and_retry
@limits(calls=5, period=1)
def rate_limited_request(url, headers, data):
    return requests.post(url, headers=headers, data=data, timeout=30)

def get_brokers_search_result(page_number, max_retries=3, retry_delay=1):
    payload = f"undefined&sortby=BusinessID&stype=a&pidx={page_number}"
    headers = {
        '__requestverificationtoken': '_ONFqOiXKpA5Eyj_GVV_LvkqbZ4wjv2O2pnOE9vammKNzRFvEYhwfV7ZkPggfQkq7JKi6X5otPYD2BpEeI_b7fd5sa5GbAyXW3U3wlzzYTTG94olZXodwN_ht448sHAOWESqcCpkTK8oTcJF8fdCIshyQHjwib_9uoW0a85WHpo1',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,be;q=0.8,ar;q=0.7',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'cookie': 'ASP.NET_SessionId=fe2padw2pgk2s4eqlmgyngz1; __RequestVerificationToken=guH7fKHi-45NKBppGq1QygAveo4VSvkoUp6PCHCSlPI2egkpCeaOCJW1wMaVO9DI3jP_eYRZY__x-CozGirvh9UJua5i0RPDH2FPBWOQmls1HU2krAd_-gP4g9C5XcKqXKiX1GeU2U4Jk8XWUrSV2g2; ; incap_ses_1178_2276107=Z3r3UdmwW0ncLC1+5xhZENaM9WYAAAAAGpiNtuhuNT2JSv6dvSEe6w==; incap_ses_1460_2276107=hvNAHmQh9j1c8smybfZCFJyM9WYAAAAAwGhZn139jfwUX7KhGiaYug==; incap_ses_1599_2276107=5bRkElfHVSrX3wIhOMowFtGM9WYAAAAAP4FIHxpLZbb8RNgtSfT9hA==; incap_ses_1683_2276107=EwJAN3er0FAQBmo0uDdbF8SM9WYAAAAA8fJ9fzys5VG6JISZblggyQ==; incap_ses_1835_2276107=6PEifUA1oSvyD/cL6jp3GaaM9WYAAAAA/RxzkHoWYHA4EUFCBILUKg==; incap_ses_2100_2276107=1ShOOcPwMhpzYvD2BLMkHbyM9WYAAAAATMuckGaDsaIiiPRb8YhMDQ==; incap_ses_2105_2276107=fkAqFUiMBFKHN+DWgnY2HWOM9WYAAAAAtcuoRwUbFFTr/moXnWS72g==; incap_ses_225_2276107=/s5FKWQUUyIaRHXAeFwfA3iM9WYAAAAA/g6IINUmljsenEqZxpzbEQ==; incap_ses_326_2276107=DV/0IlAEQFaoml+6di+GBK6M9WYAAAAAyUi73AYZIhiD3axZxMcAxg==; incap_ses_480_2276107=YXDSFNTqN32DGWiHo02pBo6M9WYAAAAA8QtCHixvQkc7aDA6lIZC1w==; incap_ses_543_2276107=jZ3YZ36zzhJEksIaxx+JB4eM9WYAAAAAd+1HfXGRqv+kipJXxLERtw==; incap_ses_6526_2276107=aKsJc5qSkhgA2RzFkQKRWpKM9WYAAAAAfZNAUTYe7bsPv8CKQ+QGiQ==; incap_ses_84_2276107=RVHSbiFkDDfMAGqmvG0qAW6M9WYAAAAAUHx6KgbxHl+Iq8IK9dRvPg==; visid_incap_2276107=mJy3ij3fQ5KANzybmLJYD+b/82YAAAAAQkIPAAAAAACA2kq3AZOHTfHWowKqBecjg7Zdly1qDjbN',
        'origin': BASE_URL,
        'referer': f"{BASE_URL}/online/DatabrokerInquire/DataBrokerSearch",
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    for attempt in range(max_retries):
        try:
            response = rate_limited_request(SEARCH_URL, headers, payload)
            response.raise_for_status()
            return response.text
        except (Timeout, RequestException) as e:
            print(f"Error occurred (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} second...")
                time.sleep(retry_delay)
    
    print("Max retries reached. Unable to fetch data.")
    return None

def parse_brokers_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', {'id': 'xhtml_grid_DBSearch'})
    if not table:
        return []
    
    rows = table.find_all('tr')[1:]
    brokers_data = []
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 5:
            name = cells[0].text.strip()
            detail_link = cells[0].find('a')['href'] if cells[0].find('a') else ''
            registration_id = cells[1].text.strip()
            address = cells[2].text.strip()
            business_status = cells[3].text.strip()
            
            full_detail_link = f"{BASE_URL}{detail_link}" if detail_link else ''
            
            broker = {
                'name': name,
                'detail_link': full_detail_link,
                'registration_id': registration_id,
                'address': address,
                'business_status': business_status
            }
            brokers_data.append(broker)
    
    return brokers_data

def fetch_all_pages():
    page_number = 0
    all_brokers_data = []

    while True:
        print(f"Fetching page {page_number}...")
        html_content = get_brokers_search_result(page_number)
        
        if html_content is None:
            print(f"Failed to fetch page {page_number}. Stopping.")
            break
        
        brokers_data = parse_brokers_data(html_content)
        
        if not brokers_data:
            print(f"No more data found. Stopping at page {page_number}")
            break
        
        all_brokers_data.extend(brokers_data)
        print(f"Found {len(brokers_data)} brokers on page {page_number}. Total brokers so far: {len(all_brokers_data)}")
        
        page_number += 1

    print(f"Stopped at page {page_number - 1}")
    return all_brokers_data

def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, 
              encoding="utf-8", 
              index=False, 
              quotechar='"', 
              quoting=csv.QUOTE_ALL,
              lineterminator="\n")  # Changed from line_terminator to lineterminator
    print(f"Data saved to {filename}")

def scrape(output_filename):
    print("Starting to fetch all pages of broker data...")
    all_brokers_data = fetch_all_pages()
    print(f"Fetching complete. Total brokers found: {len(all_brokers_data)}")
    save_to_csv(all_brokers_data, output_filename)
    print("Data saving complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Vermont data brokers and save to CSV.")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT_FILE, help=f"Name of the output CSV file (default: {DEFAULT_OUTPUT_FILE})")
    args = parser.parse_args()
    
    scrape(args.output)