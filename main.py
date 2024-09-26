import requests
import os
import random
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import json
import time
import csv
import pandas as pd

load_dotenv()

import requests
import os
from requests.exceptions import RequestException, Timeout, ProxyError
import time

def get_brokers_search_result(page_number, max_retries=3, retry_delay=5):
    url = "https://bizfilings.vermont.gov/online/DatabrokerInquire/BusinessSearchList"

    payload = f"undefined&sortby=BusinessID&stype=a&pidx={page_number}"
    headers = {
    '__requestverificationtoken': '_ONFqOiXKpA5Eyj_GVV_LvkqbZ4wjv2O2pnOE9vammKNzRFvEYhwfV7ZkPggfQkq7JKi6X5otPYD2BpEeI_b7fd5sa5GbAyXW3U3wlzzYTTG94olZXodwN_ht448sHAOWESqcCpkTK8oTcJF8fdCIshyQHjwib_9uoW0a85WHpo1',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,be;q=0.8,ar;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': 'ASP.NET_SessionId=fe2padw2pgk2s4eqlmgyngz1; __RequestVerificationToken=guH7fKHi-45NKBppGq1QygAveo4VSvkoUp6PCHCSlPI2egkpCeaOCJW1wMaVO9DI3jP_eYRZY__x-CozGirvh9UJua5i0RPDH2FPBWOQmls1HU2krAd_-gP4g9C5XcKqXKiX1GeU2U4Jk8XWUrSV2g2; ; incap_ses_1178_2276107=Z3r3UdmwW0ncLC1+5xhZENaM9WYAAAAAGpiNtuhuNT2JSv6dvSEe6w==; incap_ses_1460_2276107=hvNAHmQh9j1c8smybfZCFJyM9WYAAAAAwGhZn139jfwUX7KhGiaYug==; incap_ses_1599_2276107=5bRkElfHVSrX3wIhOMowFtGM9WYAAAAAP4FIHxpLZbb8RNgtSfT9hA==; incap_ses_1683_2276107=EwJAN3er0FAQBmo0uDdbF8SM9WYAAAAA8fJ9fzys5VG6JISZblggyQ==; incap_ses_1835_2276107=6PEifUA1oSvyD/cL6jp3GaaM9WYAAAAA/RxzkHoWYHA4EUFCBILUKg==; incap_ses_2100_2276107=1ShOOcPwMhpzYvD2BLMkHbyM9WYAAAAATMuckGaDsaIiiPRb8YhMDQ==; incap_ses_2105_2276107=fkAqFUiMBFKHN+DWgnY2HWOM9WYAAAAAtcuoRwUbFFTr/moXnWS72g==; incap_ses_225_2276107=/s5FKWQUUyIaRHXAeFwfA3iM9WYAAAAA/g6IINUmljsenEqZxpzbEQ==; incap_ses_326_2276107=DV/0IlAEQFaoml+6di+GBK6M9WYAAAAAyUi73AYZIhiD3axZxMcAxg==; incap_ses_480_2276107=YXDSFNTqN32DGWiHo02pBo6M9WYAAAAA8QtCHixvQkc7aDA6lIZC1w==; incap_ses_543_2276107=jZ3YZ36zzhJEksIaxx+JB4eM9WYAAAAAd+1HfXGRqv+kipJXxLERtw==; incap_ses_6526_2276107=aKsJc5qSkhgA2RzFkQKRWpKM9WYAAAAAfZNAUTYe7bsPv8CKQ+QGiQ==; incap_ses_84_2276107=RVHSbiFkDDfMAGqmvG0qAW6M9WYAAAAAUHx6KgbxHl+Iq8IK9dRvPg==; visid_incap_2276107=mJy3ij3fQ5KANzybmLJYD+b/82YAAAAAQkIPAAAAAACA2kq3AZOHTfHWowKqBecjg7Zdly1qDjbN',
    'origin': 'https://bizfilings.vermont.gov',
    'referer': 'https://bizfilings.vermont.gov/online/DatabrokerInquire/DataBrokerSearch',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
    }

    proxy_host = os.environ.get('PROXY_HOST', 'shared-datacenter.geonode.com')
    proxy_port = "9008"
    proxy_user = os.environ.get('PROXY_USER', 'geonode_9JCPZiW1CD')
    proxy_pass = os.environ.get('PROXY_PASS', 'e6c374e4-13ed-4f4a-9ed1-8f31e7920485')

    proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}"

    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, data=payload, proxies={'http': proxy_url, 'https': proxy_url}, timeout=30)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            return response.text
        except Timeout:
            print(f"Timeout error occurred (attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay} seconds...")
        except ProxyError:
            print(f"Proxy error occurred (attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay} seconds...")
        except RequestException as e:
            print(f"An error occurred during the request (attempt {attempt + 1}/{max_retries}): {str(e)}")
            print(f"Retrying in {retry_delay} seconds...")
        
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
    
    print("Max retries reached. Unable to fetch data.")
    return None

def save_as_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"JSON data saved to {filename}")

def save_as_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, encoding="utf-8", lineterminator="\n", quotechar='"', quoting=csv.QUOTE_ALL, index=False)
    print(f"CSV data saved to {filename}")


def parse_brokers_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', {'id': 'xhtml_grid_DBSearch'})
    if not table:
        return []  # Return empty list if table is not found
    rows = table.find_all('tr')[1:]  # Skip header row
    
    brokers_data = []
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 5:
            name = cells[0].text.strip()
            detail_link = cells[0].find('a')['href'] if cells[0].find('a') else ''
            registration_id = cells[1].text.strip()
            address = cells[2].text.strip()
            business_status = cells[3].text.strip()
            
            # Construct the full detail_link URL
            base_url = "https://bizfilings.vermont.gov"
            full_detail_link = base_url + detail_link if detail_link else ''
            
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
    results_folder = 'results'
    os.makedirs(results_folder, exist_ok=True)
    
    page_number = 0
    total_brokers = 0

    while True:
        print(f"Fetching page {page_number}...")
        html_content = get_brokers_search_result(page_number)
        
        if html_content is None:
            print(f"Failed to fetch page {page_number}. Stopping.")
            break
        
        brokers_data = parse_brokers_data(html_content)
        
        if not brokers_data:
            print(f"No more data found. Stopping at page {page_number - 1}")
            break
        
        json_filename = os.path.join(results_folder, f'data_brokers_page_{page_number}.json')
        csv_filename = os.path.join(results_folder, f'data_brokers_page_{page_number}.csv')
        
        save_as_json(brokers_data, json_filename)
        save_as_csv(brokers_data, csv_filename)
        
        total_brokers += len(brokers_data)
        print(f"Found {len(brokers_data)} brokers on page {page_number}. Total brokers so far: {total_brokers}")
        
        page_number += 1
        time.sleep(1)  # Add a small delay to avoid overwhelming the server

    return total_brokers

def save_as_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, encoding="utf-8", lineterminator="\n", quotechar='"', quoting=csv.QUOTE_ALL, index=False)
    print(f"CSV data saved to {filename}")

def consolidate_to_csv():
    results_folder = 'results'
    all_data = []
    
    # Get all CSV files in the results folder
    csv_files = [f for f in os.listdir(results_folder) if f.endswith('.csv')]
    
    for csv_file in csv_files:
        csv_path = os.path.join(results_folder, csv_file)
        df = pd.read_csv(csv_path)
        all_data.append(df)
    
    if all_data:
        consolidated_df = pd.concat(all_data, ignore_index=True)
        consolidated_filename = os.path.join(results_folder, 'all_data_brokers.csv')
        consolidated_df.to_csv(consolidated_filename, index=False)
        print(f"All data consolidated into {consolidated_filename}")
        print(f"Total records: {len(consolidated_df)}")
    else:
        print("No CSV files found to consolidate.")
def main():
    print("Starting to fetch all pages of broker data...")
    total_brokers = fetch_all_pages()
    print(f"Fetching complete. Total brokers found: {total_brokers}")
    consolidate_to_csv(total_brokers)
    print("Data consolidation complete.")

if __name__ == "__main__":
    main()