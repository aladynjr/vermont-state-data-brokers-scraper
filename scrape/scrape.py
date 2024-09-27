import asyncio
import aiohttp
from bs4 import BeautifulSoup
import os
import pandas as pd
import csv
import sys
import requests


# Constants
MAX_CONCURRENT_REQUESTS = 5
OUTPUT_DIRECTORY = '.'  # Current directory
DEFAULT_OUTPUT_FILENAME = 'vermont_data_brokers'
REQUEST_DELAY = 2  # Delay between requests in seconds

def initialize_session():
    print("Initiating session and retrieving initial data...")
    url = "https://bizfilings.vermont.gov/online/DatabrokerInquire/DataBrokerSearch"
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,be;q=0.8,ar;q=0.7',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded',
        'dnt': '1',
        'origin': 'https://bizfilings.vermont.gov',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'referer': 'https://bizfilings.vermont.gov/online/DatabrokerInquire/',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }

    payload = 'rbBusinessSearch=StartsWith&rbBasicSearch=BusinessName&txtDataBrokerName=&txtBusinessID=&txtFilingNumber=&ddlBusinessType=BusinessType&ddlBusinessStatus=&btnSearch=Search&hdnMessage='

    try:
        print("Sending initial request...")
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        print("Initial request successful.")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        token_element = soup.find('input', {'name': '__RequestVerificationToken', 'type': 'hidden'})
        token = token_element['value'] if token_element else None
        print(f"Token retrieved: {'Yes' if token else 'No'}")
        
        cookie = '; '.join([f"{cookie.name}={cookie.value}" for cookie in response.cookies if 'incap_ses' not in cookie.name])

        if not cookie:
            raise ValueError("No valid cookies found in the response")
        print("Cookies retrieved successfully.")

        pagination_info = soup.select_one('#pagination-digg > li.pageinfo')
        total_pages = 1
        if pagination_info:
            page_info = pagination_info.text.strip()
            total_pages = int(page_info.split('of')[1].split(',')[0].strip())

        print(f"Total pages found: {total_pages}")

        return cookie, token, total_pages

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"An error occurred during the initial request: {e}")

async def fetch_broker_page(session, url, cookie, token, page):
    payload = f"undefined&sortby=BusinessID&stype=a&pidx={page}"
    headers = {
        '__RequestVerificationToken': token,
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,be;q=0.8,ar;q=0.7',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'cookie': cookie,
        'dnt': '1',
        'origin': 'https://bizfilings.vermont.gov',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://bizfilings.vermont.gov/online/DatabrokerInquire/DataBrokerSearch',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    print(f"Fetching page {page}...")
    async with session.post(url, headers=headers, data=payload) as response:
        response.raise_for_status()
        html_content = await response.text()
    
    brokers_data = parse_broker_data(html_content)
    print(f"Processed page {page}")
    print(f"Parsed {len(brokers_data)} brokers from the current page.")
    return brokers_data

async def fetch_all_broker_pages(cookie, token, total_pages):
    print(f"Starting to fetch data from {total_pages} pages...")
    url = "https://bizfilings.vermont.gov/online/DatabrokerInquire/BusinessSearchList"
    all_brokers_data = []

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        async def fetch_with_rate_limit(page):
            async with semaphore:
                data = await fetch_broker_page(session, url, cookie, token, page)
                await asyncio.sleep(REQUEST_DELAY)
                return data

        tasks = [asyncio.create_task(fetch_with_rate_limit(page)) for page in range(1, total_pages + 1)]
        results = await asyncio.gather(*tasks)

        for page_data in results:
            all_brokers_data.extend(page_data)
            print(f"Total brokers found so far: {len(all_brokers_data)}")

    print(f"Progress: 100.00%")
    return all_brokers_data

def parse_broker_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', {'id': 'xhtml_grid_DBSearch'})
    if not table:
        print("Warning: No table found in the HTML content.")
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
            
            full_detail_link = f"https://bizfilings.vermont.gov{detail_link}" if detail_link else ''
            
            broker = {
                'name': name,
                'detail_link': full_detail_link,
                'registration_id': registration_id,
                'address': address,
                'business_status': business_status
            }
            brokers_data.append(broker)
    
    return brokers_data

def save_data_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, encoding="utf-8", sep=',', quotechar='"', quoting=csv.QUOTE_ALL, index=False)
    print(f"Data saved to {filename}")

def validate_broker_data(data):
    if not data:
        print("Error: No data to check.")
        return False

    required_columns = ['name', 'detail_link', 'registration_id', 'address', 'business_status']
    for column in required_columns:
        if column not in data[0]:
            print(f"Error: Missing required column '{column}'.")
            return False

    for row in data:
        if not row['name'] or not isinstance(row['name'], str):
            print(f"Error: Invalid 'name' value: {row['name']}")
            return False
        if not row['detail_link'].startswith('https://bizfilings.vermont.gov/'):
            print(f"Error: Invalid 'detail_link' value: {row['detail_link']}")
            return False
        if not row['registration_id'] or not isinstance(row['registration_id'], str):
            print(f"Error: Invalid 'registration_id' value: {row['registration_id']}")
            return False
        if not row['address'] or not isinstance(row['address'], str):
            print(f"Error: Invalid 'address' value: {row['address']}")
            return False
        if not row['business_status'] or not isinstance(row['business_status'], str):
            print(f"Error: Invalid 'business_status' value: {row['business_status']}")
            return False

    print("Data passed validation.")
    return True

async def scrape(output_filename):
    try:
        print("Starting the Vermont Data Broker Scraper...")
        cookie, token, total_pages = initialize_session()
        print(f"Session data retrieved successfully.")
        print(f"Cookie: {cookie[:50]}..." if len(cookie) > 50 else f"Cookie: {cookie}")
        print(f"Token: {token[:50]}..." if token and len(token) > 50 else f"Token: {token}")
        print(f"Total pages available: {total_pages}")
        
        all_brokers_data = await fetch_all_broker_pages(cookie, token, total_pages)
        print("\nData collection completed.")
        print(f"Total brokers found: {len(all_brokers_data)}")
        
        if validate_broker_data(all_brokers_data):
            output_path = os.path.join(OUTPUT_DIRECTORY, f"{output_filename}.csv")
            save_data_to_csv(all_brokers_data, output_path)
            
            print("\nSample of the first 5 brokers:")
            for i, broker in enumerate(all_brokers_data[:5], 1):
                print(f"\nBroker {i}:")
                for key, value in broker.items():
                    print(f"  {key}: {value}")
        else:
            print("Data failed validation. Please review the errors above.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    output_filename = DEFAULT_OUTPUT_FILENAME
    if len(sys.argv) > 1:
        output_filename = sys.argv[1]
    
    asyncio.run(scrape(output_filename))