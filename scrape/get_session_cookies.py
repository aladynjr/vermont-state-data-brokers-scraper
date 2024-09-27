import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import time 
import json
import pandas as pd
import csv

load_dotenv()

PROXY_HOST = os.getenv('PROXY_HOST')
PROXY_PORT = os.getenv('PROXY_PORT')
PROXY_USER = os.getenv('PROXY_USER')
PROXY_PASS = os.getenv('PROXY_PASS')

def get_session_data():
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
        proxies = {
            'http': f'http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}',
            'https': f'http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}'
        }
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

def fetch_all_pages(cookie, token, total_pages, max_pages=None):
    pages_to_fetch = min(total_pages, max_pages) if max_pages is not None else total_pages
    print(f"Starting to fetch data from {pages_to_fetch} pages...")
    url = "https://bizfilings.vermont.gov/online/DatabrokerInquire/BusinessSearchList"
    all_brokers_data = []

    for page in range(1, pages_to_fetch + 1):
        payload = f"undefined&sortby=BusinessID&stype=a&pidx={page}"
        headers = {
            '__requestverificationtoken': token,
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

        print(f"Fetching page {page} of {pages_to_fetch}...")
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        html_content = response.text
        
        brokers_data = parse_brokers_data(html_content)
        all_brokers_data.extend(brokers_data)

        print(f"Processed page {page} of {pages_to_fetch}")
        print(f"Total brokers found so far: {len(all_brokers_data)}")
        print(f"Progress: {(page / pages_to_fetch) * 100:.2f}%")
        
        time.sleep(2)  # 2 second delay between requests

    return all_brokers_data

def parse_brokers_data(html_content):
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
    
    print(f"Parsed {len(brokers_data)} brokers from the current page.")
    return brokers_data

def save_to_json(data, filename='vermont_brokers_data.json'):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Data saved to {filename}")

def save_to_csv(data, filename='vermont_brokers_data.csv'):
    df = pd.DataFrame(data)
    df.to_csv(filename, encoding="utf-8", sep=',', quotechar='"', quoting=csv.QUOTE_ALL, index=False)
    print(f"Data saved to {filename}")

def main(max_pages=None):
    try:
        print("Starting the Vermont Business Data Scraper...")
        cookie, token, total_pages = get_session_data()
        print(f"Session data retrieved successfully.")
        print(f"Cookie: {cookie[:50]}..." if len(cookie) > 50 else f"Cookie: {cookie}")
        print(f"Token: {token[:50]}..." if token and len(token) > 50 else f"Token: {token}")
        print(f"Total pages available: {total_pages}")
        
        if max_pages:
            print(f"Max pages set to: {max_pages}")
        
        all_brokers_data = fetch_all_pages(cookie, token, total_pages, max_pages)
        print("\nData collection completed.")
        print(f"Total brokers found: {len(all_brokers_data)}")
        
        save_to_json(all_brokers_data)
        save_to_csv(all_brokers_data)
        
        print("\nSample of the first 5 brokers:")
        for i, broker in enumerate(all_brokers_data[:5], 1):
            print(f"\nBroker {i}:")
            for key, value in broker.items():
                print(f"  {key}: {value}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    max_pages = 2  # Set this to a number for testing, or None to fetch all pages
    main(max_pages)