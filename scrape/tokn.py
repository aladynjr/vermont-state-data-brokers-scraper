import requests
# Parse the response content
from bs4 import BeautifulSoup

url = "https://bizfilings.vermont.gov/online/DatabrokerInquire/DataBrokerSearch"
# Proxy settings
from dotenv import load_dotenv
import os

load_dotenv()

PROXY_HOST = os.getenv('PROXY_HOST')
PROXY_PORT = os.getenv('PROXY_PORT')
PROXY_USER = os.getenv('PROXY_USER')
PROXY_PASS = os.getenv('PROXY_PASS')

payload = 'rbBusinessSearch=StartsWith&rbBasicSearch=BusinessName&txtDataBrokerName=&txtBusinessID=&txtFilingNumber=&ddlBusinessType=BusinessType&ddlBusinessStatus=&btnSearch=Search&hdnMessage='
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

proxies = {
    'http': f'http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}',
    'https': f'http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}'
}

try:
    response = requests.request("POST", url, headers=headers, data='', proxies=proxies)
    response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the input element with the specified attributes
    token_element = soup.find('input', {'name': '__RequestVerificationToken', 'type': 'hidden'})
    
    if token_element:
        # Extract the value attribute
        token_value = token_element['value']
        print(f"Request Verification Token: {token_value}")
    else:
        print("Request Verification Token not found in the response")
    # Log cookies from the response, ignoring 'incap_ses' cookies
    print("Cookies:")
    cookie_string = '; '.join([f"{cookie.name}={cookie.value}" for cookie in response.cookies if 'incap_ses' not in cookie.name])
    print(cookie_string)

except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
