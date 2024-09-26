import asyncio
import json
import os
import random
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Constants
URL = 'https://bizfilings.vermont.gov/online'
COOKIE_FILE = 'cookies.json'

# Proxy settings
PROXY_HOST = os.getenv('PROXY_HOST', 'shared-datacenter.geonode.com')
PROXY_PORT = str(random.randint(9000, 9010))
PROXY_USER = os.getenv('PROXY_USER', 'geonode_9JCPZiW1CD')
PROXY_PASS = os.getenv('PROXY_PASS', 'e6c374e4-13ed-4f4a-9ed1-8f31e7920485')

# User agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
]

async def simulate_human_behavior(page):
    await asyncio.sleep(random.uniform(2, 5))  # Random wait
    await page.mouse.move(random.randint(100, 500), random.randint(100, 500))

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            proxy={
                "server": f"http://{PROXY_HOST}:{PROXY_PORT}",
                "username": PROXY_USER,
                "password": PROXY_PASS
            }
        )
        
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1920, 'height': 1080},
            java_script_enabled=True,
            locale='en-US',
            timezone_id='America/New_York',
        )
        
        await context.set_extra_http_headers({
            'Accept-Language': 'en-US,en;q=0.9',
            'Sec-Ch-Ua': '"Chromium";v="91", " Not;A Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
        })

        try:
            # Open first tab
            page1 = await context.new_page()
            print('Navigating to the website in first tab...')
            await page1.goto(URL, wait_until='domcontentloaded', timeout=60000)
            # Keep the first tab open without further action

            # Open second tab
            page2 = await context.new_page()
            print('Navigating to the website in second tab...')
            await page2.goto(URL, wait_until='networkidle', timeout=60000)

            # Retrieve cookies from the second tab
            print('Retrieving cookies from second tab...')
            cookies = await context.cookies([page2.url])

            print('Saving cookies to file...')
            with open(COOKIE_FILE, 'w') as f:
                json.dump(cookies, f, indent=2)

            print(f'Cookies saved to {COOKIE_FILE}')

            # Additional random wait before closing
            await asyncio.sleep(random.uniform(1, 3))

        except Exception as e:
            print(f'An error occurred: {e}')

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())