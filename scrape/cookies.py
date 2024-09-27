import asyncio
import json
import os
import random
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Constants
URL = 'https://bizfilings.vermont.gov/online/DatabrokerInquire/'
COOKIE_FILE = 'cookies.json'

# Proxy settings
PROXY_HOST = os.getenv('PROXY_HOST')
PROXY_PORT = os.getenv('PROXY_PORT')
PROXY_USER = os.getenv('PROXY_USER')
PROXY_PASS = os.getenv('PROXY_PASS')

# User agents
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'

def format_cookies(cookies):
    # Create cookie string, ignoring 'incap_ses' cookies
    cookie_string = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in cookies if 'incap_ses' not in cookie['name']])
    return cookie_string

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
            user_agent=USER_AGENT,
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

        # Add request interception to block CSS, stylesheet, and media requests
        await context.route('**/*', lambda route: route.abort()
            if route.request.resource_type in ['stylesheet', 'image', 'media', 'font']
            else route.continue_()
        )

        try:
            # Open page
            page = await context.new_page()
            print('Navigating to the website...')
            await page.goto(URL, wait_until='networkidle', timeout=60000)
            print('Waiting for 2 seconds...')
            await asyncio.sleep(2)
            
            # Check if the page contains "Additional security check"
            if await page.get_by_text("Additional security check").count() > 0:
                print('Cloudflare detected. Waiting for 60 seconds before closing...')
                await asyncio.sleep(60)
            else:
                print('No Cloudflare detected. Continuing...')
                
                # Find and click the search button
                search_button = await page.query_selector('#btnSearch')
                if search_button:
                    await search_button.click()
                    print('Clicked on #btnSearch')
                    await asyncio.sleep(3)
                else:
                    print('Search button #btnSearch not found')
                

                # Retrieve cookies
                print('Retrieving cookies...')
                cookies = await context.cookies([page.url])

                print('Formatting cookies...')
                formatted_cookies = format_cookies(cookies)
                print('Formatted cookies:')
                print(json.dumps(formatted_cookies, indent=2))

                # Additional random wait before closing
                await asyncio.sleep(random.uniform(1, 3))

        except Exception as e:
            print(f'An error occurred: {e}')

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
