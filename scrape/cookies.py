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
PROXY_HOST = 'shared-datacenter.geonode.com'
PROXY_PORT = 9008
PROXY_USER = 'geonode_9JCPZiW1CD'
PROXY_PASS = 'e6c374e4-13ed-4f4a-9ed1-8f31e7920485'

# User agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
]


def format_cookies_new(cookies, hidden_token):
    formatted = {
        "__requestverificationtoken": hidden_token,
        "cookie": ""
    }
    print(cookies)
    # Create cookie string including the __RequestVerificationToken from cookies, ignoring 'incap_ses' cookies
    cookie_string = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in cookies if 'incap_ses' not in cookie['name']])
    formatted['cookie'] = cookie_string

    return formatted

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

        # Add request interception to block CSS, stylesheet, and media requests
        await context.route('**/*', lambda route: route.abort()
            if route.request.resource_type in ['stylesheet', 'image', 'media', 'font']
            else route.continue_()
        )

        try:
            # Open first tab
            page1 = await context.new_page()
            print('Navigating to the website in first tab...')
            await page1.goto(URL, wait_until='networkidle', timeout=60000)
            print('Waiting for 2 seconds...')
            await asyncio.sleep(2)
            # Check if the page contains "Additional security check"
            if await page1.get_by_text("Additional security check").count() > 0:
                print('Captcha detected. Pressing Shift key...')
                await page1.keyboard.down('Shift')
                await asyncio.sleep(0.1)  # Short delay to simulate human-like behavior
                await page1.keyboard.up('Shift')
                print('Waiting for 3 seconds after pressing Shift...')
                await asyncio.sleep(3)
            else:
                print('No captcha detected. Skipping Shift key press.')
            print('Pressing Enter key...')
            await page1.keyboard.press('Enter')
            
            # Wait for a moment to allow any potential page changes
            await asyncio.sleep(1)
            print('Reloading the page...')
            await page1.reload(wait_until='domcontentloaded', timeout=60000)
            
            # Check if the page still contains "Additional security check" after reload
            if await page1.get_by_text("Additional security check").count() > 0:
                print('Captcha detected after reload. Opening second tab...')
                # Open second tab
                page2 = await context.new_page()
                print('Navigating to the website in second tab...')
                await page2.goto(URL, wait_until='domcontentloaded', timeout=60000)
                # Take a screenshot of the second page
                print('Taking a screenshot of the second page...')
                await page2.screenshot(path='page2_screenshot.png')
                print('Screenshot saved as page2_screenshot.png')
            else:
                print('No captcha detected after reload. Continuing with first tab...')
                # Take a screenshot of the first page
                print('Taking a screenshot of the first page...')
                await page1.screenshot(path='page1_screenshot.png')
                print('Screenshot saved as page1_screenshot.png')
                # Set page2 to page1 for consistency in the rest of the script
                page2 = page1
            
            # Find and click the search button
            search_button = await page2.query_selector('#btnSearch')
            if search_button:
                await search_button.click()
                print('Clicked on #btnSearch')
                await asyncio.sleep(3)
            else:
                print('Search button #btnSearch not found')
            # Extract the __RequestVerificationToken value from hidden input
            token_element = await page2.query_selector('input[name="__RequestVerificationToken"]')
            if token_element:
                hidden_token = await token_element.get_attribute('value')
                print(f'__RequestVerificationToken value from hidden input: {hidden_token}')
            else:
                print('__RequestVerificationToken element not found')
                hidden_token = ""

            # Retrieve cookies from the second tab
            print('Retrieving cookies from second tab...')
            cookies = await context.cookies([page2.url])

            print('Formatting cookies...')
            formatted_cookies = format_cookies_new(cookies, hidden_token)
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
