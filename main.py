from TelegramChannelScraper import *
# Included with Python
import asyncio
import sys

async def main():
    scraper = TelegramChannelScraper()
    
    try:
        await scraper.start_client()
    except ValueError:
        print("\nERROR: The script encountered a problem using the credentials you provided credentials; make sure the credentials you provided are correct!\n")
        sys.exit(1)
    
    try:
        await scraper.get_messages()
    except:
        print("\nERROR: The script encountered a problem while tring to scrape messages from the channel you provided; make sure the channel exists!\n")
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())