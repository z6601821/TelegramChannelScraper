# TelegramChannelScraper
This is a Python script that pulls all intact (non-deleted, non-hidden) messages for a target channel using Telethon to interact with the Telegram API.

## Requirements
- Python 3.11.X
- Git Bash
- Visual Studio Code (or similar text editor)
- Telegram API acount and credentials:
   - API ID
   - API hash
   - Phone number associated with the account

## Installation
[In Git Bash]

Clone with HTTPS:

```
$ git clone https://github.com/z6601821/TelegramChannelScraper.git
```

Change directory into the local repository:

```
$ cd TelegramChannelScraper/
```

Install dependencies:

```
$ pip install -r requirements.txt
```

## Usage
[In Visual Studio Code or text editor]

Create and run the scraper in a separate .py file:
```
# Imports
from TelegramChannelScraper import TelegramChannelScraper
import asyncio

# Create a scraper object and pass in API credentials
scraper = TelegramChannelScraper(
    credentials={
        "api_id": "<your API ID>", 
        "api_hash": "<your API hash>", 
        "phone": "<your account phone number>",
        }
)

# Option 1: provide a channel and pull all messages, OR
asyncio.run(
    scraper.get_messages(
        channel_name="<channel name>",
    )
)

# Option 2: provide a channel and pull from a specific message ID onwards
asyncio.run(
    scraper.get_messages(
        channel_name="<channel name>",
        start_message_id=<message ID>,
    )
)
```
