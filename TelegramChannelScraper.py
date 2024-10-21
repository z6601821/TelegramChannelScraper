# Need to pip / conda install
from telethon import TelegramClient # v1.25.2
import pandas as pd
# Included with Python
from datetime import datetime as dt
from time import sleep
import os

class TelegramChannelScraper():
    def __init__(self):
        self.credentials = {}
        credentials_provided = False
        
        while not credentials_provided:
            self.credentials["api_id"] = input(
                "\nProvide your Telegram api_id:\n(Shift + insert to paste)\n").strip()
            self.credentials["api_hash"] = input(
                "\nProvide your Telegram api_hash:\n(Shift + insert to paste)\n").strip()
            self.credentials["phone"] = input(
                "\nProvide your Telegram account phone number:\n(Shift + insert to paste)\n").strip()
            print(f"\nThese are the credentials you provided:\n{self.credentials}\n")
            user_accepts = input(
                "\nDo you wish to proceed? Enter 'y' to continue or 'n' to start over:\n")
            
            if user_accepts.strip().lower() == 'y':
                credentials_provided = True
        
    async def start_client(self):
        self.client = TelegramClient(
            "", 
            self.credentials["api_id"], 
            self.credentials["api_hash"],
        )
        print("\n------------------------------\n")
        print("\nClient initialized\n")
        await self.authenticate()

    async def authenticate(self):        
        await self.client.connect()
        print("Client connected\n")

        is_auth = await self.client.is_user_authorized()
        if not is_auth:
            print("Not authenticated; signing in\n")
            await self.client.send_code_request(self.credentials["phone"])
            await self.client.sign_in(
                self.credentials["phone"], 
                input("Enter code:\n")
            )
            print("\nClient authenticated\n\n")
        
    async def get_messages(self):
        self.today_date = dt.today().strftime("%Y-%m-%d")
        self.target_channel = None
        
        channel_provided = False
        while not channel_provided:
            print("\n------------------------------\n")
            entered_channel = input("Enter a Telegram channel to scrape:\n")
            if entered_channel.strip() == "":
                print("You must enter a valid channel name!\n")
                continue

            print(f"\nThis is the channel you provided:\n{entered_channel}\n")
            user_accepts = input(
                "Do you wish to proceed? Enter 'y' to continue or 'n' to start over:\n")
            
            if user_accepts.strip().lower() == 'y':
                self.target_channel = entered_channel
                channel_provided = True
                
        self.folder_name = f"Output_{self.target_channel}_{self.today_date}"
        
        try:
            os.mkdir(self.folder_name)
        except FileExistsError:
            pass
        print(f"\nCreated folder '{self.folder_name}' to store output\n")

        pulled_messages = await self.pull_messages_in_batches()
        cleaned_messages = self.parse_raw_messages(pulled_messages)
        self.create_messages_table(cleaned_messages)
        print(f"[Finished]")

    async def pull_messages_in_batches(self):
        # Find highest message ID; this is most recent message
        all_messages = await self.client.get_messages(self.target_channel)
        highest_message_id = all_messages[0].id
        print(f"Most recent message in channel '{self.target_channel}' has ID {highest_message_id}\n")
    
        # Divide highest ID by 1000 and floor to get number of batches
        batches_needed = int(highest_message_id / 1000)
        print(f"Preparing to pull messages in {batches_needed} batches of 1000 each, oldest to newest\n")

        # Loop through batches pulling 1000 msgs at a time; final batch must encompass highest message ID
        pulled_messages = []
        for i in range(1, batches_needed + 2): # add 2 to be high-inclusive for highest ID
            batch_high = i * 1000
            batch_low = batch_high - 999
            print(f"Working on batch {i} (message ID: {batch_low} - message ID: {batch_high})")

            async for message in self.client.iter_messages(
                self.target_channel, 
                limit=1000, 
                min_id=batch_low,
                max_id=batch_high,
                reverse=True): # oldest to newest

                pulled_messages.append(message)
                
            sleep(30.0)
            print(f"Finished batch {i}\n")

        print(f"Pulled {len(pulled_messages)} total messages for channel '{self.target_channel}'\n")
        return pulled_messages

    def parse_raw_messages(self, pulled_messages):
        print("Extracting fields and cleaning messages\n")
        cleaned_messages = []
        for message in pulled_messages:
            cleaned_messages.append(self.extract_message_fields(message))
        print(f"Cleaned {len(cleaned_messages)} messaged\n")
        return cleaned_messages
    
    def extract_message_fields(self, message):
        # Messaage info: https://docs.telethon.dev/en/stable/quick-references/objects-reference.html
        cleaned = {}

        try:
            cleaned['message_id'] = message.id
        except AttributeError:
            cleaned['message_id'] = None

        try:
            cleaned['channel_id'] = message.peer_id.channel_id
        except AttributeError:
            cleaned['channel_id'] = None

        try:
            cleaned['message_date'] = str(message.date)
        except AttributeError:
            cleaned['message_date'] = None

        try:
            cleaned['post_author'] = message.post_author
        except AttributeError:
            cleaned['post_author'] = None

        try:
            cleaned['is_posted_by_bot'] = message.via_bot_id
        except AttributeError:
            cleaned['is_posted_by_bot'] = None

        try:
            cleaned['total_replies'] = message.replies.replies
        except AttributeError:
            cleaned['total_replies'] = None

        try:
            cleaned['is_reply_comments'] = message.replies.comments
        except AttributeError:
            cleaned['is_reply_comments'] = None

        try:
            # Sometimes recent_repliers list is there but it can be empty
            if message.replies.recent_repliers and len(message.replies.recent_repliers) > 0:
                cleaned['recent_replier_ids'] = [i.user_id for i in message.replies.recent_repliers]
            # Handle when list is empty; still need to create empty list and except won't catch this
            else:
                cleaned['recent_replier_ids'] = []
        except AttributeError:
            cleaned['recent_replier_ids'] = []

        try:
            cleaned['raw_text'] = message.raw_text
        except AttributeError:
            cleaned['raw_text'] = None

        return cleaned

    def create_messages_table(self, cleaned_messages):
        print("Making messages table\n")
        df = pd.DataFrame(cleaned_messages)
        df = df.explode("recent_replier_ids") # should be 1 ID per row
        df.fillna("", inplace=True)
        df.drop_duplicates(inplace=True)
        # print(f"Messages table rows: {df.shape[0]}")
        # Needs a try except to handle if already try to create file and failed (CSV will still be there)
        df.to_csv(f"{self.folder_name}/{self.folder_name}.csv", index=False)
        print(f"Created file '{self.folder_name}.csv' in output folder\n\n")