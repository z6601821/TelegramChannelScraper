# Pip / Conda install
from telethon import TelegramClient
import pandas as pd
# Python standard libraries
from datetime import datetime as dt
from time import sleep
import os

class TelegramChannelScraper():
    def __init__(self, credentials={}):
        self.client = None
        self.is_signed_in = False
        self.credentials = credentials

        self.__is_credentials()

    def __is_credentials(self):
        if not self.credentials:
            raise Exception(
                "You must provide your Telegram API credentials! " +
                "(API ID, API hash, and account phone number)")
    
    # ////////// Client initialization, connection, and authorization //////////
    async def __init_connect_authorize_client(self):
        if not self.client:
            self.__init_client()
            print("Client initialized")
        else:
            pass
        if not self.client.is_connected():
            await self.client.connect()
            print("Client connected")
        else:
            pass
        if not await self.client.is_user_authorized():
            await self.__authorize_client()
            print("Client authorized")
        else:
            pass
        self.is_signed_in = True

    def __init_client(self):
        print("Initializing Telegram client...")
        self.client = TelegramClient(
            "", 
            self.credentials["api_id"], 
            self.credentials["api_hash"],
        )
        
    async def __authorize_client(self):
        print("Not authenticated, signing in...")
        await self.client.send_code_request(self.credentials["phone"])
        await self.client.sign_in(
            self.credentials["phone"], 
            input("Enter one-time login code sent to account phone number:")
        )

    # ////////// Get messages and helpers //////////
    async def get_messages(self, channel_name, start_message_id=None):
        if not self.is_signed_in:
            await self.__init_connect_authorize_client()
        await self.__verify_channel_name_and_exists(channel_name)
        output_folder_name = self.__create_output_folder(channel_name)
        await self.__pull_messages_in_batches(
            channel_name, 
            start_message_id, 
            output_folder_name,
            )
        print(f"\nAppended messages to file '{output_folder_name}.csv' in folder '{output_folder_name}'")
        print(f"Finished pulling messages for channel '{channel_name}'")
    
    async def __verify_channel_name_and_exists(self, channel_name):
        if not channel_name.strip():
            raise Exception("You must provide a non-empty channel name!")
        try:
            channel_info = await self.client.get_entity(channel_name)
        except ValueError:
            raise Exception("No user or channel with that name! (Try another channel name)")
        return
    
    def __create_output_folder(self, channel_name):
        today_date = dt.today().strftime("%Y-%m-%d_%H%M")
        output_folder_name = f"TelegramChannelScraper_'{channel_name}'_{today_date}"
        try:
            os.mkdir(output_folder_name)
            print(f"\nCreated folder '{output_folder_name}' to store output")
        except FileExistsError:
            print(f"\nFound existing folder {output_folder_name}' to store output")
            pass
        return output_folder_name

    async def __pull_messages_in_batches(self, channel_name, start_message_id, output_folder_name):
        # Find the current highest message ID (most recent message)
        all_messages = await self.client.get_messages(channel_name)
        highest_message_id = all_messages[0].id
        # highest_message_id = 6399 # TESTING ONLY
        print(f"Current highest message ID is {highest_message_id}")
        
        if start_message_id:
            self.__validate_start_message_id(highest_message_id, start_message_id)

        num_batches_need = self.__determine_num_batches_needed(highest_message_id, start_message_id)
        print(f"Need {num_batches_need} batches to pull messages for channel")
    
        # Slider is used to determine where to start each batch of 1000 and is incremented 
        # to the next 1000 on each pass until reaching the current highest message ID
        batch_slider = start_message_id if start_message_id else 0

        for i in range(1, num_batches_need + 1):
            batch_low_message_id = batch_slider
            batch_high_message_id = ((batch_low_message_id + 999) 
                                     if (batch_low_message_id + 999) <= highest_message_id 
                                     else highest_message_id)
            batch_slider = batch_high_message_id + 1
            
            print(f"\nWorking on batch {i} (message ID: {batch_low_message_id} - " +
                  f"message ID: {batch_high_message_id})")

            raw_batch_messages = []
            async for message in self.client.iter_messages(
                channel_name, 
                limit=1000, 
                min_id=batch_low_message_id - 1,# include message _000 instead of starting at _001
                max_id=batch_high_message_id + 1, # include message _999 instead of stopping at __998
                reverse=True, # oldest to newest
                ):
                raw_batch_messages.append(message)
            parsed_batch_messages = self.__parse_raw_messages(raw_batch_messages)
            self.__append_messages_to_file(parsed_batch_messages, output_folder_name)
            sleep(30.0)
            print(f"Finished batch {i}")
    
    def __validate_start_message_id(self, highest_message_id, start_message_id):
        if start_message_id < 0:
            raise Exception("Provided start message ID is invalid (less than 0)")
        if start_message_id > highest_message_id:
            raise Exception("Provided start message ID is greater than the current highest message ID")
        return
    
    def __determine_num_batches_needed(self, highest_message_id, start_message_id):
        """
        Returns the number of batches need to include all messsages. 
        
        Example: 
        Highest message ID is 6399. 6399 / 1000 = 6.399, rounded down to int 6. But add
        1 so total number of batches needed is 7 as 6 batches would only cover up to 5999.
        7 batches would cover up to message ID 6999. Batch 0: 0-999, batch 1: 1000-1999, 
        batch 2: 2000-2999, batch 3: 3000-3999, batch 4: 4000-4999, batch 5: 5000-5999, 
        batch 6: 6000-6999. (Starting from 0 this is 7 batches.)

        Example:
        Highest message ID is 6399 and start message ID is 5000. (6399 - 5000) / 1000 is 
        1.399, rounded down to int 1. But add 1 to make total number of batches needed 2: 
        batch 0 would cover 5000-5999 and batch 1 would cover 6000-6999. (Starting from 0 
        this is 2 batches.)
        """
        batch_size = 1000
        if not start_message_id:
            return (int(highest_message_id / batch_size) + 1)
        else:
            return (int((highest_message_id - start_message_id) / batch_size) + 1)

    def __parse_raw_messages(self, pulled_messages):
        print("Extracting fields and cleaning messages...")
        cleaned_messages = []
        for message in pulled_messages:
            cleaned_messages.append(self.__extract_message_fields(message))
        return cleaned_messages

    def __extract_message_fields(self, message):
        # Message info: https://docs.telethon.dev/en/stable/quick-references/objects-reference.html
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

    def __append_messages_to_file(self, cleaned_messages, output_folder_name):
        print("Appending messages to file...")
        df = pd.DataFrame(cleaned_messages)
        df = df.explode("recent_replier_ids") # should be 1 ID per row
        df.fillna("", inplace=True)
        df.drop_duplicates(inplace=True)
        full_output_file_path = f"{output_folder_name}/{output_folder_name}.csv"
        # If file does not yet exist, write file with headers
        if not os.path.isfile(full_output_file_path):
            df.to_csv(full_output_file_path, index=False)
        # Otherwise append new data without headers
        else:
            df.to_csv(full_output_file_path, mode="a", header=False, index=False)