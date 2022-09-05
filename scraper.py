from telethon import TelegramClient
from telethon.errors.rpcerrorlist import SessionPasswordNeededError
from telethon.tl.functions.messages import GetHistoryRequest

import asyncio
from datetime import datetime
from cleantext import clean
from string import punctuation
import re
from dotenv import load_dotenv
from os import getenv

import pandas as pd

# Get credentials
load_dotenv()
api_id = getenv("API_ID")
api_hash = getenv("API_HASH")
user_name = getenv("USER_NAME")
phone_number = getenv("PHONE_NUMBER")

# Create data frame for storing data
df = pd.DataFrame(
    data=None,
    index=None,
    columns=["Source Type", "Title", "User ID", "Message", "Date"],
)

# Load keyword list
keywords = []
with open("./src/keywords.txt", "r") as f:
    for line in f.readlines():
        keywords.append(clean(line, no_emoji=True).lower().strip())


# Start scraper
async def scraper():
    # Create client and connect
    async with TelegramClient(user_name, api_id, api_hash) as client:
        await client.connect()

        # Authorization assurance
        if not await client.is_user_authorized():
            await client.send_code_request(pone_number)
            try:
                await client.sign_in(phone_number, input("Enter verification code: "))
            except SessionPasswordNeededError:
                await client.sign_in(password=input("Password: "))

        # Set list of channels for scraping data
        dialogs = [d for d in await client.get_dialogs() if d.is_group or d.is_channel]

        # Iterate over channel/group list
        msg_count = 0
        msg_match_count = 0
        msg_no_match_count = 0
        channel_count = 0
        group_count = 0
        reply_count = 0
        for dialog in dialogs:

            if dialog.is_group:
                group_count += 1  # Update group count
            else:
                channel_count += 1  # Update channel count

            # Iterate over messages
            async for message in client.iter_messages(dialog, limit):
                # print(f"{clean(message.text, no_emoji=True)}\n")

                # Build regex pattern based on keywords
                pattern = re.compile("|".join(keywords))

                # Check if message is a reply
                if message.is_reply:
                    reply_count += 1

                msg_count += 1  # update message counter
                if message.message is None:
                    continue
                # Clean messages
                clean_msg = clean(message.message, no_emoji=True).lower().strip()

                # Check if keyword on message
                matches = re.findall(pattern, clean_msg)
                if len(matches) > 0:
                    msg_match_count += 1  # Update message match count
                    user_id = None
                    source_title = dialog.name
                    raw_message = clean(message.message, no_emoji=True)
                    date = message.date.date()
                    source_type = "Group" if dialog.is_group else "Channel"
                    if dialog.is_group and message.from_id:
                        try:
                            user_id = message.from_id.user_id
                        except Exception as e:
                            print(e)
                            user_id = message.from_id.channel_id
                    else:
                        try:
                            if message.from_id:
                                user_id = message.from_id.channel_id
                            if message.fwd_from:
                                user_id = message.fwd_from.from_id.channel_id
                            if message.fwd_from is None:
                                user_id = message.peer_id.channel_id
                        except Exception as e:
                            if message.from_id:
                                user_id = message.from_id.user_id
                            if message.fwd_from and message.fwd_from.from_id:
                                user_id = message.fwd_from.from_id.user_id
                            if message.fwd_from is None:
                                # user_id = message.peer_id.user_id
                                user_id = "NA"
                            print(f"\n{e}\n")
                    try:
                        df.loc[len(df)] = [
                            source_type,
                            source_title,
                            user_id,
                            raw_message,
                            date,
                        ]
                    except Exception as e:
                        print(e)
                else:
                    msg_no_match_count += 1

        print("\n")
        print(f"[+] {len(dialogs)} dialogs checked")
        print(f"[+] {group_count} groups checked")
        print(f"[+] {channel_count} channels checked")
        print(f"[+] {msg_count} messages checked")
        print(f"[+] {reply_count} message replies checked")
        print(f"[+] {msg_match_count} messages matches")
        print(f"[+] {msg_no_match_count} messages didn't match")
        print("\n")
        df.to_csv("./data/messages.csv", index=False)
