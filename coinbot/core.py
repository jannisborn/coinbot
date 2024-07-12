import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from random import random
from typing import Tuple

import pandas as pd
import requests
import telegram
from loguru import logger
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    Filters,
    MessageHandler,
    Updater,
)

from coinbot.db import DataBase
from coinbot.llm import INSTRUCTION_MESSAGE, LLM, get_feature_value
from coinbot.slack import SlackClient
from coinbot.utils import (
    CURRENT_YEAR,
    contains_germany,
    fuzzy_search_country,
    get_file_content,
    get_tuple,
    get_year,
    large_int_to_readable,
    log_to_csv,
    sane_no_country,
)
from coinbot.vectorstorage import VectorStorage

log_level = os.getenv("LOGLEVEL", "DEBUG")
logger.configure(handlers=[{"sink": sys.stdout, "level": log_level}])
logger.debug("Starting script")


missing_hints = ["miss", "provided", "not"]
username_message = " Only one more thing: What's your name? 🤗"


class CoinBot:
    def __init__(
        self,
        public_link: str,
        telegram_token: str,
        anyscale_token: str,
        slack_token: str,
        vectorstorage_path: str,
        base_llm: str = "meta-llama/Meta-Llama-3-8B-Instruct",
    ):
        # Load tokens and initialize variables
        self.telegram_token = telegram_token
        self.anyscale_token = anyscale_token
        self.base_llm = base_llm

        # Initialize language preferences dictionary
        self.user_prefs = defaultdict(dict)

        # Initialize the bot and dispatcher
        self.updater = Updater(self.telegram_token, use_context=True)
        self.dp = self.updater.dispatcher

        # Register handlers
        self.dp.add_handler(
            CommandHandler(
                "start", lambda update, context: update.message.reply_text("Hi!")
            )
        )
        self.dp.add_handler(
            MessageHandler(Filters.text & (~Filters.command), self.handle_text_message)
        )
        self.dp.add_handler(CallbackQueryHandler(self.callback_query_handler))
        self.dp.add_error_handler(self.error_handler)

        self.filepath = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "coins.xlsm"
        )
        self.public_link = public_link
        self.fetch_file(link=public_link)
        self.db = DataBase(self.filepath)
        self.vectorstorage_path = vectorstorage_path
        self.vectorstorage = VectorStorage.load(vectorstorage_path)

        self.set_llms()
        self.slackbot = SlackClient(slack_token)

    def error_handler(self, update, context):
        def shutdown():
            self.updater.stop()
            self.updater.is_idle = False

        logger.error(f'Update "{update}" caused error "{context.error}"')
        threading.Thread(target=shutdown).start()
        time.sleep(3)

    def fetch_file(self, link: str):
        """
        Download a file from a given path and save to `self.filepath`.

        Args:
            link: The public link from which to download the file
        """
        if os.path.exists(self.filepath):
            last_modified_time = datetime.fromtimestamp(os.path.getmtime(self.filepath))
            if datetime.now() - last_modified_time < timedelta(hours=12):
                logger.info(
                    f"File at {self.filepath} is up-to-date. Skipping download."
                )
                return

        logger.debug("Downloading data...")
        response = requests.get(link)
        # Check if the request was successful
        if response.status_code == 200:
            # Write the content of the response to a file
            with open(self.filepath, "wb") as f:
                f.write(response.content)
            logger.info(f"File downloaded successfully from {link}")
        else:
            logger.warning(f"Failed to download file from {link}")

    def start_periodic_reload(self, interval: int = 3600):
        """Starts the periodic reloading of data."""
        # Set up a timer to call this method after `interval` seconds
        threading.Timer(interval, self.reload_data, [interval]).start()

    def reload_data(self, interval: int = 3600):
        """Fetches the file and re-initializes the database."""
        try:
            self.fetch_file(link=self.public_link)
            self.db = DataBase(self.filepath)
            logger.debug("Data reloaded successfully.")
        except Exception as e:
            logger.error(f"Failed to reload data: {e}")
        threading.Timer(interval, self.reload_data, [interval]).start()

    def setup(self, update, context) -> bool:
        """
        Set up the user's language preference and collect their name.
        Returns whether the user message was part of the setup process.
        """

        user_id = update.message.from_user.id
        text = update.message.text.strip()
        overwrite_language = text.lower().startswith(
            "language:"
        ) or text.lower().startswith("sprache:")
        overwrite_username = text.lower().startswith(
            "username:"
        ) or text.lower().startswith("name:")

        if text.lower().startswith("status"):
            self.return_message(update, self.db.get_status())
            return True

        if overwrite_username:
            if "username" in self.user_prefs[user_id].keys():
                response = (
                    f"Username used to be {self.user_prefs[user_id]['username']}.\n"
                )
            else:
                response = ""
            new_username = text.split(":")[-1].strip()
            response += f"Username has now been set to {new_username}."
            self.user_prefs[user_id]["username"] = new_username
            self.return_message(update, response)
            return True

        elif overwrite_language:
            if "language" in self.user_prefs[user_id].keys():
                response = (
                    f"Language used to be {self.user_prefs[user_id]['language']}.\n"
                )
            else:
                response = ""
            new_language = text.split(":")[-1].strip()
            response += f"Language has now been set to {new_language}."
            self.user_prefs[user_id]["language"] = new_language
            if self.user_prefs[user_id]["collecting_username"]:
                time.sleep(0.5)
                self.return_message(update, username_message)
            elif not self.user_prefs[user_id]["collecting_language"]:
                time.sleep(0.5)
                response += "\nHere are the instructions again:\n"
                context.bot.send_chat_action(
                    chat_id=update.message.chat_id, action=telegram.ChatAction.TYPING
                )
                response_message = self.return_message(
                    update, response + INSTRUCTION_MESSAGE
                )
                # Pinning the message
                context.bot.unpin_all_chat_messages(chat_id=update.message.chat_id)
                context.bot.pin_chat_message(
                    chat_id=update.message.chat_id,
                    message_id=response_message.message_id,
                    disable_notification=False,
                )
            return True
        # Check if the user's language preference is already set
        elif user_id not in self.user_prefs:
            update.message.reply_text(
                "Welcome!\nThis is Jannis' coincollector! 🪙\n\nWhich language do you want me to speak?"
            )
            self.user_prefs[user_id]["collecting_language"] = True
            return True
        elif self.user_prefs[user_id]["collecting_language"]:
            # Set language
            self.user_prefs[user_id]["language"] = text.capitalize().strip()
            response = f"Language was set to {text}. You can always change it by writing\n`Language: YOUR_LANGUAGE`."
            if text.capitalize() == "English":
                time.sleep(0.2)
            self.return_message(update, response + username_message)
            self.user_prefs[user_id]["collecting_language"] = False
            self.user_prefs[user_id]["collecting_username"] = True
            return True
        elif self.user_prefs[user_id]["collecting_username"]:
            self.user_prefs[user_id]["username"] = text
            context.bot.unpin_all_chat_messages(chat_id=update.message.chat_id)
            txt = f"Nice to meet you, {text}!🤝 You can always change your username by texting\n`Name: YOUR_NAME`\nHere is the manual:"
            if self.user_prefs[user_id]["language"] != "English":
                txt += "(Sorry translating this may take a while)"
            reponse_name = self.return_message(update, txt)

            # Pinning the message to change username
            context.bot.pin_chat_message(
                chat_id=update.message.chat_id,
                message_id=reponse_name.message_id,
                disable_notification=False,
            )
            context.bot.send_chat_action(
                chat_id=update.message.chat_id, action=telegram.ChatAction.TYPING
            )
            response_message = self.return_message(update, INSTRUCTION_MESSAGE)
            time.sleep(1)

            # Pin instructions
            context.bot.pin_chat_message(
                chat_id=update.message.chat_id,
                message_id=response_message.message_id,
                disable_notification=False,
            )
            self.user_prefs[user_id]["collecting_username"] = False
            return True
        elif "language" in self.user_prefs[user_id].keys():
            # Language was already set
            return False
        else:
            update.message.reply_text("No language recognized, consider setting it")
            return False

    def return_message(self, update: Update, text: str, amount: int = 0) -> Message:
        user_id = update.message.from_user.id
        if amount > 0:
            number_text = large_int_to_readable(amount * 1000)
            text = f"{text}\n\n(Coin was minted {number_text} times)"

        if user_id not in self.user_prefs.keys():
            language = "English"
        else:
            language = self.user_prefs[user_id].get("language", "English")
        if language == "English":
            response_message = update.message.reply_text(text, parse_mode="Markdown")
        else:
            # # Uncommented until a better LM is available
            # self.translate_llm = LLM(
            #     model="meta-llama/Llama-3-70b-chat-hf",
            #     token=self.anyscale_token,
            #     task_prompt=(
            #         f"You are a translation tool. Translate the following into {language}. Translate exactly and word by word. NEVER make any meta comments!"
            #         "Here's the text to translate:\n\n"
            #     ),
            #     temperature=0.5,
            #     remind_task=1,
            # )
            # if "`Special" in text:
            #     # Split by each occurrence of "`Special", translate snippets and then fuse with "`Special"
            #     snippets = text.split("`Special")
            #     t_snips = [self.translate_llm(snippet) for snippet in snippets]
            #     t_snips = [
            #         t.split("➡️")[0] + "` ➡️ " + t.split("➡️")[1] if "➡️" in t else t
            #         for t in t_snips
            #     ]
            #     text = "\n`Special".join(t_snips)
            # else:
            #     text = self.translate_llm(text)
            #     text = (
            #         text
            #         if text != ""
            #         else "A translation error occurred. Please set language to English"
            #     )
            update.message.reply_text(
                f"Language set to {language}. Currently only `English` is supported. Set by typing `Language: english`."
            )
            response_message = update.message.reply_text(text)

        log_to_csv(update.message.text, text)
        return response_message

    def verify(self, update, context):
        user_id = update.message.from_user.id
        # Check if the user's language preference is set
        if user_id not in self.user_prefs:
            # Ask for the user's language preference
            update.message.reply_text("Which language do you want me to speak?")
            return False
        return True

    def handle_text_message(self, update, context):

        if random() < 0.005:
            output = self.joke_llm(update.message.text)
            self.return_message(update, output)
            return
        is_setting_up = self.setup(update, context)
        if is_setting_up:
            return

        if not self.verify(update, context):
            return

        # Determine whether query is about searching a coin or querying a series
        msg = update.message.text.lower().strip()
        if msg.startswith("series"):
            self.extract_and_report_series(update, msg)
        else:
            # Query the DB with a specific coin
            self.search_coin_in_db(update, context)

    def extract_and_report_series(self, update, context):
        """
        Report the status of a series (year, country)-tuple of coins.
        """
        message = update.message.text.lower().strip()

        year = get_year(message)
        if year == -1:
            self.return_message(
                update,
                "No year or multiple year found, please provide single year with four digits.",
            )
            return

        words = message.split("series ")[1].split(" ")
        words.remove(str(year))
        country = self.to_english_llm(" ".join(words)).strip().lower()

        # Search in the dataframe
        coin_df = self.db.df[
            (self.db.df["Country"] == country)
            & (self.db.df["Year"] == year)
            & (~self.db.df["Special"])
        ]
        if len(coin_df) == 0:
            response = f"🤷🏻‍♂️ For year {year} and country {country} no data was found. Check your input 🧐"
            self.return_message(update, response)

        self.report_series(update, coin_df)

    def report_series(self, update, coin_df: pd.DataFrame, special: bool = False):
        """
        Report the search status of a series of results.
        """
        dict_mapper = {"unavailable": "⚫", "collected": "✅", "missing": "❌"}
        for i, row in coin_df.iterrows():
            status = row["Status"]
            icon = dict_mapper[status]

            if special:
                image = get_file_content(row.Link) if row.Link else None
                match = get_tuple(
                    row.Country, row.Year, row["Source"], name=row.Name, isspecial=True
                )
            else:
                match = get_tuple(
                    row.Country, row.Year, row["Source"], value=row["Coin Value"]
                )

            if status != "unavailable" and row["Amount"] > 0:
                amount = " (Mints: " + large_int_to_readable(row["Amount"] * 1000) + ")"
            else:
                amount = ""
            response = f"{match}:\n{icon}{status.upper()}{icon}{amount}"

            if special:
                if image is None:
                    response = response.replace("(Mints:", "📷 No picture 📷(Mints:")
                    update.message.reply_text(response, parse_mode="Markdown")
                else:
                    update.message.reply_photo(
                        photo=image, caption=response, parse_mode="Markdown"
                    )
            else:
                update.message.reply_text(response, parse_mode="Markdown")

    def extract_features(self, llm_output: str) -> Tuple[str, int, str]:
        """
        Extracts the country, year, value, and source from the LLM output.

        Args:
            llm_output: The output from the LLM model.

        Returns:
            Tuple[str, int, str, str]: The country, year, value, and source.
        """
        c = get_feature_value(llm_output, "country")
        value = get_feature_value(llm_output, "value").lower().strip()
        value = (
            value.replace("€", " euro")
            .replace("cents", "cent")
            .replace("  ", " ")
            .strip()
        )

        if "cent" not in value and "euro" not in value:
            if int(value) in [5, 10, 20, 50]:
                value += " cent"

        year = get_feature_value(llm_output, "year")
        logger.debug(f"Raw features {c}, {value}, {year}")
        try:
            year = int(year)
        except ValueError:
            year = -1

        country = c if c == "" else self.to_english_llm(c)
        country = country.strip().lower().replace(".", "")
        return country, year, value

    def get_year_from_full(self, update, text: str) -> int:
        years = []
        for word in text.split(" "):
            y = get_year(word)
            if y < 1999 or y > CURRENT_YEAR:
                continue
            years.append(y)
        if len(years) > 1:
            self.return_message(update, f"Found more than one year {years}, try again!")
            return -1
        elif len(years) == 0:
            return -1
        return years[0]

    def search_special_coin(self, update, message: str):
        # User asked for a special/commemorative coin
        text = message.split("Special")[1].strip()

        # Extract basic features
        year = self.get_year_from_full(update, text)
        country, matched = fuzzy_search_country(text)
        logger.debug(f"Special coin: {text}, Country: {country}, Year: {year}")

        coin_df = self.db.df[self.db.df["Special"]]
        num_specials = len(coin_df)
        query = ""
        if year != -1:
            coin_df = coin_df[coin_df["Year"] == year]
            query += f"`Year: {year}`, "
            logger.debug(f"After Year {year}: {len(coin_df)} entries remain")
        if country != "":
            coin_df = coin_df[coin_df["Country"] == country]
            query += f"`Country: {country.capitalize()}`, "
            logger.debug(f"After country {country}: {len(coin_df)} entries remain")

        text = text.replace(str(year), "").replace(matched, "").strip()
        logger.debug(f"Remaining text: {text}")
        if len(text.split(" ")) > 0:
            # The query contains more information. Pass this to the vectorstorage
            coin_df = self.vectorstorage.query(text, coin_df)
            query += f"`Description: {text}`"
            index = True
        else:
            index = False

        if len(coin_df) == 0:
            self.return_message(
                update,
                f"For your special coin with:\n{query}\nthere are no special 2 euro coins. Please retry!",
            )
            return
        elif not index and len(coin_df) == num_specials:
            self.return_message(
                update, "Be more specific, the query could not be parsed. Please retry!"
            )
            return

        if not index:
            self.return_message(
                update,
                f"Found {len(coin_df)} special coins for your query:\n{query}",
            )
            coin_df = coin_df.sort_values(by=["Year", "Country", "Name"])
            self.report_series(update, coin_df, special=True)
            return

        self.return_message(update, f"Results for your special coin query:\n{query}")
        # Performed vector index lookup, so needs to enter loop to potentially display more
        user_id = update.message.from_user.id
        self.user_prefs[user_id]["data"] = coin_df
        self.keep_displaying_special(update, user_id=user_id)

    def keep_displaying_special(self, update, user_id: int, start_index: int = 0):
        """
        Displays a slice of the DataFrame and asks if the user wants to see more.

        Args:
            update: The Update object used to send messages. Contains the user ID
                and thus also the data to display
            user_id: The user ID.
            start_index: The index to start slicing the DataFrame from.
        """
        coin_df = self.user_prefs[user_id]["data"]
        end_index = start_index + 5
        slice_df = coin_df.iloc[start_index:end_index]

        # Display these coins using the existing report_series method
        if not slice_df.empty:
            self.report_series(update, slice_df, special=True)
        else:
            self.return_message(update, "No more special coins to display.")
            return

        # Check if there are more items to display
        if end_index < len(coin_df):
            # Send a message with a button asking if the user wants to see more
            keyboard = [
                [
                    InlineKeyboardButton(
                        "Show more", callback_data=f"showmore_{end_index}"
                    )
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            update.message.reply_text(
                "Would you like to see more?", reply_markup=reply_markup
            )
        else:
            update.message.reply_text("Those were all special coins 🙂")

    def callback_query_handler(self, update, context):
        """
        Handles callback queries for pagination of special coins display.
        """
        query = update.callback_query
        query.answer()
        user_id = query.from_user.id

        # Extract the index from the callback data
        _, start_index = query.data.split("_")
        start_index = int(start_index)
        logger.debug(f"Continue displaying from entry {start_index} for user {user_id}")

        # Continue displaying special coins starting from the next index
        self.keep_displaying_special(query, user_id=user_id, start_index=start_index)

    def search_coin_in_db(self, update, context):
        """Search for a coin in the database when a message is received."""

        if True:
            user_id = update.message.from_user.id
            # Parse the message
            message = update.message.text
            logger.debug(f"Received: {message}")

            if message.lower().startswith("special"):
                return self.search_special_coin(update, message)

            if sane_no_country(message):
                message += " Germany "

            if contains_germany(message, threshold=99):
                output = self.ger_llm(message).lower()
                logger.debug(f"German model says {output}")
                if any([x in output for x in missing_hints]) or any(
                    [x not in output for x in ["source", "year", "country", "value"]]
                ):
                    self.return_message(
                        update,
                        text=output
                        + "\nFor a German coin, you need to provide the features `year`, `country`, `coin value` and `mint location` (A, D, F, G or J)",
                    )
                    return

                source = get_feature_value(output, "source").lower()
            else:
                output = self.eu_llm(message).lower()
                logger.debug(f"EU model says {output}")
                if any([x in output for x in missing_hints]) or any(
                    [x not in output for x in ["year", "country", "value"]]
                ):
                    self.return_message(
                        update,
                        text=output
                        + "\nYou need to provide the features `year`, `country` and `coin value`",
                    )
                    return
                source = None
            country, year, value = self.extract_features(output)
            logger.debug(f"Features for lookup: {country, year, value, source}")

            # Search in the dataframe
            coin_df = self.db.df[
                (self.db.df["Country"] == country)
                & (self.db.df["Coin Value"] == value)
                & (self.db.df["Year"] == year)
                & (
                    (
                        (self.db.df["Country"] == "germany")
                        & (self.db.df["Source"] == source)
                    )
                    | (
                        (self.db.df["Country"] != "germany")
                        & (self.db.df["Source"].isna())
                    )
                )
            ]

            match = get_tuple(country, year, source, value=value)

            # Respond to the user
            if len(coin_df) == 0:
                response = f"🤷🏻‍♂️ The coin {match} was not found. Check your input 🧐"
                logger.info(f"Returns: {response}\n")
                self.return_message(update, response)
                return

            coin_status = coin_df["Status"].values[0]
            if coin_status == "unavailable":
                response = f"🤯 Are you sure? The coin {match} should not exist. If you indeed have it, it's a SUPER rare find!"
                amount = 0
            elif coin_status == "missing":
                response = (
                    f"🚀🎉 Hooray! The coin {match} is not yet in the collection 🤩"
                )
                amount = coin_df["Amount"].values[0]
                self.slackbot(
                    f"User {self.user_prefs[user_id]['username']}: {response} (Amount: {amount})"
                )
            elif coin_status == "collected":
                response = f"😢 No luck! The coin {match} was already collected 😢"
                amount = coin_df["Amount"].values[0]
            else:
                response = "❓Coin not found."

            response = response.split("\n")[0]
            self.return_message(update, response, amount=amount)

            if coin_status == "missing":
                # Subsequently print status update
                time.sleep(1)
                self.return_message(
                    update,
                    self.db.status_delta(year=year, value=value, country=country),
                )

        # except Exception as e:
        #     self.return_message(update, f"An error occurred: {e}")

    def run(self):
        logger.info("Starting bot")
        self.start_periodic_reload()
        self.updater.start_polling()
        self.updater.idle()

    def set_llms(self):
        self.eu_llm = LLM(
            model=self.base_llm,
            token=self.anyscale_token,
            task_prompt="You are a feature extractor! Extract 3 features, Country, coin value (in euro or cents) and year. Never give the coin value in fractional values, use 10 cent rather than 0.1 euro. Use a colon (:) before each feature value. If one of the three features is missing reply simply with `Missing feature`. Be concise and efficient!",
            temperature=0.0,
        )
        self.ger_llm = LLM(
            model=self.base_llm,
            token=self.anyscale_token,
            task_prompt=(
                "You are a feature extractor! Extract 4 features, Country, coin value (in euro or cents), year and source. The source is given as single character, A, D, F, G or J. Never give the coin value in fractional values, use 10 cent rather than 0.1 euro. If one of the three features is missing reply simply with `Missing feature`. Do not overlook the source!"
                "Use a colon (:) before each feature value. Be concise and efficient!"
            ),
            temperature=0.0,
        )
        self.joke_llm = LLM(
            model=self.base_llm,
            token=self.anyscale_token,
            task_prompt=(
                "Tell me a very short joke about the following coin. Start with `Here's a funny story about your coin:`"
            ),
            temperature=0.6,
        )
        self.to_english_llm = LLM(
            model=self.base_llm,
            token=self.anyscale_token,
            task_prompt=("Give me the ENGLISH name of this country. Be concise!"),
            temperature=0.0,
        )
