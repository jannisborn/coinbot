import os
import sys
import threading
import time
from collections import defaultdict
from random import random
from typing import Tuple

import requests
import telegram
from loguru import logger
from telegram import Message, Update
from telegram.ext import CommandHandler, Filters, MessageHandler, Updater

from coinbot.db import DataBase
from coinbot.llm import INSTRUCTION_MESSAGE, LLM, get_feature_value
from coinbot.metadata import countries
from coinbot.slack import SlackClient
from coinbot.utils import (
    contains_germany,
    get_tuple,
    large_int_to_readable,
    log_to_csv,
    logger_filter,
    sane_no_country,
)
from coinbot.vectorstorage import VectorStorage

logger.remove()
logger.add(sys.stderr, filter=logger_filter)

missing_hints = ["feature", "missing", "provided", "not"]
username_message = "Only one more thing: What's your name? 🤗"


class CoinBot:
    def __init__(
        self,
        public_link: str,
        telegram_token: str,
        anyscale_token: str,
        slack_token: str,
        vectorstorage_path: str,
    ):
        # Load tokens and initialize variables
        self.telegram_token = telegram_token
        self.anyscale_token = anyscale_token

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

    def fetch_file(self, link: str):
        """
        Download a file from a given path and save to `self.filepath`.

        Args:
            link: The public link from which to download the file
        """
        response = requests.get(link)
        # Check if the request was successful
        if response.status_code == 200:
            # Write the content of the response to a file
            with open(self.filepath, "wb") as f:
                f.write(response.content)
            logger.debug(f"File downloaded successfully from {link}")
        else:
            logger.warning(f"Failed to download file from {link}")

    def start_periodic_reload(self, interval: int = 3600):
        """Starts the periodic reloading of data."""
        self.reload_data()
        # Set up a timer to call this method again after `interval` seconds
        threading.Timer(interval, self.start_periodic_reload, [interval]).start()

    def reload_data(self):
        """Fetches the file and re-initializes the database."""
        logger.debug("Reloading data...")
        # TODO: Rewrite the vector storage periodically (if new coins were added)
        try:
            self.fetch_file(link=self.public_link)
            self.db = DataBase(self.filepath)
            logger.debug("Data reloaded successfully.")
        except Exception as e:
            logger.error(f"Failed to reload data: {e}")

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
            self.return_message(update, response)
            if self.user_prefs[user_id]["collecting_username"]:
                time.sleep(0.5)
                self.return_message(update, username_message)
            elif not self.user_prefs[user_id]["collecting_language"]:
                time.sleep(0.5)
                self.return_message(update, "Here are the instructions again:")
                context.bot.send_chat_action(
                    chat_id=update.message.chat_id, action=telegram.ChatAction.TYPING
                )
                response_message = self.return_message(update, INSTRUCTION_MESSAGE)
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
            response = f"Language was set to {text}. You can always change it by texting `Language: YOUR_LANGUAGE`."
            self.return_message(update, response)
            if text.capitalize() == "English":
                time.sleep(0.2)
            self.return_message(update, username_message)
            self.user_prefs[user_id]["collecting_language"] = False
            self.user_prefs[user_id]["collecting_username"] = True
            return True
        elif self.user_prefs[user_id]["collecting_username"]:
            self.user_prefs[user_id]["username"] = text
            context.bot.unpin_all_chat_messages(chat_id=update.message.chat_id)
            reponse_name = self.return_message(
                update,
                f"Nice to meet you, {text}!🤝 You can always change your username by texting `Name: YOUR_NAME`\nHere is the manual:",
            )
            context.bot.send_chat_action(
                chat_id=update.message.chat_id, action=telegram.ChatAction.TYPING
            )
            # Pinning the message to change username
            context.bot.pin_chat_message(
                chat_id=update.message.chat_id,
                message_id=reponse_name.message_id,
                disable_notification=False,
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
            self.translate_llm = LLM(
                model="Open-Orca/Mistral-7B-OpenOrca",
                token=self.anyscale_token,
                task_prompt=(
                    f"You are a translation chatbot. Translate the following into {language}, use colloquial language like in a personal chat"
                ),
                temperature=0.5,
            )
            if "`Special" in text:
                # Split by each occurrence of "`Special", translate snippets and then fuse with "`Special"
                snippets = text.split("`Special")
                t_snips = [self.translate_llm(snippet) for snippet in snippets]
                t_snips = [
                    t.split("➡️")[0] + "` ➡️ " + t.split("➡️")[1] if "➡️" in t else t
                    for t in t_snips
                ]
                text = "\n`Special".join(t_snips)
            else:
                text = self.translate_llm(text)

            response_message = update.message.reply_text(text, parse_mode="Markdown")

        log_to_csv(update.message.text, text)
        return response_message

    def handle_text_message(self, update, context):

        if random() < 0.005:
            output = self.joke_llm(update.message.text)
            self.return_message(update, output)
            return
        is_setting_up = self.setup(update, context)
        if not is_setting_up:
            self.search_coin_in_db(update, context)

    def extract_features(self, llm_output: str) -> Tuple[str, int, str]:
        """
        Extracts the country, year, value, and source from the LLM output.

        Args:
            llm_output: The output from the LLM model.

        Returns:
            Tuple[str, int, str, str]: The country, year, value, and source.
        """
        c = get_feature_value(llm_output, "Country")
        value = get_feature_value(llm_output, "Value").lower()
        value = value.replace("€", " euro").replace("  ", " ")
        year = get_feature_value(llm_output, "Year")
        try:
            year = int(year)
        except ValueError:
            year = -1
        country = c if c == "" else self.to_english_llm(c)
        country = country.strip().lower()
        return country, year, value

    def format_coin_result(self, row) -> str:
        amount = large_int_to_readable(row["Amount"] * 1000)
        header = f"Title: {row['Name']}"
        if row["IsFederalStateSeries"]:
            header += f" {row['Year']} {row['Source'].capitalize()}"
            further = f"(Country = {row['Country'].capitalize()};"
        elif row["Country-specific"]:
            header += f" {row['Country'].capitalize()}"
            further = f"(Year: {row['Year']};"
            if row.Country == "germany":
                header += f" {row['Source'].upper()}"
        else:
            further = f"(Country: {row['Country'].capitalize()}, Year: {row['Year']};"

        further += f" Total coin count: {amount})"
        return f"{header}\n{further}"

    def search_special_coin(self, update, message: str):
        user_id = update.message.from_user.id
        # User asked for a special/commemorative coin
        to_llm = message.split("Special")[1]
        print("to lLm", to_llm)
        output = self.special_llm(to_llm)

        output += "\nValue: 2 Euro"
        print("LLM output", output)
        # Extract feature values
        country, year, value = self.extract_features(output)
        if country.capitalize() not in countries:
            country = "none"
        if year < 1999:
            year = -1
        name = get_feature_value(output, "Name")
        print("Features", country, year, value, name)
        # Nail down the Dataframe feature by feature
        tdf = self.db.df[
            (self.db.df["Coin Value"] == "2 euro") & (self.db.df["Special"])
        ]
        print(len(tdf), "org", len(self.db.df))

        # If year was given, filter by year
        if year != -1:
            tdf = tdf[(tdf.Year == year)]
            print("Year", year, len(tdf))

        # If country was given, filter by country
        if country and country.lower() not in ["unknown", "missing", "none"]:
            tdf = tdf[(tdf.Country == country)]
            print("Country", country, len(tdf))
            if country == "germany":
                sources = [
                    s.lower()
                    for s in message.split(" ")
                    if len(s) == 1 and s.lower() in ["a", "d", "f", "g", "j"]
                ]
                if len(sources) == 1:
                    source = sources[0]
                elif len(sources) > 1:
                    self.return_message(
                        update,
                        f"Found more than one mint location: {sources}, try again!",
                    )
                else:
                    source = None
                if source:
                    tdf = tdf[(tdf.Source == source)]
        if len(name) > 4 and name.lower() not in ["unknown", "missing", "none"]:
            tdf = self.vectorstorage.query(name, df=tdf, verbose=True).sort_values(
                by="Distance", ascending=True
            )
            # TODO: Threshold could be optimized
            tdf = tdf[tdf["Distance"] < 0.65]
        query = "\n\n\tValue = 2 Euro\n"
        if year != -1:
            query += f"\tYear = {year}\n"
        if country:
            query += f"\tCountry = {country.capitalize()}\n"
        if name:
            query += f"\tName = {name}\n"
        if country == "germany" and source:
            query += f"\tMint location = {source.upper()}\n"

        final_df = tdf[tdf["Status"] == "collected"]
        query += "\n"

        # Return all coins in DB
        if len(final_df) == 0:

            response = f"🚀🎉 Hooray! A commemorative coin:{query}is not yet in the collection 🤩"
            self.return_message(update, response)
            self.slackbot(f"User {self.user_prefs[user_id]['username']}: {response}")
            return
        self.return_message(
            update,
            f"For your query {query}the following coins are already in the collection:",
        )
        final_df = final_df.sort_values(by=["Year", "Country", "Name"])
        for i, row in final_df.iterrows():
            text = self.format_coin_result(row)
            self.return_message(update, text)

        print()

    def search_coin_in_db(self, update, context):
        """Search for a coin in the database when a message is received."""

        # try:
        if True:
            user_id = update.message.from_user.id
            # Check if the user's language preference is set
            if user_id not in self.user_prefs:
                # Ask for the user's language preference
                update.message.reply_text("Which language do you want me to speak?")
                return

            # Parse the message
            message = update.message.text
            logger.debug(f"Received: {message}")

            if message.lower().startswith("special"):
                return self.search_special_coin(update, message)

            if sane_no_country(message):
                message += " Germany "

            if contains_germany(message, threshold=99):
                output = self.ger_llm(message)
                if any([x in output.lower() for x in missing_hints]) or any(
                    [
                        x not in output.lower()
                        for x in ["source", "year", "country", "value"]
                    ]
                ):
                    self.return_message(
                        update,
                        text=output
                        + "\nFor a German coin, you need to provide the features `year`, `country`, `coin value` and `mint location` (A, D, F, G or J)",
                    )
                    return

                source = get_feature_value(output, "Source").lower()
            else:
                output = self.eu_llm(message)
                if any([x in output.lower() for x in missing_hints]) or any(
                    [x not in output.lower() for x in ["year", "country", "value"]]
                ):
                    self.return_message(
                        update,
                        text=output
                        + "\nYou need to provide the features `year`, `country` and `coin value`",
                    )
                    return
                source = None

            country, year, value = self.extract_features(output)
            logger.debug("Feature extraction LLM says", output)
            logger.debug("Features for lookup", country, year, value, source)

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

            match = get_tuple(country, value, year, source)

            # Respond to the user
            if len(coin_df) == 0:
                response = f"🤷🏻‍♂️ The coin {match} was not found. Check your input 🧐"
                print(f"Returns: {response}\n")
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

            res = response.split("\n")[0]
            self.return_message(update, response, amount=amount)

        # except Exception as e:
        #     response = f"An error occurred: {e}"
        #     self.return_message(update, response)

    def run(self):
        logger.info("Starting bot")
        self.start_periodic_reload()
        self.updater.start_polling()
        self.updater.idle()

    def set_llms(self):
        self.eu_llm = LLM(
            model="Open-Orca/Mistral-7B-OpenOrca",
            token=self.anyscale_token,
            task_prompt="You are a feature extractor! Extract 3 features, Country, value and Year. Use a colon (:) before each feature value. If one of the three features is missing reply simply with `Missing feature`",
            temperature=0.0,
        )
        self.ger_llm = LLM(
            model="Open-Orca/Mistral-7B-OpenOrca",
            token=self.anyscale_token,
            task_prompt=(
                "You are a feature extractor! Extract 4 features, Country, value, year and source. The source is given as single character, A, D, F, G or J. If one of the three features is missing reply simply with `Missing feature`. Do not overlook the source!"
                "Use a colon (:) before each feature value"
            ),
            temperature=0.0,
        )
        self.joke_llm = LLM(
            model="Open-Orca/Mistral-7B-OpenOrca",
            token=self.anyscale_token,
            task_prompt=(
                "Tell me a very short joke about the following coin. Start with `Here's a funny story about your coin:`"
            ),
            temperature=0.6,
        )
        self.to_english_llm = LLM(
            model="Open-Orca/Mistral-7B-OpenOrca",
            token=self.anyscale_token,
            task_prompt=(
                "Give me the ENGLISH name of this country. Be concise, only one word."
            ),
            temperature=0.0,
        )
        self.special_llm = LLM(
            model="Open-Orca/Mistral-7B-OpenOrca",
            token=self.anyscale_token,
            task_prompt="You are a feature extractor! Extract up to three (3) features; Country, year and name. The name can be the name of a state, city, a celebrity or any other text, BUT it must NOT be a country and it must NOT be a single character! Use a colon (:) before each feature value. Ignore missing features. Do NOT invent information, only EXTRACT.",
            temperature=0.5,
        )
