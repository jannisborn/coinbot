import os
import threading
from collections import defaultdict
from random import random

import requests
from loguru import logger
from telegram.ext import CommandHandler, Filters, MessageHandler, Updater

from coinbot.db import DataBase
from coinbot.llm import LLM, get_feature_value
from coinbot.utils import (
    contains_germany,
    get_tuple,
    large_int_to_readable,
    log_to_csv,
    string_to_bool,
)


class CoinBot:
    def __init__(self, public_link: str, telegram_token: str, anyscale_token: str):
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

        self.set_llms()

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
        logger.info("Reloading data...")
        try:
            self.fetch_file(link=self.public_link)
            self.db = DataBase(self.filepath)
            logger.info("Data reloaded successfully.")
        except Exception as e:
            logger.error(f"Failed to reload data: {e}")

    def collecting_language(self, update, context) -> bool:
        user_id = update.message.from_user.id
        text = update.message.text.strip()
        overwrite = text.lower().startswith("language:") or text.lower().startswith(
            "sprache:"
        )

        # Check if the user's language preference is already set
        if user_id not in self.user_prefs:
            update.message.reply_text(
                "Welcome!\nThis is Jannis' coincollector! 🪙\n\nWhich language do you want me to speak?"
            )
            self.user_prefs[user_id]["collecting"] = True
            return True
        elif self.user_prefs[user_id]["collecting"]:
            # Set language
            self.user_prefs[user_id]["language"] = text
            response = f"Language was set to {text}. You can always change it by texting `Language: YOUR_LANGUAGE`."
            update.message.reply_text(response)
            if text.lower() != "english":
                self.return_message(update, response)
            self.user_prefs[user_id]["collecting"] = False
            return True
        elif overwrite:
            if "language" in self.user_prefs[user_id].keys():
                response = (
                    f"Language used to be {self.user_prefs[user_id]['language']}.\n"
                )
            else:
                response = ""
            response += f"Language has now been set to {text}."
            self.user_prefs[user_id]["language"] = text
            if text.lower() != "english":
                self.return_message(update, response)
            return True
        elif "language" in self.user_prefs[user_id].keys():
            # Language was already set
            return False
        else:
            update.message.reply_text("No language recognized, consider setting it")
            return False

    def return_message(self, update, text: str, amount: int = 0):
        if amount > 0:
            number_text = large_int_to_readable(amount * 1000)
            text = f"{text}\n\n(Coin was minted {number_text} times)"

        language = self.user_prefs[update.message.from_user.id].get(
            "language", "english"
        )
        if language == "english":
            update.message.reply_text(text)
        else:
            translate_llm = LLM(
                model="Open-Orca/Mistral-7B-OpenOrca",
                token=self.anyscale_token,
                task_prompt=(
                    f"You are a translation chatbot. Translate the following into {language}, use colloquial language like in a personal chat"
                ),
                temperature=0.0,
            )

            text = translate_llm(text)
            update.message.reply_text(text)

        log_to_csv(update.message.text, text)

    def handle_text_message(self, update, context):

        if random() < 0.001:
            output = self.joke_llm(update.message.text)
            self.return_message(update, output)
            return
        done = self.collecting_language(update, context)
        if not done:
            self.search_coin_in_db(update, context)

    def search_coin_in_db(self, update, context):
        """Search for a coin in the database when a message is received."""
        missing_hints = ["feature", "missing", "provided", "not"]
        try:
            user_id = update.message.from_user.id
            # Check if the user's language preference is set
            if user_id not in self.user_prefs:
                # Ask for the user's language preference
                update.message.reply_text("Which language do you want me to speak?")
                return

            # Parse the message
            message = update.message.text
            logger.debug(f"Received: {message}")

            if string_to_bool(self.ommitted_country_llm(message)):
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
            c = get_feature_value(output, "Country")
            value = get_feature_value(output, "Value").lower()
            value = value.replace("€", " euro").replace("  ", " ")
            year = int(get_feature_value(output, "Year"))
            country = self.to_english_llm(c)
            country = country.capitalize().strip().lower()
            print("Feature extraction LLM says", output)
            print("Features for lookup", country, year, value, source)

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
                response = f"🤯 The coin {match} should not exist. If you indeed have it, it's a SUPER rare find!"
                amount = 0
            elif coin_status == "missing":
                response = (
                    f"🚀🎉 Hooray! The coin {match} is not yet in the collection 🤩"
                )
                amount = coin_df["Amount"].values[0]
            elif coin_status == "collected":
                response = f"😢 Bad news! The coin {match} was already collected 😢"
                amount = coin_df["Amount"].values[0]
            else:
                response = "❓Coin not found."

            res = response.split("\n")[0]
            print(f"Returns: {res}\n")
            self.return_message(update, response, amount=amount)

        except Exception as e:
            response = f"An error occurred: {e}"
            self.return_message(update, response)

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
            temperature=0.0,
        )
        self.to_english_llm = LLM(
            model="Open-Orca/Mistral-7B-OpenOrca",
            token=self.anyscale_token,
            task_prompt=(
                "Give me the ENGLISH name of this country. Be concise, only one word."
            ),
            temperature=0.0,
        )
        self.ommitted_country_llm = LLM(
            model="Open-Orca/Mistral-7B-OpenOrca",
            token=self.anyscale_token,
            task_prompt=(
                "Does this string contain the value of a coin AND a year AND a single character? Reply with a single word, either `True` or `False`."
            ),
            temperature=0.0,
        )
