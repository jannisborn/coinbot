import json
import os

from telegram.ext import CommandHandler, Filters, MessageHandler, Updater

with open(os.path.join(os.path.dirname(__file__), "secrets.json"), "r") as f:
    secrets = json.load(f)
    TOKEN = secrets["telegram-token"]


def start(update, context):
    """Send a message when the command /start is issued."""
    update.message.reply_text("Hi!")


def echo(update, context):
    """Echo the user message."""
    update.message.reply_text(update.message.text)


def hello_world(update, context):
    """Reply with 'Hello World' to every message."""
    update.message.reply_text("Hello World")


def main():
    """Start the bot."""
    # Replace 'YOUR_TOKEN' with the token given by BotFather
    updater = Updater(TOKEN, use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # On different commands - answer in Telegram
    dp.add_handler(CommandHandler("start", start))

    # On noncommand i.e message - echo the message on Telegram
    dp.add_handler(MessageHandler(Filters.text, hello_world))

    # Start the Bot
    print("Starting bot")
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
