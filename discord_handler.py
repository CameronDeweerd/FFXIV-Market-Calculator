"""
Module for handling all discord api functions for FFXIV-Market-Calculator
"""
import json
import os

import requests
from log_handler import LogHandler


class DiscordHandler:
    """
    Class for handling the script discord api calls.

    Attributes:
    -------
    ffxiv_logger : Logger object
        Used for logging functions
    discord_id : str
        Store the Discord Webhook ID
    discord_token : str
        Store the Discord Webhook Token
    global_db : SqlManager object
        Global database to store shared values
    webhook_base : str
        Base url for the Discord Webhook calls

    Methods:
    -------
    discord_message_create(data):
        Creates a new Discord Message
    discord_message_update(message_id, data):
        Updates existing Discord Message/s
    """
    def __init__(self, logging_config):
        """
        Constructs all the necessary attributes for the DiscordHandler object.

        Parameters:
            logging_config : dict
                The config for logging
        """
        self.ffxiv_logger = LogHandler.get_logger(__name__, logging_config)
        self.discord_id = os.getenv('DISCORDID')
        self.discord_token = os.getenv('DISCORDTOKEN')
        self.ffxiv_logger.debug(f'{str(self.discord_token)} {str(self.discord_id)}')
        if not all([
            self.discord_id,
            self.discord_token,
            isinstance(self.discord_id, str),
            isinstance(self.discord_token, str)
        ]):
            self.ffxiv_logger.error("Discord Webhook ID or Token missing")
            raise TypeError
        self.webhook_base = f"https://discord.com/api/webhooks/" \
                            f"{self.discord_id}/{self.discord_token}"

    def discord_message_create(self, data):
        """
        Calls the Discord Webhook API to create a new message.

        Parameters:
            data : str
                Message data to be sent to the Discord Webhook
        """
        self.ffxiv_logger.info("Creating new discord message")
        response = requests.post(self.webhook_base, json.dumps({"content": data}),
                                 headers={'content-type': 'application/json'})
        self.ffxiv_logger.debug(f'{str(response.status_code)} {response.text} '
                                f'{response.request.url}')
        self.ffxiv_logger.info("Discord message sent")

    def discord_message_update(self, message_id, data):
        """
        Calls the Discord Webhook API to update existing message/s.

        Parameters:
            message_id : list[str]
                Message ID's to update
            data : str
                Message data to be sent to the Discord Webhook
        """
        self.ffxiv_logger.info("Updating discord message")
        webhook_path = f"{self.webhook_base}/messages/"
        response = requests.patch(f"{webhook_path}{str(message_id)}",
                                  json.dumps({"content": data}),
                                  headers={'content-type': 'application/json'})
        self.ffxiv_logger.debug(f'{str(response.status_code)} {response.text} '
                                f'{response.request.url}')
        self.ffxiv_logger.info("Discord message updated")

    def discord_queue_handler(self, message):
        """
        Processes the message queue through create or update.

        Parameters:
            message : tuple
                Message ID and message data
        """
        self.ffxiv_logger.debug(message)
        if message[0] == 0:
            self.discord_message_create(message[1])
        else:
            self.discord_message_update(message[0], message[1])
