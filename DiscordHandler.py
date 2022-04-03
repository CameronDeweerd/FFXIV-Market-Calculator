import json
import os

import requests
from LogHandler import LogHandler


class DiscordHandler:
    def __init__(self, discord_config, logging_config):
        self.ffxiv_logger = LogHandler.get_logger(__name__, logging_config)
        self.discord_id = os.getenv('DISCORDID')
        self.discord_token = os.getenv('DISCORDTOKEN')
        if (
            not self.discord_id or
            not self.discord_token or
            not isinstance(self.discord_id, str) or
            not isinstance(self.discord_token, str)
        ):
            self.ffxiv_logger.error("Discord Webhook ID or Token missing")
            raise ValueError
        self.webhook_base = f"https://discord.com/api/webhooks/{self.discord_id}/{self.discord_token}/"

    def discord_message_create(self, data):
        self.ffxiv_logger.info("Creating new discord message")
        requests.post(self.webhook_base, json.dumps({"content": data}),
                      headers={'content-type': 'application/json'})
        self.ffxiv_logger("Discord message sent")

    def discord_message_update(self, message_id, data):
        self.ffxiv_logger.info("Updating discord message")
        webhook_path = f"{self.webhook_base}messages/"
        requests.patch(f"{webhook_path}{str(message_id)}", json.dumps({"content": data}),
                       headers={'content-type': 'application/json'})
        self.ffxiv_logger.info("Discord message updated")
