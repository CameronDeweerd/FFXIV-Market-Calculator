"""
Module for handling all message building functions for FFXIV-Market-Calculator
"""
from datetime import datetime
import pandas as pd

from log_handler import LogHandler


class MessageBuilder:  # pylint: disable=too-few-public-methods
    """
    Class for handling the script message building.

    Attributes:
    -------
    ffxiv_logger : Logger object
        Used for logging functions
    update_time : str
        Data update time

    Methods:
    -------
    message_builder(location, sales_data, no_craft):
        Builds a message into the appropriate format
    """
    def __init__(self, logging_config):
        self.ffxiv_logger = LogHandler.get_logger(__name__, logging_config)
        self.update_time = datetime.now().strftime('%d/%m/%Y %H:%M')

    @staticmethod
    def message_builder(location, sales_data, no_craft):
        """
        Takes the data and builds message/s for console or Discord.

        Parameters:
            location : str
                World/DC the sales data is for
            sales_data : dict
                Sales data for parsing into the message
            no_craft : bool
                Whether to also display most profitable without crafting costs
        """
        update_time = datetime.now().strftime('%d/%m/%Y %H:%M')
        if no_craft:
            message_header = (
                f"*(No Craft Cost)* **Data from {location} > 2 avg daily sales @ "
                f"{update_time}**\n```"
            )
        else:
            message_header = (
                f"**Data from {location} > 2 avg daily sales @ "
                f"{update_time}**\n```"
            )
        message_footer = "```"
        to_display = ["Name", "Profit", "Avg-Sales", "Avg-Cost", "Avg-Cft-Cost"]
        frame = pd.DataFrame(sales_data)
        frame.columns = to_display
        message = message_header + frame.to_string(index=False).replace('"', '') + message_footer
        return message
