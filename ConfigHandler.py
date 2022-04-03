import ast
import configparser
import os
import logging
import pathlib


class ConfigHandler:
    parser = configparser.ConfigParser()

    def __init__(self, configfile, global_db):
        self.parser = configparser.ConfigParser()
        self.configfile = configfile
        self.global_db = global_db
        self.log_handler = logging
        self.ffxiv_logger = self.log_handler.getLogger(__name__)
        self.config = {}
        if not os.path.exists(pathlib.Path(__file__).parent / self.configfile):
            self.ffxiv_logger.info("Config File does not exist, creating one with default values")
            self.config_create()

    def config_create(self):
        self.parser.add_section('MAIN')
        self.parser['MAIN']['MarketboardType'] = 'World'
        self.parser['MAIN']['Datacentre'] = 'Crystal'
        self.parser['MAIN']['World'] = 'Zalera'
        self.parser['MAIN']['ResultQuantity'] = '50'
        self.parser['MAIN']['UpdateQuantity'] = '0'
        self.parser['MAIN']['MinAvgSalesPerDay'] = '20'

        self.parser.add_section('LOGGING')
        self.parser['LOGGING']['LogEnable'] = 'True'
        self.parser['LOGGING']['LogLevel'] = 'INFO'
        self.parser['LOGGING']['LogMode'] = 'WRITE'
        self.parser['LOGGING']['LogFile'] = 'ffxiv_market_calculator.log'

        self.parser.add_section('DISCORD')
        self.parser['DISCORD']['DiscordEnable'] = 'False'
        self.parser['DISCORD']['MessageIds'] = '[123456789123456789,123456789123456789]'

        with open(self.configfile, 'w') as configfile:
            self.parser.write(configfile)

    def parse_main_config(self):
        self.parser.read(self.configfile)
        try:
            self.config = {
                "marketboard_type": self.parser["MAIN"].get('MarketboardType', 'World').capitalize(),
                "datacentre": self.parser["MAIN"].get('Datacentre', 'Crystal').capitalize(),
                "world": self.parser["MAIN"].get('World', 'Zalera').capitalize(),
                "result_quantity": self.parser["MAIN"].getint('ResultQuantity', 50),
                "update_quantity": self.parser["MAIN"].getint('UpdateQuantity', 0),
                "min_avg_sales_per_day": self.parser["MAIN"].getint('MinAvgSalesPerDay', 20)
            }
        except Exception as err:
            self.ffxiv_logger.error(f"MAIN Config was invalid, setting back to defaults: {err}")

            self.parser["MAIN"]['MarketboardType'] = 'World'.capitalize()
            self.parser["MAIN"]['Datacentre'] = 'Crystal'.capitalize()
            self.parser["MAIN"]['World'] = 'Zalera'.capitalize()
            self.parser["MAIN"]['ResultQuantity'] = '50'
            self.parser["MAIN"]['UpdateQuantity'] = '0'
            self.parser["MAIN"]['MinAvgSalesPerDay'] = '20'
            with open(self.configfile, 'w') as configfile:
                self.parser.write(configfile)

            self.config = {
                "marketboard_type": 'World',
                "datacentre": 'Crystal',
                "world": 'Zalera',
                "result_quantity": 50,
                "update_quantity": 0,
                "min_avg_sales_per_day": 20
            }
        self.main_validation()
        self.ffxiv_logger.info("Main Config Loaded")
        return self.config

    def parse_logging_config(self):
        self.ffxiv_logger.info("Loading Logging Config")
        self.parser.read(self.configfile)
        try:
            self.config = {
                "log_enable": self.parser["LOGGING"].getboolean('LogEnable', False),
                "log_level": self.parser['LOGGING'].get('LogLevel', 'INFO'),
                "log_mode": self.parser['LOGGING'].get('LogMode', 'Write'),
                "log_file": self.parser['LOGGING'].get('LogFile', 'ffxiv_market_calculator.log'),
            }
        except Exception as err:
            self.ffxiv_logger.error(f"LOGGING Config was invalid, setting back to defaults: {err}")
            self.parser['LOGGING']['LogEnable'] = 'True'
            self.parser['LOGGING']['LogLevel'] = 'INFO'
            self.parser['LOGGING']['LogMode'] = 'WRITE'
            self.parser['LOGGING']['LogFile'] = 'ffxiv_market_calculator.log'
            with open(self.configfile, 'w') as configfile:
                self.parser.write(configfile)

            self.config = {
                "log_enable": False,
                "log_level": 'INFO',
                "log_mode": 'Write',
                "log_file": 'ffxiv_market_calculator.log'
            }
        self.logging_validation()
        return self.config

    def parse_discord_config(self):
        self.ffxiv_logger.info("Loading Discord Config")
        self.parser.read(self.configfile)
        try:
            self.config = {
                "discord_enable": self.parser["DISCORD"].getboolean('DiscordEnable', False),
                "message_ids": ast.literal_eval(self.parser['DISCORD'].get('MessageIds'))
            }
        except Exception as err:
            self.ffxiv_logger.error(f"DISCORD Config was invalid, setting back to defaults: {err}")
            self.parser["DISCORD"]['DiscordEnable'] = 'False'
            self.parser['DISCORD']['MessageIds'] = '[123456789123456789, 123456789123456789]'
            with open(self.configfile, 'w') as configfile:
                self.parser.write(configfile)

            self.config = {
                "discord_enable": False,
                "message_ids": []
            }
        self.discord_validation()
        self.ffxiv_logger.info("Loaded Discord Config")
        return self.config

    def main_validation(self):
        self.ffxiv_logger.info("Performing Main Config Validation")
        datacentre_data = self.global_db.return_query(f'SELECT name FROM datacentre')
        world_data = self.global_db.return_query(f'SELECT name FROM world')
        valid_datacentres = valid_worlds = [] = []
        for dc in datacentre_data:
            valid_datacentres.append(dc[0])
        for world in world_data:
            valid_worlds.append(world[0])

        if (
                self.config["marketboard_type"] not in ["World", "Datacentre", "Datacenter"] or
                self.config["datacentre"] not in valid_datacentres or
                self.config["world"] not in valid_worlds or
                not isinstance(self.config["result_quantity"], int) or self.config["result_quantity"] == 0 or
                not isinstance(self.config["update_quantity"], int) or
                not isinstance(self.config["min_avg_sales_per_day"], int) or
                self.config["min_avg_sales_per_day"] == 0
        ):
            self.ffxiv_logger.error("Main Config Validation FAILED")
            raise ValueError
        self.ffxiv_logger.info("Main Config Validation Complete")

    def logging_validation(self):
        self.ffxiv_logger.info("Performing Logging Config Validation")
        if not isinstance(self.config["log_enable"], bool):
            raise ValueError
        elif self.config["log_enable"] and (
                not isinstance(self.config["log_level"], str) or
                self.config["log_level"] not in ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"] or
                not isinstance(self.config["log_mode"], str) or
                self.config["log_mode"] not in ["WRITE", "APPEND"] or
                not isinstance(self.config["log_file"], str)
        ):
            self.ffxiv_logger.error("Logging Config Validation FAILED")
            raise ValueError
        self.ffxiv_logger.info("Logging Config Validation Complete")

    def discord_validation(self):
        self.ffxiv_logger.info("Performing Discord Config Validation")
        if not isinstance(self.config["discord_enable"], bool):
            self.ffxiv_logger.error("Discord Config Validation FAILED")
            raise ValueError
        self.ffxiv_logger.info("Discord Config Validation Complete")

    def get_config_parser(self):
        return self.parser.read(self.configfile), self.configfile
