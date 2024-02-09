'''
Load configuration parameters from configuration file.
'''

import configparser
import sys


class ConfigLoader:
    '''
    Initialize a ConfigLoader instance.

        Parameters:
            config_file (str): The path to the configuration file.
    '''

    def __init__(self, config_file:str) -> None:
        self.config_file = config_file
        self.config = None

    def load_config(self):
        '''
        Load the configuration from the specified file.
        If the configuration has not been loaded yet, this method reads and parses the configuration file.
        '''

        if self.config is None:
            try:
                self.config = configparser.ConfigParser()
                self.config.read(self.config_file)
            except FileNotFoundError:
                print(f"Config file '{self.config_file}' not found.")
                sys.exit(1)


    def get_config_section(self, section_name:str) -> dict:
        '''
        Retrieve a section from the loaded configuration.

        Parameters:
            section_name (str): The name of the configuration section.

        Returns:
            dict: A dictionary containing the key-value pairs from the specified section.
        '''

        self.load_config()
        if section_name in self.config:
            return dict(self.config[section_name])
        else:
            print(f"Section '{section_name}' not found in the config file.")
            return {}

    def get_config_value(self, section_name:str, key:str):
        '''
        Retrieve a value from a specific section in the loaded configuration.

        Parameters:
            section_name (str): The name of the configuration section.
            key (str): The key for the value to retrieve.

        Returns:
            str or None: The value associated with the specified key, or None if not found.
        '''

        self.load_config()
        if section_name in self.config and key in self.config[section_name]:
            return self.config[section_name][key]
        else:
            print(f"Key '{key}' in section '{section_name}' not found in the config file.")
            return None