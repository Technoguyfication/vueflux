from pathlib import Path
from pyemvue import PyEmVue
import json
import logging

vue: PyEmVue
config: dict[str] = None


def main(debug: bool, show_devices: bool, token_file: str = None, **kwargs):

    # Set up logging
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)

    # Load config
    with open(kwargs["config"]) as f:
        global config
        config = json.load(f)

    global vue
    vue = PyEmVue()

    # Login with username/password
    username = config_value("vue.username")
    password = config_value("vue.password")
    vue.login(username=username, password=password)

    if show_devices:
        devices = vue.get_devices()
        for device in devices:
            vue.populate_device_properties(device)
            
            print(f"Device: {device.device_name}")
            print(f"ID: {device.device_gid}")
            print("Channels:")
            for channel in device.channels:
                print(f"Channel {channel.channel_num} - Multiplier: {channel.channel_multiplier}")


def config_value(namespaced_key: str) -> str:
    """Returns a namespaced config value or the default value if it doesn't exist."""

    # Split the key into a list of keys
    keys = namespaced_key.split('.')

    # Interate through the keys to get the value
    value = config

    try:
        for key in keys:
            value = value[key]
    except KeyError:
        return None
    return str(value)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="An InfluxDB data source for Emporia Vue")

    parser.add_argument("--config", type=str,
                        default="config.json", help="Path to config file")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    parser.add_argument("--token-file", type=str, default=None,
                        help="Optional file to store the token used to authenticate with the Emporia Vue API")
    # parser.add_argument("--show-token", action="store_true",
    #                     help="Show the token used to authenticate with the Emporia Vue API")
    parser.add_argument("--show-devices", action="store_true",
                        help="Show the devices associated with the Emporia Vue account")

    args = parser.parse_args()

    exit_code = main(**vars(args))
    exit(exit_code if exit_code is not None else 0)
