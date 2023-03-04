from pathlib import Path
from pyemvue import PyEmVue
from pyemvue.enums import Scale, Unit
from pyemvue.device import VueDevice, VueUsageDevice
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

            # Print usages for device and all channels
            print("Usages:")

            usage_amps: dict[str, VueUsageDevice] = vue.get_device_list_usage(deviceGids=[device.device_gid], unit=Unit.AMPHOURS.value, scale=Scale.SECOND.value, instant=None)
            usage_watts: dict[str, VueUsageDevice] = vue.get_device_list_usage(deviceGids=[device.device_gid], unit=Unit.KWH.value, scale=Scale.SECOND.value, instant=None)

            MULTIPLIER = 60 * 60 # Convert from -hours to instantaneous usage

            # Aggregate data for each channel into a single dict
            channels: dict[str, dict[str, float]] = {}

            for _device_gid, device in usage_amps.items():
                for _channel_gid, channel in device.channels.items():
                    amps = channel.usage * MULTIPLIER
                    watts = usage_watts[device.device_gid].channels[channel.channel_num].usage * MULTIPLIER * 1000 if channel.channel_num in usage_watts[device.device_gid].channels else None
                    volts = watts / amps if amps > 0 and watts else None

                    channels[channel.channel_num] = {
                        "amps": amps,
                        "watts": watts,
                        "volts": volts
                    }


            # Print usages
            for channel_num, channel in channels.items():
                
                try:
                    chan_num_int = int(channel_num)
                    friendly_name = config_value(f"vue.devices.{str(device.device_gid)}.channels.{chan_num_int - 1}")
                except:
                    friendly_name = None
                
                print(f"Channel {channel_num}{f' ({friendly_name})' if friendly_name else ''}: ", end="")

                # Amps always exists
                print(f"{channel['amps']:.2f}A", end="")

                # Watts and volts are optional
                if channel["watts"] is not None:
                    print(f" {channel['watts']:.2f}W", end="")
                if channel["volts"] is not None:
                    print(f" {channel['volts']:.2f}V", end="")

                print()
            
            print() 

                


def config_value(namespaced_key: str) -> str:
    """Returns a namespaced config value or the default value if it doesn't exist."""

    # Split the key into a list of keys
    keys = namespaced_key.split('.')

    # Interate through the keys to get the value
    value = config

    try:
        for key in keys:
            if isinstance(value, list):
                key = int(key)
            
            value = value[key]
    except:
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
