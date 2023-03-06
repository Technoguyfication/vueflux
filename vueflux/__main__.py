from datetime import datetime, timedelta, timezone
import signal
import time
from pyemvue import PyEmVue
from pyemvue.enums import Scale, Unit
from pyemvue.device import VueDevice, VueUsageDevice
from influxdb_client import InfluxDBClient, Point, WriteOptions
import json
import logging
import numpy as np

vue: PyEmVue
config: dict[str] = None
running = True

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

    # Show devices and exit on --show-devices
    if show_devices:
        show_devices()
        return
    
    all_devices = vue.get_devices()

    # Create list of device IDs to query metrics from
    device_ids = list(map(lambda device: device.device_gid, all_devices))

    # Remove duplicate device IDs
    # Sometimes the API returns the same device multiple times
    device_ids = list(dict.fromkeys(device_ids))

    # Filter device IDs by the list in the config
    if config_value("vue.devices") is not None:
        device_ids = list(filter(lambda device: str(device) in config_value("vue.devices").keys(), device_ids))

    # Get devices from device IDs
    devices = list(filter(lambda device: device.device_gid in device_ids, all_devices))
    
    # Get global influx tags
    global_tags = config_value("influx.tags")

    # Get device-specific influx tags
    device_tags = {}
    config_devices = config_value("vue.devices")
    if config_devices is not None:
        for device in device_ids:
            if str(device) in config_devices.keys():
                if "tags" in config_devices[str(device)].keys():
                    device_tags = config_devices[str(device)]["tags"]
                    if device_tags is not None:
                        device_tags[device] = device_tags

    print(f"Collecting metrics for devices: {device_ids}")

    # Set up InfluxDB client
    with InfluxDBClient(
        url=config_value("influx.url"),
        token=config_value("influx.token"),
        org=config_value("influx.org-id")
    ) as _influx_client:
        
        # Set up some default options for the client
        client_options = {
            "flush_interval": 10_000,
            "jitter_interval": 1_000
        }

        # Update the client options with any options from the config
        config_options = config_value("influx.client-options")
        if config_options is not None:
            client_options.update(config_options)

        with _influx_client.write_api(WriteOptions(**client_options)) as _write_client:

            interval = config_value("metrics.interval")
            resolution = config_value("metrics.resolution")

            if interval % resolution != 0:
                raise ValueError("Poll interval must be a multiple of resolution")

            # Determine scale based on data resolution
            # If resolution is 60 seconds or more, use MINUTE scale
            # Otherwise, use SECOND scale
            scale = Scale.MINUTE if resolution >= 60 else Scale.SECOND

            # Keep track of the time window we need to query for data
            # Default time window is now-(poll interval)
            start_time = datetime.now(tz=timezone.utc) - timedelta(seconds=interval)
            end_time = datetime.now(tz=timezone.utc)

            # Gather metrics continuously
            while running:

                logging.info(f"Collecting metrics for time window {start_time} - {end_time}")

                for device in devices:

                    device_custom_id = config_value(f"vue.devices.{device.device_gid}.custom-id")
                    logging.debug(f"Collecting metrics for device {device_custom_id if device_custom_id else device.device_gid}")

                    # Keep track of channels that have already been queried
                    channels_queried = []

                    for channel in device.channels:

                        # Skip channels that have already been queried.
                        # Sometimes multiple devices expose the same channels and this is the only sane way to handle it.
                        if channel.channel_num in channels_queried:
                            continue
                        else:
                            channels_queried.append(channel.channel_num)

                        # Try to get a friendly name for the channel
                        if channel.channel_num == "1,2,3":
                            channel_friendly_name = "All"
                        else:
                            try:
                                channel_id_int = int(channel.channel_num)
                                channel_friendly_name = config_value(f"vue.devices.{device.device_gid}.channels.{channel_id_int - 1}")
                            except ValueError:
                                channel_friendly_name = None

                        logging.debug(f"Collecting metrics for channel {channel_friendly_name if channel_friendly_name else channel.channel_num}")

                        # Query Kilowatts and Amps
                        units = {
                            "watts": Unit.KWH.value,
                            "amps": Unit.AMPHOURS.value
                        }

                        for unit, unit_value in units.items():
                            usages, first_timestamp = vue.get_chart_usage(
                                channel=channel,
                                start=start_time,
                                end=end_time,
                                scale=scale.value,
                                unit=unit_value
                            )

                            logging.debug(f"Got {len(usages)} {unit} usage data points for channel {channel.channel_num}")

                            # Turn usage data into a numpy array
                            usages = np.array(usages)

                            # Determine the step size for the data
                            step = resolution // 60 if scale == Scale.MINUTE else resolution

                            logging.debug(f"Using step size {step}")

                            # Split usage data into chunks based on step size
                            for i in range(0, len(usages), step):

                                # Get the usage data for the current chunk
                                chunk = usages[i:i+resolution]

                                # Remove Nones from the chunk
                                chunk = chunk[chunk != None]

                                # Skip chunk if it's empty
                                if len(chunk) == 0:
                                    continue

                                # Calculate the average usage for the chunk
                                average = np.average(chunk)

                                # Calculate the timestamp for the chunk
                                timestamp = first_timestamp + timedelta(minutes=i) if scale == Scale.MINUTE else first_timestamp + timedelta(seconds=i)

                                # Skip chunk if timestamp is out of range
                                if timestamp > end_time:
                                    continue

                                # Convert kWh to Wh
                                if unit == "watts":
                                    average = average * 1000
                                
                                # Convert usage to rate
                                if scale == Scale.MINUTE:
                                    average *= 60   # Convert from kWh/min to kW
                                else:
                                    average *= 60 * 60  # Convert from kWh/sec to kW

                                # Create the InfluxDB point
                                point = Point("energy_usage") \
                                    .tag("device", device_custom_id if device_custom_id else device.device_gid) \
                                    .tag("channel", channel.channel_num) \
                                    .field(unit, average) \
                                    .time(timestamp)

                                # Add global tags to the point
                                if global_tags is not None:
                                    for key, value in global_tags.items():
                                        point = point.tag(key, value)

                                # Add device-specific tags to the point
                                if device.device_gid in device_tags.keys():
                                    for key, value in device_tags[device.device_gid].items():
                                        point = point.tag(key, value)

                                # Add channel-specific tags to the point
                                if channel_friendly_name is not None:
                                    point = point.tag("channel_name", channel_friendly_name)

                                # Write the point to InfluxDB
                                _write_client.write(bucket=config_value("influx.bucket"), record=point)
                
                # Update the start and end times for the next query
                start_time = end_time
                end_time = start_time + timedelta(seconds=interval)

                # Sleep until 1 second after the next start time
                sleep_time = (start_time + timedelta(seconds=1) - datetime.now(tz=timezone.utc)).total_seconds()
                print(f"Sleeping for {sleep_time: 0.3f} seconds...")
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
def show_devices():
    print("Listing devices...\n")

    _devices = vue.get_devices()
    for device in _devices:
        vue.populate_device_properties(device)
        
        print(f"Device: {device.device_name}")
        print(f"ID: {device.device_gid}")
        print("Channels:")
        for channel in device.channels:
            print(f"Channel {channel.channel_num} - Multiplier: {channel.channel_multiplier}")
        
        print()

def config_value(namespaced_key: str):
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
    return value

def exit_handler(sig, frame):
    global running
    running = False

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
                        help="Show the devices associated with the Emporia Vue account and exit")

    # Handle termination signals
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    args = parser.parse_args()

    exit_code = main(**vars(args))
    exit(exit_code if exit_code is not None else 0)
