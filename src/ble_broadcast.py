#!/usr/bin/env python3

import asyncio
import struct
import logging
import json
import requests
from bleak import BleakClient, BleakError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ARDUINO_BLE_ADDRESS = ""
CHAR_UUID = "00001a19-0000-1000-8000-00805f9b34fb"

OPENHAB_URL = "http://localhost:8080/rest/items/batch_update/state"
HEADERS = {"Content-Type": "text/plain"}


def send_batch_to_openhab(data: dict):
    try:
        batch_payload = json.dumps(data)
        resp = requests.put(OPENHAB_URL, headers=HEADERS, data=batch_payload)
        resp.raise_for_status()
        logger.info(f"[openHAB] Batch update successful: {data}")
    except requests.RequestException as e:
        logger.error(f"[openHAB] Failed to batch update: {e}")


def notification_handler(sender, data: bytearray):
    if len(data) != 32:
        logger.error(f"Unexpected data length: {len(data)} bytes. Skipping.")
        return

    pm1, pm25, pm4, pm10, humidity, temperature, voc, nox = struct.unpack(
        '<8f', data)

    logger.info(f"Received Data: PM1={pm1}, PM2.5={pm25}, PM4={pm4}, PM10={pm10}, "
                f"Humidity={humidity}, Temp={temperature}, VOC={voc}, NOx={nox}")

    openhab_data = {
        "pm1": pm1,
        "pm25": pm25,
        "pm4": pm4,
        "pm10": pm10,
        "humidity": humidity,
        "temperature": temperature,
        "voc": voc,
        "nox": nox
    }

    send_batch_to_openhab(openhab_data)


async def main():
    while True:
        try:
            logger.info(f"Connecting to BLE device at {ARDUINO_BLE_ADDRESS}")
            async with BleakClient(ARDUINO_BLE_ADDRESS) as client:
                logger.info("Connected!")

                await client.start_notify(CHAR_UUID, notification_handler)
                logger.info("Subscribed to characteristic notifications")

                while True:
                    await asyncio.sleep(1)

        except BleakError as e:
            logger.error(
                f"BLE connection error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except KeyboardInterrupt:
            logger.info("Script terminated by user.")
            break

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Script terminated by user.")
