from pymavlink.dialects.v20 import common as mavlink2
from pymavlink import mavutil
import time

# Open UART to your SiK radio
master = mavutil.mavlink_connection('/dev/ttyUSB0', baud=57600)

# System ID, Component ID
sysid = 1
compid = 1

# Send heartbeats in a loop
while True:
    master.mav.heartbeat_send(
        mavlink2.MAV_TYPE_GENERIC,
        mavlink2.MAV_AUTOPILOT_GENERIC,
        0, 0, 0
    )
    time.sleep(1)
