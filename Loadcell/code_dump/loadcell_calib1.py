# ----------------------------
# Loadcell Calibration CODE
# ----------------------------
import time
import statistics
from gpiozero import DigitalInputDevice, DigitalOutputDevice
import sys

class HX711:
    def __init__(self, dout_pin, sck_pin, calibration_factor=25.4):
        self.dout = DigitalInputDevice(dout_pin)
        self.sck = DigitalOutputDevice(sck_pin, initial_value=False)
        self.offset_3v3_100 = 0.6625 / 6.6
        self.calibration_factor = calibration_factor
        self.offset = 0
        self.last_valid_reading = 0
        time.sleep(0.1)

    def read(self):
        timeout_start = time.time()
        while self.dout.value == 1:
            if time.time() - timeout_start > 0.01:
                return None

        data = 0
        for _ in range(24):
            self.sck.on()
            self.sck.off()
            data = (data << 1) | self.dout.value

        self.sck.on()
        self.sck.off()

        if data & 0x800000:
            data -= 1 << 24
        return None if data == 0 or data == -1 else data

    def read_average(self, num_readings=10):
        readings = []
        while len(readings) < num_readings:
            reading = self.read()
            if reading is not None:
                readings.append(reading)
            time.sleep(0.01)
        return statistics.median(readings)

    def zero(self, num_readings=10):
        self.offset = self.read_average(num_readings)
        return self.offset

    def get_weight(self, num_readings=6):
        reading = self.read_average(num_readings)
        raw_value = reading - self.offset
        out_volt = raw_value * 20 / 8388607
        weight = out_volt * 120 * 1000 * self.offset_3v3_100  # 6.6 = 3.3v * 2mV
        return weight / 1000, raw_value  # weight in kg, raw ADC value


# ----------------------------
# Run this module standalone
# ----------------------------
if __name__ == "__main__":
    # Hardcoded GPIO pins (BCM numbering)
    DOUT_PIN = 20     # e.g., GPIO5
    SCK_PIN = 21      # e.g., GPIO6

    hx = HX711(dout_pin=DOUT_PIN, sck_pin=SCK_PIN)

    print("Taring... Please remove any weight from the scale.")
    time.sleep(2)
    
    offset = hx.zero()
    print(f"Scale zeroed. Offset value: {offset}")
    print("Starting weight readings...\nPress Ctrl+C to stop.\n")

    try:
        while True:
            weight, raw = hx.get_weight()
            print(f"Weight: {weight:.3f} kg | Raw: {raw}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nMeasurement stopped by user.")
        sys.exit(0)
