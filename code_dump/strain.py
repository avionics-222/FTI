
import time
import numpy as np
from gpiozero import DigitalInputDevice, DigitalOutputDevice

class HX711:
    def __init__(self, dout, pd_sck, gain=32):
        self.dout = DigitalInputDevice(dout)
        self.pd_sck = DigitalOutputDevice(pd_sck)
        self.gain = gain
        self.offset = 0  # Offset correction
        self.set_gain()
        self.tare()

    def set_gain(self):
        gain_map = {128: 1, 64: 3, 32: 2}
        for _ in range(gain_map[self.gain]):
            self.pd_sck.on()
            self.pd_sck.off()

    def read_raw(self):
        while self.dout.value == 1:
            pass  # Wait for data ready

        count = 0
        for _ in range(24):
            self.pd_sck.on()
            count = (count << 1) | self.dout.value
            self.pd_sck.off()
        
        # Set gain for the next measurement
        for _ in range({128: 1, 64: 3, 32: 2}[self.gain]):
            self.pd_sck.on()
            self.pd_sck.off()

        if count & (1 << 23):  # If the 24-bit value is negative in two's complement
            count -= 1 << 24
        return count

    def read_average(self, samples=20):
        values = [self.read_raw() for _ in range(samples)]
        return np.mean(values)

    def tare(self, samples=20):
        """Set offset based on no-load condition"""
        self.offset = self.read_average(samples)
        print(f"Tare Offset: {self.offset}")

    def get_strain(self):
        raw_value = self.read_average()
        strain = raw_value - self.offset  # Adjust for tare offset
        return strain

# Define GPIO Pins
DOUT_PIN = 5
PD_SCK_PIN = 6

hx711 = HX711(DOUT_PIN, PD_SCK_PIN, gain=32)

while True:
    strain = hx711.get_strain()
    print(f"Strain Value: {strain:.6f}")
    time.sleep(0.1)

