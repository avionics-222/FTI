
import time
from gpiozero import DigitalInputDevice, DigitalOutputDevice
import statistics

# HX711 GPIO pins (adjust as needed for your wiring)
DT_PIN = 5  # Data pin
SCK_PIN = 6  # Clock pin

# Constants for strain calculation
GAUGE_FACTOR = 2.0  # Typical value for strain gauges, adjust based on your sensor specs
BRIDGE_VOLTAGE = 5.0  # Voltage supplied to the Wheatstone bridge
STRAIN_GAUGE_RESISTANCE = 350.0  # Ohms

class HX711_gpiozero:
    def __init__(self, dout_pin, pd_sck_pin, gain=128):
        # Initialize pins using gpiozero
        self.PD_SCK = DigitalOutputDevice(pd_sck_pin)
        self.DOUT = DigitalInputDevice(dout_pin)
        
        self.GAIN = 0
        self.set_gain(gain)
        
        # Reference unit for scale
        self.reference_unit = 1
        self.offset = 0
        
        self.reset()
        
    def set_gain(self, gain):
        if gain == 128:
            self.GAIN = 1
        elif gain == 64:
            self.GAIN = 3
        elif gain == 32:
            self.GAIN = 2
        else:
            # Default to 128
            self.GAIN = 1
            
        self.PD_SCK.off()
        self.read()
    
    def is_ready(self):
        return not self.DOUT.value
    
    def reset(self):
        # Reset the chip by setting SCK high for at least 60 microseconds
        self.PD_SCK.on()
        time.sleep(0.0001)  # 100 microseconds
        self.PD_SCK.off()
        time.sleep(0.0001)  # 100 microseconds
    
    def read(self):
        # Wait for the chip to be ready
        timeout = 0
        while not self.is_ready():
            timeout += 1
            if timeout > 40:
                print("HX711 not responding. Check wiring.")
                return 0
            time.sleep(0.01)
            
        # Read the 24-bit value
        count = 0
        for i in range(24):
            self.PD_SCK.on()
            count = count << 1
            self.PD_SCK.off()
            if self.DOUT.value:
                count += 1
                
        # Set the channel and gain for the next reading
        for i in range(self.GAIN):
            self.PD_SCK.on()
            self.PD_SCK.off()
                
        # Handle 2's complement for negative values
        if count & 0x800000:
            count = count - 0x1000000
            
        return count
    
    def read_average(self, times=5):
        readings = []
        for i in range(times):
            readings.append(self.read())
            time.sleep(0.01)  # Small delay between readings
        
        # Filter out any outliers (optional)
        if len(readings) > 2:
            readings = self._filter_outliers(readings)
            
        if not readings:
            return 0
            
        return sum(readings) / len(readings)
    
    def _filter_outliers(self, readings, z_threshold=2.0):
        """Remove outliers using Z-score method"""
        if len(readings) <= 3:
            return readings
            
        mean = statistics.mean(readings)
        stdev = statistics.stdev(readings)
        
        if stdev == 0:
            return readings
            
        filtered = [r for r in readings if abs((r - mean) / stdev) < z_threshold]
        return filtered if filtered else readings
    
    def set_offset(self, offset):
        self.offset = offset
        
    def tare(self, times=10):
        # Set the offset to the average reading
        print("Taring... keep the sensor stable")
        sum_reading = self.read_average(times)
        self.set_offset(sum_reading)
        print(f"Tare complete. Offset: {sum_reading}")
        return sum_reading
    
    def set_reference_unit(self, reference_unit):
        self.reference_unit = reference_unit
        
    def get_weight(self, times=5):
        value = self.read_average(times) - self.offset
        return value / self.reference_unit
    
    def close(self):
        self.PD_SCK.close()
        self.DOUT.close()

class StrainGaugeHX711:
    def __init__(self, dt_pin, sck_pin, calibration_factor=1):
        print(f"Initializing HX711 with DT pin: {dt_pin}, SCK pin: {sck_pin}")
        self.hx = HX711_gpiozero(dt_pin, sck_pin)
        self.hx.set_reference_unit(calibration_factor)
        print("Starting tare...")
        self.hx.tare()
        print("Tare done. Ready for strain measurements.")
        
    def set_calibration_factor(self, calibration_factor):
        self.hx.set_reference_unit(calibration_factor)
        
    def tare(self):
        self.hx.tare()
        
    def read_raw_data(self):
        return self.hx.get_weight(1)
    
    def read_strain(self):
        # Read raw value from HX711
        raw_value = self.read_raw_data()
        
        # Convert raw value to strain
        # This is a simplified calculation - you may need to adjust based on your setup
        strain = raw_value / (BRIDGE_VOLTAGE * GAUGE_FACTOR)
        
        return strain
    
    def read_micro_strain(self):
        # Return strain in micro-strain units (μɛ)
        return self.read_strain() * 1_000_000
    
    def cleanup(self):
        self.hx.close()
        print("GPIO resources released")

# Main execution
if __name__ == "__main__":
    try:
        print("Starting strain gauge application")
        print(f"Using DT_PIN={DT_PIN}, SCK_PIN={SCK_PIN}")
        
        # Initialize the strain gauge with HX711
        strain_gauge = StrainGaugeHX711(DT_PIN, SCK_PIN)
        
        # Optional: Calibration process
        print("Place a known reference specimen for calibration, then press Enter")
        input()
        raw_reading = strain_gauge.read_raw_data()
        print(f"Raw reading: {raw_reading}")
        
        print("Starting strain measurement. Press CTRL+C to exit.")
        
        while True:
            # Read strain
            raw_value = strain_gauge.read_raw_data()
            micro_strain = strain_gauge.read_micro_strain()
            
            # Print results
            print(f"Raw value: {raw_value:.2f}, Bending Strain: {micro_strain:.2f} μɛ (micro-strain)")
            
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        print("Measurement stopped by user")
    except Exception as e:
        print(f"Error: {e}")
