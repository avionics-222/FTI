
import time
import statistics
from gpiozero import DigitalInputDevice, DigitalOutputDevice
import sys
#82243,  # Loadcell 0
#30683,    # Loadcell 1
#213752,    # Loadcell 2
#81425
global offset_list
class HX711:
    def __init__(self, dout_pin=5, sck_pin=6):
        """Initialize HX711 using gpiozero"""
        try:
            self.dout = DigitalInputDevice(dout_pin)
            self.sck = DigitalOutputDevice(sck_pin, initial_value=False)
        except Exception as e:
            print(f"Error initializing GPIO: {e}")
        

        self.offset_3v3_100 = 0.6625
        self.calibration_factor = 1000         #//10kg: 145  //50kg: 40.04
        self.offset = 30425 
        self.last_valid_reading = 0
        time.sleep(0.2)  # Reduced startup delay

    def read(self):
        """Optimized HX711 raw data reading with reduced delay"""
        timeout_start = time.time()
        while self.dout.value == 1:  # Wait for HX711 to be ready
            if time.time() - timeout_start > 0.01:  # 100ms timeout
                return None
        
        delay_time = 1e-9
        data = 0
        for _ in range(24):
            self.sck.on()
            #time.sleep(delay_time)
            self.sck.off()
            
            data = (data << 1) | self.dout.value  # Faster bit shifting
            #time.sleep(delay_time)

        self.sck.on()
        #time.sleep(delay_time)
        self.sck.off()  # 25th clock pulse
        #print(data)

        if data & 0x800000:
            data -= 1 << 24  # Convert 2's complement
            #time.sleep(0.01)
        #print(data)
        return None if data == 0 or data == -1 else data
        

    def read_average(self, num_readings=10, max_retries=10):
        """Optimized function to take multiple readings and return median-filtered average"""
        readings = []
        retries = 0

        #while len(readings) < num_readings and retries < max_retries:
        while len(readings) < num_readings:
            reading = self.read()
            
            if reading is not None:
                readings.append(reading)
            #else:
             #   retries += 1
            
            time.sleep(0.01)  # Reduced sleep time

        #if not readings:
            #return self.last_valid_reading if self.last_valid_reading else 0
        
        #return reading
        return statistics.median(readings)  # Faster noise filtering

    def zero(self, num_readings=20):
        """Optimized tare function using median filtering"""
        print("Zeroing scale...")
        self.offset = self.read_average(num_readings)
        
        print(f"Zero offset: {self.offset}")
        #offset_list.append(self.offset)
        
        return self.offset

    def get_weight(self, num_readings=10):
        """Get weight with optimized averaging"""
        reading = self.read_average(num_readings)
        raw_value = reading - self.offset#+1500
        
        out_volt = raw_value * 20/8388607
        weight = out_volt * 120*1000/6.6*self.offset_3v3_100     #6.6 = 3.3v * 2mV
        
        #print("---------",reading, raw_value, weight)
        return weight/1000, raw_value

    def reset_hx711(self):
        """Reset the HX711 chip by toggling the clock line"""
        self.sck.on()
        time.sleep(0.06)
        self.sck.off()
        time.sleep(0.01)

    def cleanup(self):
        """Cleanup resources (gpiozero handles this automatically)"""
        pass

def main(dout_pin,sck_pin, angle,offset_hard,calibration_check):
    """Main function to demonstrate optimized HX711 readings"""
    hx = None
    try:
        hx = HX711(dout_pin, sck_pin)
        print(f"Using fixed calibration factor: {hx.calibration_factor}")
        #hx.offset = offset_hard
        if calibration_check == 1:
            
            hx.zero(100)
            return
        else:
            hx.offset = offset_hard
        print("Scale ready! Starting weight measurements...")
        print("Press Ctrl+C to exit")

        failure_count = 0
        count_rep = 0
        last_weight = 0
        while True:
            
            
            try:
                weight, raw_value = hx.get_weight(num_readings=4)  # Reduced readings
                print(f"Raw value: {raw_value:.2f} | Weight: {weight:.2f} Kg")
                print(f"Angle {angle} | Weight: {weight:.2f} Kg")
                failure_count = 0  # Reset on success
                count_rep += 1 
                last_weight = weight
                if count_rep == 6:
                    return last_weight
                
            except Exception as e:
                print(f"Error: {e}")
                return last_weight
                """
                failure_count += 1
                if failure_count >= 3:  # Reduced reset threshold
                    print("Resetting HX711...")
                    hx.reset_hx711()
                    failure_count = 0
                    """

            time.sleep(0.01)  # Faster updates

    except KeyboardInterrupt:
        return last_weight
        
        print("\nExiting program")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if hx:
            hx.cleanup()

if __name__ == "__main__":
    do_calib = 0
    
    #below Ones are without motor cables its mounting plate but Torque Load Cell connected.
    weight1 = main(5,6,0,-1560757.0,do_calib)
    weight2 = main(12,13,90,1897135.5,do_calib)
    weight3 = main(20,21,180,323036.5,do_calib)
    weight4 = main(14,15,270,378924.5,do_calib)
    
    print("Weight = ", weight1+weight2+weight3+weight4," Kg")
    #below Ones are without motor cables its mounting plate.
    #main(5,6,0,-1407056.0,do_calib)
    #main(12,13,90,1990178.5,do_calib)
    #main(17,27,180,216426.5,do_calib)
    #main(22,23,270,321052.5,do_calib)
    #Below ones are with Motor Without Prop 
    """
    main(5,6,0,-1329711.0)
    main(12,13,90,2121215.0)
    main(17,27,180,315572.5)
    main(22,23,270,456413.5)
    #print(offset_list)
    """



