import time
import ADS1263
import RPi.GPIO as GPIO
from gpiozero import Button
import sys
import csv
from datetime import datetime
import os
import threading

# Configuration
REF = 5.00  # ADC and sensor supply reference voltage (5V)
FLOW_SENSOR_1_PIN = 23  # GPIO23 for flow sensor 1
FLOW_SENSOR_2_PIN = 24  # GPIO24 for flow sensor 2

# Global variables for sensor 1
pulse_count_1 = 0
flow_rate_1 = 0.0
pressure_bar_1 = 0.0
voltage_1 = 0.0
pbar_1 = 0.0
pbar_2 = 0.0
# Global variables for sensor 2
pulse_count_2 = 0
flow_rate_2 = 0.0
pressure_bar_2 = 0.0
voltage_2 = 0.0

# Thread lock for data synchronization
data_lock = threading.Lock()

# Sensor location input
loc_fil = input('Sensor Location: ')
log_dir = "sensor_logs"
os.makedirs(log_dir, exist_ok=True)
filename = os.path.join(
    log_dir, f"{loc_fil}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
)

# Interrupt-based pulse counter for flow sensor 1
def count_pulse_1():
    global pulse_count_1
    pulse_count_1 += 1

# Interrupt-based pulse counter for flow sensor 2
def count_pulse_2():
    global pulse_count_2
    pulse_count_2 += 1

# Initialize flow sensors
flow_sensor_1 = Button(FLOW_SENSOR_1_PIN, pull_up=True)
flow_sensor_1.when_pressed = count_pulse_1

flow_sensor_2 = Button(FLOW_SENSOR_2_PIN, pull_up=True)
flow_sensor_2.when_pressed = count_pulse_2

def get_flow_rate_1():
    global pulse_count_1
    # Convert pulses to flow rate using formula
    flow = (pulse_count_1 / 6.9)  # L/min
    pulse_count_1 = 0  # Reset count after reading
    return round(flow, 2)

def get_flow_rate_2():
    global pulse_count_2
    # Convert pulses to flow rate using formula
    flow = (pulse_count_2 / 6.9)  # L/min
    pulse_count_2 = 0  # Reset count after reading
    return round(flow, 2)

def pressure_sensor_thread():
    """Thread function for reading both pressure sensors"""
    global pressure_bar_1, voltage_1, pressure_bar_2, voltage_2
    
    try:
        ADC = ADS1263.ADS1263()
        if ADC.ADS1263_init_ADC1('ADS1263_50SPS') == -1:
            print("Failed to initialize ADC")
            return
        
        ADC.ADS1263_SetMode(0)  # Single-ended mode
        channelList = [0, 1]  # Channel 0 for sensor 1, Channel 1 for sensor 2
        
        while True:
            ADC_Value = ADC.ADS1263_GetAll(channelList)
            
            # Process sensor 1 (channel 0)
            raw_value_1 = ADC_Value[0]
            if raw_value_1 & 0x80000000:
                raw_value_1 -= 1 << 32  # Convert unsigned to signed int
            
            current_voltage_1 = (raw_value_1 / 2147483648.0) * REF
            
            # Convert voltage to pressure for sensor 1
            if current_voltage_1 < 0.5:
                pressure_psi_1 = 0.0
            elif current_voltage_1 > 4.5:
                pressure_psi_1 = 100.0
            else:
                pressure_psi_1 = ((current_voltage_1 - 0.5) / 4.0) * 100.0
            
            pbar_1 = pressure_psi_1 * 0.0689
            current_pressure_bar_1 = (.82 * pbar_1) - 0.017
            if(current_pressure_bar_1  < 0):
                current_pressure_bar_1 = 0.0
            
            
            
            # Process sensor 2 (channel 1)
            raw_value_2 = ADC_Value[1]
            if raw_value_2 & 0x80000000:
                raw_value_2 -= 1 << 32  # Convert unsigned to signed int
            
            current_voltage_2 = (raw_value_2 / 2147483648.0) * REF
            
            # Convert voltage to pressure for sensor 2
            if current_voltage_2 < 0.5:
                pressure_psi_2 = 0.0
            elif current_voltage_2 > 4.5:
                pressure_psi_2 = 100.0
            else:
                pressure_psi_2 = ((current_voltage_2 - 0.5) / 4.0) * 100.0
            
            pbar_2 = pressure_psi_2 * 0.0689
            current_pressure_bar_2 = (.82 * pbar_2) - 0.017
            if(current_pressure_bar_2  < 0):
                current_pressure_bar_2 = 0.0
            
                        
            # Update global variables with thread safety
            with data_lock:
                pressure_bar_1 = current_pressure_bar_1
                voltage_1 = current_voltage_1
                pressure_bar_2 = current_pressure_bar_2
                voltage_2 = current_voltage_2
            
            time.sleep(0.1)  # Read pressure every 100ms
            
    except Exception as e:
        print(f"Pressure sensor error: {e}")
    finally:
        try:
            ADC.ADS1263_Exit()
        except:
            pass

def flow_sensor_thread():
    """Thread function for reading both flow sensors"""
    global flow_rate_1, flow_rate_2
    
    try:
        while True:
            time.sleep(1)  # 1-second interval for flow measurement
            current_flow_1 = get_flow_rate_1()
            current_flow_2 = get_flow_rate_2()
            
            # Update global variables with thread safety
            with data_lock:
                flow_rate_1 = current_flow_1
                flow_rate_2 = current_flow_2
                
    except Exception as e:
        print(f"Flow sensor error: {e}")

def main():
    """Main logging function"""
    global pressure_bar_1, voltage_1, flow_rate_1, pressure_bar_2, voltage_2, flow_rate_2
    
    try:
        # Start sensor threads
        pressure_thread = threading.Thread(target=pressure_sensor_thread, daemon=True)
        flow_thread = threading.Thread(target=flow_sensor_thread, daemon=True)
        
        pressure_thread.start()
        flow_thread.start()
        
        # Wait a moment for sensors to initialize
        time.sleep(2)
        
        # Open CSV file for logging
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                "Timestamp", 
                "Pressure_1(Bar)", "Flow_1(L/min)",
                "Pressure_2(Bar)", "Flow_2(L/min)"
            ])
            
            print(f"Logging to {filename}...")
            print("Press Ctrl+C to stop.")
            print("Timestamp                    | Sensor 1                           | Sensor 2")
            print("                             | Press1(Bar) | Press2(Bar) | Flow1(L/min) | Flow2(L/min)")
            print("-" * 105)
            
            last_print = time.time()
            
            while True:
                # Get current timestamp
                timestamp = datetime.now()
                timestamp_str = timestamp.isoformat()
                
                # Read current sensor values with thread safety
                with data_lock:
                    current_pressure_1 = pressure_bar_1
              
                    current_flow_1 = flow_rate_1
                    current_pressure_2 = pressure_bar_2
            
                    current_flow_2 = flow_rate_2
                
                # Log to CSV
                writer.writerow([
                    timestamp_str, 
                    f"{current_pressure_1:.3f}", 
        
                    f"{current_flow_1:.2f}",
                    f"{current_pressure_2:.3f}", 
             
                    f"{current_flow_2:.2f}"
                ])
                
                # Print to console every 0.5 seconds
                if time.time() - last_print >= 0.5:
                    print(f"{timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}     | "
                          f"{current_pressure_1:9.3f}    | "
                          f"{current_pressure_2:9.3f}   | "
                          f"{current_flow_1:10.2f}  | "
                          f"{current_flow_2:10.2f}")
                    last_print = time.time()
                
                # Flush file buffer to ensure data is written
                file.flush()
                
                time.sleep(0.1)  # Main loop delay
                
    except IOError as e:
        print(f"IOError: {e}")
    except KeyboardInterrupt:
        print("\nCtrl+C: Exiting program...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Clean up
        try:
            flow_sensor_1.close()
            flow_sensor_2.close()
        except:
            pass
        print("Program terminated.")

if __name__ == "__main__":
    main()
