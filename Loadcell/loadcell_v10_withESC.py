import multiprocessing
import time
import can_v1
import can
import argparse
import sys
import os
import csv
from datetime import datetime
from math import sqrt

import signal
## noutused
running = True

def handle_termination(signum, frame):
    global running
    print("Termination signal received. Cleaning up...")
    running = False

signal.signal(signal.SIGTERM, handle_termination)

##____
TIMEOUT_SECONDS = 30

global nominal_rpm, kt_value, can_bus_timeout, multi_rate, CAN_CYCLIC_RATE, num_esc_param
CAN_CYCLIC_RATE = 0x3C
can_bus_timeout = 0.05
kt_value = 0.88
nominal_rpm = 6000
multi_rate = 0.5
num_esc_param = 11


def loadcell_worker(dout, sck, load_cell_label, offset, queue, index):
    from hx711_module import HX711
    hx = HX711(dout, sck)
    hx.offset = offset
    try:
        while True:
            weight_out, raw = hx.get_weight()
            queue.put((index, weight_out))
            time.sleep(multi_rate)
    except KeyboardInterrupt:
        print(f"Stopped: {load_cell_label}")

def loadcell_worker_test(dout, sck, load_cell_label, offset, queue, index):
    try:
        while True:
            if index == 2:
                time.sleep(15)
            weight = index * 10 + 0.1
            queue.put((index, weight))
            time.sleep(multi_rate)
    except KeyboardInterrupt:
        print(f"Stopped: {load_cell_label}")

def send_rpm_and_torque_and_kt(CAN_CYCLIC_RATE = CAN_CYCLIC_RATE):
    try:
        cyclic_rpm_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.RPM_REGISTER, CAN_CYCLIC_RATE],
            is_extended_id=False
        )
        cyclic_torque_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.TORQUE_REGISTER, CAN_CYCLIC_RATE],
            is_extended_id=False
        )
        cyclic_temp_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.TEMP_REGISTER, CAN_CYCLIC_RATE],
            is_extended_id=False
        )
        cyclic_current_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.CURRENT_REGISTER, CAN_CYCLIC_RATE],
            is_extended_id=False
        )
        cyclic_vdc_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.VDC_REGISTER, CAN_CYCLIC_RATE],
            is_extended_id=False
        )
        cyclic_vout_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.VOUT_VXXX, CAN_CYCLIC_RATE],
            is_extended_id=False
        )
        cyclic_igbt_temp_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.IGBT_TEMP, CAN_CYCLIC_RATE],
            is_extended_id=False
        )

        kt_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.KT_REGISTER, 0x00],
            is_extended_id=False
        )
        kt_received = False
        with can_v1.can.interface.Bus(channel=can_v1.CAN_INTERFACE, interface="socketcan") as bus:
            while not kt_received:
                bus.send(kt_req)
                time.sleep(0.1)
                message = bus.recv(timeout=can_bus_timeout)
                if message and message.arbitration_id == can_v1.BAMOCAR_RESPONSE_ID:
                    data = message.data
                    if data[0] == can_v1.KT_REGISTER:
                        kt_raw = (data[2] << 8) | data[1]
                        kt_value = kt_raw * 0.001
                        print(f"Kt Value: {kt_value}")
                        kt_received = True

            for msg in [cyclic_rpm_req, cyclic_torque_req, cyclic_temp_req, cyclic_current_req, cyclic_vdc_req, cyclic_vout_req,cyclic_igbt_temp_req]:
                #print(msg.data)
                bus.send(msg)
                time.sleep(0.1)
            print("Sent RPM, Torque, Temp and Current requests")
    except Exception as e:
        print(f"Error sending RPM/Torque/KT: {e}")

def temp_out(temperature_raw):
    temperature_actual = 0.0000003 * temperature_raw ** 2 + 0.0103 * temperature_raw - 127.43
    return round(temperature_actual, 1)
def igbt_temp_out(igbt_temp_raw):
    igbt_temp_value = 6e-08 *igbt_temp_raw *igbt_temp_raw + 0.0032*igbt_temp_raw - 23.236
    
    return round(igbt_temp_value)
def read_rpm_and_torque():
    try:
        with can_v1.can.interface.Bus(channel=can_v1.CAN_INTERFACE, interface="socketcan") as bus:
            while True:
                message = bus.recv(timeout=can_bus_timeout)
                #print(message)
                if message and message.arbitration_id == can_v1.BAMOCAR_RESPONSE_ID:
                    #print(message.data)
                    data = message.data
                    if data[0] == can_v1.RPM_REGISTER:
                        rpm_raw = (data[2] << 8) | data[1]
                        if rpm_raw > 32767:
                            rpm_raw -= 65536
                        rpm = round(rpm_raw * nominal_rpm / 32767,2)
                        if rpm is not None:
                            queue.put(("RPM", rpm))
                    elif data[0] == can_v1.TORQUE_REGISTER:
                        torque_raw = (data[2] << 8) | data[1]
#                        print("##################",torque_raw)
                        torque = round(torque_raw * 169.7 * kt_value / (32767 * sqrt(2)),2)
                        if round(torque)>=200:
                            pass
                        elif torque is not None:
                            queue.put(("Torque", torque))
                    elif data[0] == can_v1.TEMP_REGISTER:
                        temperature_raw = (data[2] << 8) | data[1]
                        temperature_actual = temp_out(temperature_raw)
                        queue.put(("Temperature", temperature_actual))
                    elif data[0] == can_v1.CURRENT_REGISTER:
                        current_raw = (data[2] << 8) | data[1]
                        if current_raw > 32767:
                            current_raw -= 65536
                        current = round(current_raw * 0.138768, 2)
                        queue.put(("Current", current))
                    elif data[0] == can_v1.VDC_REGISTER:
                        vdc_raw = (data[2] << 8) | data[1]
                        if vdc_raw > 32767:
                            vdc_raw -= 65536
                        
                        vdc = round(vdc_raw * 0.0316635, 2)
                        queue.put(("Vdc", vdc))
                    elif data[0] == can_v1.VOUT_VXXX:
                        vout_vxxx = (data[2] << 8) | data[1]
                        if vout_vxxx > 32767:
                            vout_vxxx-= 65536
                        vout = round(vout_vxxx,2)
                        vout_ph = vout/4096*vdc/sqrt(2)*0.92
                       # print(vout)
                        queue.put(("Vout",vout_ph))
                    elif data[0] == can_v1.IGBT_TEMP:
                        itemp_raw = (data[2] << 8) | data[1]
                        igtemp = igbt_temp_out(itemp_raw)
                        queue.put(('Igbt',igtemp))
                elif message and message.arbitration_id == can_v1.BAMOCAR_ID:
                    data = message.data
                    #print(data)
                    if data[0] == can_v1.RPM_COMMAND_REGISTER:
                        rpm_command_raw = (data[2] << 8) | data[1]
                        if rpm_command_raw > 32767:
                            rpm_command_raw -= 65536
                        rpm_command = round(rpm_command_raw * nominal_rpm / 32767,2)
                        if rpm_command is not None:
                            queue.put(("RPM_Command", rpm_command))
                elif message and message.arbitration_id == can_v1.BATTERY_ID:
                    data = message.data
                    #print(list(data))
            
                    #value = (data[0] << 48) | (data[1] << 32) | (data[2] << 16) | data[3]
                    
                    #print("Value",value)
                    #print(data)
                    byte_data = bytes(data)

                    # Convert to binary string
                    bin_data = ''.join(f'{byte:08b}' for byte in byte_data)
                   #$ print(bin_data)
                    # Extract bit fields
                    first_14_bits = bin_data[0:14]
                    second_14_bits = bin_data[14:28]
                    third_18_bits = bin_data[28:46]
                    fourth_18_bits = bin_data[46:64]

                    # Convert binary strings to integers
                    battery_volt_message = int(first_14_bits, 2)*0.1
                    battery_bus_voltage_message = int(second_14_bits, 2)*0.1
                    
                    #battery_power_raw = int(third_18_bits, 2)
                    #battery_current = int(fourth_18_bits, 2)
                    #print(third_18_bits)
                    def to_signed_18bit(bin_str):
                        val = int(bin_str, 2)
                        if val & (1 << 17):  # If sign bit is set (bit 17)
                            val -= 1 << 18   # Subtract 2^18 to get negative value
                        return val

# Convert signed fields
                    battery_power_raw = to_signed_18bit(third_18_bits)*10
                    battery_current_raw = to_signed_18bit(fourth_18_bits)*0.01
                    """
                   
                    if battery_power_raw & (1 << 17):  # Check if the sign bit is set (bit 21 of 22)
                        battery_power = (battery_power_raw - (1 << 18))*0.01
                    else:
                        battery_power = battery_power_raw*0.01  # 0x3FFF = (1 << 14) - 1
                    """
                    #print(value,"&&&&&&&&&&&&&&&&&")
                    queue.put(("Battery_Volt", round(battery_volt_message,2)))
                    queue.put(("Battery_Bus_Volt", round(battery_bus_voltage_message,2)))
                    queue.put(("Battery_Power", round(battery_power_raw,2)))


                    

    except Exception as e:
        print(f"Error reading ESC data: {e}")
        time.sleep(0.1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ESC Data Reader")
    parser.add_argument('--mode', type=str, choices=["esc_on", "esc_off", "esc_only"], required=False,
                        help='Mode to read: ESC ON or ESC OFF or ESC ONLY')
    args = parser.parse_args()

    if args.mode == "esc_on" or args.mode == "esc_only":
        print("ESC ON")
        esc_data_state = 1
        try:
            can_v1.setup_can_interface()
        except Exception as e:
            print("Error while interfacing CAN:", e)
        load_cell_state = 1
        if args.mode == "esc_only":
            load_cell_state = 0
    elif args.mode == "esc_off":
        print("ESC OFF")
        esc_data_state = 0
        load_cell_state = 1
    else:
        print("Invalid mode. Use 'esc_on' or 'esc_off'.")
        esc_data_state = 0
        load_cell_state = 1

    load_cells_config = [
        {"dout": 12, "sck": 13},
        {"dout": 20, "sck": 21},
        {"dout": 26, "sck": 19},
        {"dout": 23, "sck": 24},
        {"dout": 1, "sck": 2},
        {"dout": 1, "sck": 2},
        {"dout": 1, "sck": 2},
        {"dout": 1, "sck": 2}
    ]
    
    #hardcoded_offsets = [585364, 699695.5, 1332767.5, -1095913.0, 0, 0, 0, 0]
    hardcoded_offsets = [311303.0, 1043999.0, 1099991.0, -821255.5, 0, 0, 0, 0]
    
    load_cell_labels = [
        "Thrust_0Deg", "Thrust_90Deg", "Thrust_180Deg", "Thrust_270Deg",
        "Torque_0Deg", "Torque_90Deg", "Torque_180Deg", "Torque_270Deg"
    ]

    process_list = []
    queue = multiprocessing.Queue()
    number_lc = 4

    latest_weights = [0.0] * number_lc
    last_update_times = [time.time()] * number_lc
    ESC_data_output = [0.0] * num_esc_param
    last_update_times_ESC = [time.time()] * num_esc_param

    # Setup CSV file
    os.makedirs("logs", exist_ok=True)
    csv_filename = f"logs/{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_file = open(csv_filename, mode='w', newline='')
    csv_writer = csv.writer(csv_file)

    headers = ['Timestamp']
    for i in range(number_lc):
        headers.append(f"{load_cell_labels[i]} (Kg)")
    if esc_data_state == 1:
        headers += ['Total Weight (Kg)', 'Total Thrust (N)', 'RPM'+str(can_v1.RPM_REGISTER), 'Torque (Nm)'+str(can_v1.TORQUE_REGISTER), 'Motor Temp (°C)'+str(can_v1.TEMP_REGISTER), 'Current (A)'+str(can_v1.CURRENT_REGISTER),'Vdc (V)'+str(can_v1.VDC_REGISTER),'Vout(V)'+str(can_v1.VOUT_VXXX),'IGBT Temp(°C)'+str(can_v1.IGBT_TEMP),'RPM Command'+str(can_v1.RPM_COMMAND_REGISTER),'Battery_Volt(V)'+str(can_v1.BATTERY_ID),'Battery_Bus_Volt(V)'+str(can_v1.BATTERY_ID),'Battery_Power(W)'+str(can_v1.BATTERY_ID)]
    csv_writer.writerow(headers)

    try:
        if esc_data_state == 1:
            send_rpm_and_torque_and_kt()

        if load_cell_state == 1:
            for i in range(number_lc):
                p = multiprocessing.Process(
                    target=loadcell_worker,
                    args=(load_cells_config[i]["dout"], load_cells_config[i]["sck"],
                          load_cell_labels[i], hardcoded_offsets[i], queue, i)
                )
                process_list.append(p)
                p.start()

        if esc_data_state == 1:
            ESC_data_thread = multiprocessing.Process(
                target=read_rpm_and_torque
            )
            process_list.append(ESC_data_thread)
            ESC_data_thread.start()

        while True:
            time.sleep(can_bus_timeout)
            while not queue.empty():
                try:
                    key_id, value = queue.get()
                except Exception as e:
                    print("Issue with Queue.get")
                if isinstance(key_id, int):
                    index = key_id
                    latest_weights[index] = round(value, 2)
                    last_update_times[index] = time.time()
                elif isinstance(key_id, str):
                    if key_id == "RPM":
                        ESC_data_output[0] = value
                        last_update_times_ESC[0] = time.time()
                    elif key_id == "Torque":
                        ESC_data_output[1] = value
                        last_update_times_ESC[1] = time.time()
                    elif key_id == "Temperature":
                        ESC_data_output[2] = value
                        last_update_times_ESC[2] = time.time()
                    elif key_id == "Current":
                        ESC_data_output[3] = value
                        last_update_times_ESC[3] = time.time()
                    elif key_id == "Vdc":
                        ESC_data_output[4] = value
                        last_update_times_ESC[4] = time.time()
                    elif key_id == "Vout":
                        ESC_data_output[5] = value
                        last_update_times_ESC[5] = time.time()
                    elif key_id == "Igbt":
                        ESC_data_output[6] = value
                        last_update_times_ESC[6] = time.time()
                    elif key_id == "RPM_Command":
                        ESC_data_output[7] = value
                        last_update_times_ESC[7] = time.time()  
                    elif key_id == "Battery_Volt":
                        ESC_data_output[8] = value
                        last_update_times_ESC[8] = time.time()
                    elif key_id == "Battery_Bus_Volt":
                        ESC_data_output[9] = value
                        last_update_times_ESC[9] = time.time()
                    elif key_id == "Battery_Power":
                        ESC_data_output[10] = value
                        last_update_times_ESC[10] = time.time()

            now = time.time()
            for i in range(number_lc):
                if now - last_update_times[i] > TIMEOUT_SECONDS and load_cell_state == 1:
                    print(f"No data from sensor {i + 1} for more than {TIMEOUT_SECONDS} seconds.")
                    raise TimeoutError
                if now - last_update_times_ESC[i] > TIMEOUT_SECONDS and esc_data_state == 1:
                    esc_keys = ['RPM'+str(can_v1.RPM_REGISTER), 'Torque (Nm)'+str(can_v1.TORQUE_REGISTER), 'Motor Temp (°C)'+str(can_v1.TEMP_REGISTER), 'Current (A)'+str(can_v1.CURRENT_REGISTER),'Vdc (V)'+str(can_v1.VDC_REGISTER),'Vout(V)'+str(can_v1.VOUT_VXXX),'IGBT Temp(°C)'+str(can_v1.IGBT_TEMP),'RPM Command'+str(can_v1.RPM_COMMAND_REGISTER),'Battery_Volt(V)'+str(can_v1.BATTERY_ID),'Battery_Bus_Volt(V)'+str(can_v1.BATTERY_ID),'Battery_Power(W)'+str(can_v1.BATTERY_ID)]
                    print(f"No data from {esc_keys[i]} for more than {TIMEOUT_SECONDS} seconds.")
                    raise TimeoutError

            total_weight = sum(latest_weights[0:4])     #-2.05      #Propeller + Bolt Weight ~= 2Kg (two blade) ~= 4.05 Kg (three blade)
            
            if load_cell_state == 1:
                print(f"Latest Weights: {latest_weights} | Total: {total_weight:.2f} Kg | Thrust: {total_weight * 9.8: .3f} N")
            if esc_data_state == 1:
                print(f"RPM: {ESC_data_output[0]} | Torque: {ESC_data_output[1]} Nm | M. Temp: {ESC_data_output[2]}  °C  | Current: {ESC_data_output[3]} A | VDC: {ESC_data_output[4]} V | Vout: {ESC_data_output[5]} V | IGBT Temp: {ESC_data_output[6]}°C | RPM Command: {ESC_data_output[7]} | Battery Volt: {ESC_data_output[8]} | Battery Bus Volt: {ESC_data_output[9]} | Battery Power: {ESC_data_output[10]} W" )
            print("-----------------------------------------------------------------")
            
            
            # Log data to CSV
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            row = [timestamp]
            if load_cell_state == 1 and esc_data_state == 0:
                row.extend(latest_weights[:number_lc])
                row.extend([round(total_weight,3), round(total_weight*9.8,3)])
                row.extend([0,0,0,0,0])
            if esc_data_state == 1 and load_cell_state == 0:
                row.extend([0.0]*number_lc)
                row.extend([round(total_weight,3), round(total_weight*9.8,3)])
                row.extend(ESC_data_output)
            if esc_data_state == 1 and load_cell_state == 1:
                row.extend(latest_weights[:number_lc])
                row.extend([round(total_weight,3), round(total_weight*9.8,3)])
                row.extend(ESC_data_output)
            csv_writer.writerow(row)
            csv_file.flush()



    except (KeyboardInterrupt, TimeoutError):
        print("Exiting due to KeyboardInterrupt or timeout.")
        for p in process_list:
            p.terminate()
            p.join()
        csv_file.close()
        can_v1.clear_can_bus_buffer()
        try:
            send_rpm_and_torque_and_kt(CAN_CYCsLIC_RATE=0xFF)
        except Exception as e:
            print("ESC CAN Stop failed.")
