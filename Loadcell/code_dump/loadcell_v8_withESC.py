
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
'''
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import client
'''
TIMEOUT_SECONDS = 30

global nominal_rpm, kt_value, can_bus_timeout, multi_rate, CAN_CYCLIC_RATE, num_esc_param, refresh_rate
CAN_CYCLIC_RATE = 0x64
can_bus_timeout = 0.05
kt_value = 0.88
nominal_rpm = 6000
multi_rate = 0.5
num_esc_param = 5
refresh_rate = 0.1

###########
cloud_test = 0
'''
token = 'ZdwOtVTj7kWLmHqBk_p7d3Snz1Gbt2Xm1EPNUlY96xUxhV93wtU1KWb5mBZ6ubRl8I_m-ty0Jnxbb38Nl5nrFg=='
influxdb_url = "https://us-east-1-1.aws.cloud2.influxdata.com"  # InfluxDB URL
org = "IdeaForge"  # Organization name

bucket = "Thrust_Rig_v1"  # Bucket name

# Create an InfluxDB client
client = InfluxDBClient(url=influxdb_url, token=token)

# Create a Write API instance
write_api = client.write_api()
'''

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

def send_rpm_and_torque_and_kt():
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

            for msg in [cyclic_rpm_req, cyclic_torque_req, cyclic_temp_req, cyclic_current_req, cyclic_vdc_req]:
                print(msg.data)
                bus.send(msg)
                time.sleep(0.1)
            print("Sent RPM, Torque, Temp and Current requests")
    except Exception as e:
        print(f"Error sending RPM/Torque/KT: {e}")

def temp_out(temperature_raw):
    temperature_actual = 0.0000003 * temperature_raw ** 2 + 0.0103 * temperature_raw - 127.43
    return round(temperature_actual, 1)

def read_rpm_and_torque():
    try:
        with can_v1.can.interface.Bus(channel=can_v1.CAN_INTERFACE, interface="socketcan") as bus:
            while True:
                message = bus.recv(timeout=refresh_rate)
                if message and message.arbitration_id == can_v1.BAMOCAR_RESPONSE_ID:
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
    except Exception as e:
        print(f"Error reading ESC data: {e}")
        time.sleep(0.1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ESC Data Reader")
    parser.add_argument('--mode', type=str, choices=["esc_on", "esc_off", "esc_only"], required=False,
                        help='Mode to read: ESC ON or ESC OFF or ESC ONLY')
    parser.add_argument('--rate', type=float, required=False,
                        help='write Rate as 0.1, 0.02 ms')
    args = parser.parse_args()
    
    if args.rate is not None:
        refresh_rate = float(args.rate)
        if refresh_rate <= 0.001:
            CAN_CYCLIC_RATE = 0x01
        elif refresh_rate>0.01 and refresh_rate<0.25:
            CAN_CYCLIC_RATE = round(refresh_rate*100)
        else:
            CAN_CYCLIC_RATE = 0xFE
        

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
        {"dout": 27, "sck": 17},
        {"dout": 27, "sck": 17},
        {"dout": 14, "sck": 15},
        {"dout": 26, "sck": 19}
    ]
    #main(12,13,0,672139.0,do_calib)

    #main(20,21,90,607098.0,do_calib)
    #sys.exit()
    #main(26,19,180,1312548.0,do_calib)
   # main(23,24,270,-979799.5,do_calib)
    #hardcoded_offsets = [672139,607098,1312548,-979799,-541366,0,0,0]
    hardcoded_offsets = [585364, 699695.5, 1332767.5, -1095913.0, 0, 0, 0, 0]
    load_cell_labels = [
        "Thrust_0Deg", "Thrust_90Deg", "Thrust_180Deg", "Thrust_270Deg",
        "Torque_0Deg", "Torque_90Deg", "Torque_180Deg", "Torque_270Deg"
    ]

    process_list = []
    queue = multiprocessing.Queue()
    number_lc = 5

    latest_weights = [0.0] * number_lc
    last_update_times = [time.time()] * number_lc
    ESC_data_output = [0.0] * num_esc_param
    last_update_times_ESC = [time.time()] * num_esc_param

    # Setup CSV file
    os.makedirs("logs", exist_ok=True)
    csv_filename = f"logs/test_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_file = open(csv_filename, mode='w', newline='')
    csv_writer = csv.writer(csv_file)

    headers = ['Timestamp']
    for i in range(number_lc):
        headers.append(f"{load_cell_labels[i]} (Kg)")
    if esc_data_state == 1:
        headers += ['Total Weight (Kg)', 'Total Thrust (N)', 'RPM', 'Torque (Nm)', 'Motor Temp (°C)', 'Current (A)','Vdc (V)']
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
            time.sleep(refresh_rate)
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

            now = time.time()
            
            for i in range(number_lc):
                if now - last_update_times[i] > TIMEOUT_SECONDS and load_cell_state == 1:
                    print(f"No data from sensor {i + 1} for more than {TIMEOUT_SECONDS} seconds.")
                    raise TimeoutError
                if now - last_update_times_ESC[i] > TIMEOUT_SECONDS and esc_data_state == 1:
                    esc_keys = ["RPM", "Torque", "M.Temp", "Current","Vdc"]
                    print(f"No data from {esc_keys[i]} for more than {TIMEOUT_SECONDS} seconds.")
                    raise TimeoutError
            
            total_weight = sum(latest_weights[0:4])
            
            #if load_cell_state == 1:
             #   print(f"Latest Weights: {latest_weights} | Total: {total_weight:.2f} Kg | Thrust: {total_weight * 9.8: .3f} N")
            #if esc_data_state == 1:
             #   print(f"RPM: {ESC_data_output[0]} | Torque: {ESC_data_output[1]} Nm | M. Temp: {ESC_data_output[2]}  °C  | Current: {ESC_data_output[3]} A | VDC: {ESC_data_output[4]} V")
            #print("-----------------------------------------------------------------")
            
            '''
            # InfluxDB CLOUD DATA
            
            point = (
            Point("Weight_Data")
            .tag("Sensor", "4_Loadcell")
            .field("Weight", round(total_weight,2))
            )  
            
            point1 = (
            Point("Thrust_Data")
            .tag("Sensor", "Thrust")
            .field("Thrust", round(total_weight * 9.8,3))
            )  
            
            point2 = (
            Point("RPM_Data")
            .tag("Sensor", "RPM")
            .field("RPM",abs(float(ESC_data_output[0])))
            )  
            
            point3 = (
            Point("Torque_Data")
            .tag("Sensor", "Torque")
            .field("Torque", float(ESC_data_output[1]))
            )  
            
            point4 = (
            Point("Motor_Temp_Data")
            .tag("Sensor", "M_Temp")
            .field("Motor_Temp", ESC_data_output[2])
            )  
            
            point5 = (
            Point("Current_Data")
            .tag("Sensor", "Current")
            .field("Current", ESC_data_output[3])
            )  
            point6 = (
            Point("Vdc_Data")
            .tag("Sensor", "Vdc")
            .field("Vdc", ESC_data_output[4])
            )  
            if cloud_test != 1:
                write_api.write(bucket=bucket, org=org, record=point)
                write_api.write(bucket=bucket, org=org, record=point1)
                write_api.write(bucket=bucket, org=org, record=point2)
                write_api.write(bucket=bucket, org=org, record=point3)
                write_api.write(bucket=bucket, org=org, record=point4)
                write_api.write(bucket=bucket, org=org, record=point5)
                write_api.write(bucket=bucket, org=org, record=point6)
            '''
            
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
