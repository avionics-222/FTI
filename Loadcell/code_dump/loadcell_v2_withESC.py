import multiprocessing
import time
import can_v1
import can
import argparse
from math import sqrt
TIMEOUT_SECONDS = 20

global nominal_rpm, kt_value, can_bus_timeout, multi_rate
can_bus_timeout = 0.05
kt_value = 0.88
nominal_rpm = 6000
multi_rate = 0.5

def loadcell_worker(dout, sck, load_cell_label, offset, queue, index):
    from hx711_module import HX711
    hx = HX711(dout, sck)
    hx.offset = offset
    try:

        while True:
            # Simulate weight reading
            weight_out, raw = hx.get_weight() # Dummy value for demonstration
            queue.put((index, weight_out))  # Send (index, weight) to main process
            time.sleep(multi_rate)
    except KeyboardInterrupt:
        print(f"Stopped: {load_cell_label}")

def loadcell_worker_test(dout, sck, load_cell_label, offset, queue, index):
    try:
        while True:
            # Simulate weight reading
            if index == 2:
                time.sleep(15)
            weight = index * 10 + 0.1  # Dummy value for demonstration
            queue.put((index, weight))  # Send (index, weight) to main process
            time.sleep(multi_rate)
    except KeyboardInterrupt:
        print(f"Stopped: {load_cell_label}")

def send_rpm_and_torque_and_kt():
    try:
        cyclic_rpm_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.RPM_REGISTER, 0x64],
            is_extended_id=False
        )
        cyclic_torque_req = can.Message(  
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.TORQUE_REGISTER, 0x64],
            is_extended_id=False
        )
        cyclic_temp_req = can.Message(  
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.TEMP_REGISTER, 0x64],
            is_extended_id=False
        )
        cyclic_current_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.CURRENT_REGISTER, 0x64],
            is_extended_id=False
        )
        kt_req = can.Message(
            arbitration_id=can_v1.BAMOCAR_ID,
            data=[0x3D, can_v1.KT_REGISTER, 0x00],
            is_extended_id=False
        )
        kt_received = False
        with can_v1.can.interface.Bus(channel=can_v1.CAN_INTERFACE, interface="socketcan") as bus:
            try:
                while kt_received == False:
                    bus.send(kt_req)
                    time.sleep(0.1)
                    #print("##############################################")
                    message = bus.recv(timeout=can_bus_timeout)
                    if message and message.arbitration_id == can_v1.BAMOCAR_RESPONSE_ID:
                        data = message.data
                        if data[0] == can_v1.KT_REGISTER:
                            kt_raw = (data[2] << 8) | data[1]
                            kt_value = kt_raw * 0.001
                            print(f"Kt Value: {kt_value}")
                            kt_received = True
                    time.sleep(0.1)
            except Exception as e:
                print(f"Error occurred while receiving Kt Value: {e}")
                kt_received = False
            
                bus.send(cyclic_rpm_req)
                time.sleep(0.1)
                bus.send(cyclic_torque_req)
                time.sleep(0.1)
                bus.send(cyclic_temp_req)
                time.sleep(0.1)
                bus.send(cyclic_current_req)
                time.sleep(0.1)
                print("Sent RPM, Torque, Temp and Current requests")
    except Exception as e:
        print(f"Error occurred while sending RPM and Torque and KT Req: {e}")


def temp_out(temperature_raw):
    temperature_actual = (-4e-6 * temperature_raw**4) + (0.0004 * temperature_raw**3) - (0.0189 * temperature_raw**2) + (56.338 * temperature_raw) + 9384.7
    return temperature_actual

def read_rpm_and_torque():
    try:
        with can_v1.can.interface.Bus(channel=can_v1.CAN_INTERFACE, interface="socketcan") as bus:
            while True:
                message = bus.recv(timeout=can_bus_timeout)
                
                if message and message.arbitration_id == can_v1.BAMOCAR_RESPONSE_ID:
                    data = message.data
                    print("##############################################")
                    if data[0] == can_v1.RPM_REGISTER:
                        rpm_raw = (data[2] << 8) | data[1]
                        if rpm_raw > 32767:
                            rpm_raw -= 65536
                        rpm = round(float(rpm_raw)*nominal_rpm/32767.0, 2) 
                        print(f"RPM: {rpm}")
                        queue.put("RPM",rpm)
                    elif data[0] == can_v1.TORQUE_REGISTER:
                        torque_raw = (data[2] << 8) | data[1]
                        torque = round(float(torque_raw)*169.7*kt_value/(32767*sqrt(2)), 2)
                        print(f"Torque: {torque}")
                        queue.put("Torque",torque)
                    elif data[0] == can_v1.TEMP_REGISTER:
                        temperature_raw = (data[2] << 8) | data[1]
                        temperature_actual = temp_out(temperature_raw)
                        print(f"Temperature: {temperature_actual}")
                        queue.put("Temperature",temperature_actual)
                    elif data[0] == can_v1.CURRENT_REGISTER:
                        current_raw = (data[2] << 8) | data[1]
                        if current_raw > 32767:
                            current_raw -= 65536    
                        current = round(float(current_raw)*0.138768, 2)
                        print(f"Current: {current}")
                        queue.put("Current",current)

    except Exception as e:
        print(f"Error occurred while reading RPM and Torque: {e}")
        time.sleep(0.1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ESC Data Reader")
    parser.add_argument('--mode', type=str, choices=["esc_on", "esc_off"], required=False,
                        help='Mode to read: ESC ON or ESC OFF')
    args = parser.parse_args()
    if args.mode == "esc_on":
        print("ESC ON")
        esc_data_state = 1
        try:
            can_v1.setup_can_interface()
        except Exception as e:
            print("Error while interfacing CAN :",e)
    elif args.mode == "esc_off":
        print("ESC OFF")
        esc_data_state = 0
    else:
        print("Invalid mode. Use 'esc_on' or 'esc_off'.")
        esc_data_state = 0
    

        
        

    load_cells_config = [
        {"dout": 12,  "sck": 13},
        {"dout": 20, "sck": 21},
        {"dout": 14, "sck": 15},
        {"dout": 23, "sck": 24},
        {"dout": 12, "sck": 13},
        {"dout": 20, "sck": 21},
        {"dout": 14, "sck": 15},
        {"dout": 26, "sck": 19}
    ]
    print(load_cells_config)
    hardcoded_offsets = [585364,699695.5,1332767.5,-1095913.0,0,0,0,0]#with Motor
    hardcoded_offsets_1=[503894,606426,1188189,-1238130,0,0,0,0]#Only Stand
    hardcoded_offsets_2 = [-1139, 30683, 213752, 81425,
        109366, 38272, -216303, 84388]
    load_cell_labels = [
        "Thrust_0Deg", "Thrust_90Deg", "Thrust_180Deg", "Thrust_270Deg",
        "Torque_0Deg", "Torque_90Deg", "Torque_180Deg", "Torque_270Deg"
    ]

    process_list = []
    queue = multiprocessing.Queue()
    number_lc = 4 #Change this number if you want to increase number of load cell5

    latest_weights = [0.0] * number_lc
    last_update_times = [time.time()] * number_lc
    ESC_data_output = [0.0]*4
    last_update_times_ESC = [time.time()] * 4

    try:
        if esc_data_state == 1:
            send_rpm_and_torque_and_kt()
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
            time.sleep(0.1)
            # Read all messages
            while not queue.empty():
                key_id, value = queue.get()
                if isinstance(key_id, int):
                    index = key_id
                    weight = value
                    latest_weights[index] = round(weight,2)
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

                        
                
                

            # Check for timeout
            now = time.time()
            for i in range(number_lc):
                if now - last_update_times[i] > TIMEOUT_SECONDS:
                    print(f"No data from sensor {i+1} for more than {TIMEOUT_SECONDS} seconds.")
                    raise TimeoutError
                if now - last_update_times_ESC[i] > TIMEOUT_SECONDS and esc_data_state == 1:
                    if i == 0: 
                        value_type = "RPM"
                    elif i == 1:
                        value_type = "Torque"
                    elif i == 2:
                        value_type = "Temperature"
                    elif i == 3:
                        value_type = "Current"
                    print(f"No data from {value_type} for more than {TIMEOUT_SECONDS} seconds.")
                    #print(f"No data from ESC {i+1} for more than {TIMEOUT_SECONDS} seconds.")
                    raise TimeoutError

            total_weight = sum(latest_weights)
            
            print(f"Latest Weights: {latest_weights} | Total: {total_weight:.2f} Kg | Thrust: {total_weight*9.8: .3f} N")
            if esc_data_state == 1:
                print(f"RPM: {ESC_data_output[0]} | Torque: {ESC_data_output[1]} | Temperature: {ESC_data_output[2]} | Current: {ESC_data_output[3]}")
                time.sleep(0.1)

    except (KeyboardInterrupt, TimeoutError):
        print("Exiting due to KeyboardInterrupt or timeout.")
        for p in process_list:
            p.terminate()
            p.join()
