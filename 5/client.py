from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
from datetime import datetime, timedelta, timezone
import random
import time
import threading
import queue
import socket
import json

def load_config():
    try:
        with open('../config.json', 'r') as f:
            config = json.load(f)
        print("Configuration loaded successfully from config.json")
        return config
    except FileNotFoundError:
        print("Error: config.json file not found!")
        exit(1)
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in config.json!")
        exit(1)

def create_server_url(config, server_type):
    server_config = config[server_type]
    protocol = server_config.get('protocol', 'http')
    ip = server_config['ip']
    port = server_config['port']
    url = f"{protocol}://{ip}:{port}"
    print(f"Created {server_type} URL: {url}")
    return url

# Load configuration from JSON file
config = load_config()
server_url = create_server_url(config, 'server')
teacher_url = create_server_url(config, 'teacher')
client_url = create_server_url(config, 'client')

server = ServerProxy(server_url + "/RPC2", allow_none=True)
teacher = ServerProxy(teacher_url + "/RPC2", allow_none=True)

# Student data from config
student_list = config.get('student_list', [29, 40, 42, 50, 52])
student_status = {i: 0 for i in student_list}
test_duration_sec = config.get('test_duration_sec', 60)

students_info = {29: "Mayuresh", 40: "Ayush", 42: "Aashna", 50: "Rohit", 52: "Rushikesh"}

# Global variables
clock_offset = None
logical_clock = 0
request_queue = queue.PriorityQueue()
deferred_replies = []
requesting_cs = False
in_cs = False
num_nodes = 3
replies_received = 0
cs_lock = threading.Lock()
my_process_id = "client"
copying_students = [29, 40, 50, 52]  # Students who use critical section

def increment_clock():
    global logical_clock
    logical_clock += 1
    return logical_clock

def update_clock(received_timestamp):
    global logical_clock
    logical_clock = max(logical_clock, received_timestamp) + 1

def get_formatted_time():
    global clock_offset
    current_time = datetime.now() + clock_offset
    return current_time.strftime("%d/%b/%Y %H:%M:%S")

def input_initial_time():
    h, m, s = map(int, input("Enter client time (HH MM SS): ").split())
    manual_time = datetime.now(timezone.utc).replace(hour=h, minute=m, second=s, microsecond=0)
    system_utc = datetime.now(timezone.utc).replace(microsecond=0)
    return manual_time - system_utc

def get_local_time():
    global clock_offset
    return datetime.now(timezone.utc) + clock_offset

def adjust_time(offset_sec):
    global clock_offset
    clock_offset += timedelta(seconds=offset_sec)
    print(f"Client time adjusted by {offset_sec:.6f} seconds")

def send_request_with_timeout(url, method, *args, timeout=5):
    try:
        proxy = ServerProxy(url + "/RPC2", allow_none=True)
        proxy._ServerProxy__transport.timeout = timeout
        result = getattr(proxy, method)(*args)
        return result
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT] Timeout/Error calling {method}: {e}")
        return False

# Berkeley Clock Synchronization (PRESERVED FROM BASE)
def berkeley_sync():
    print("\n=== BERKELEY CLOCK SYNCHRONIZATION ===")
    print("Step 1: Time Master (Client) collecting times from all nodes")
    
    times = {}
    self_time = get_local_time().replace(tzinfo=None)
    print(f"Step 2: [Client] Local time: {self_time.time()}")
    times["client"] = self_time

    try:
        times["server"] = datetime.fromisoformat(server.get_time()).replace(tzinfo=None)
        times["teacher"] = datetime.fromisoformat(teacher.get_time()).replace(tzinfo=None)
        
        print(f"Step 3: [Server] Time: {times['server'].time()}")
        print(f"Step 3: [Teacher] Time: {times['teacher'].time()}")

        all_times = list(times.values())
        avg_time = sum([t.timestamp() for t in all_times]) / len(all_times)
        avg_dt = datetime.fromtimestamp(avg_time)
        
        print(f"Step 4: [Berkeley] Calculated average time: {avg_dt.time()}")
        print(f"Step 5: Applying time adjustments to all nodes:")
        
        adjust_time((avg_dt - self_time).total_seconds())
        server.adjust_time((avg_dt - times['server']).total_seconds())
        teacher.adjust_time((avg_dt - times['teacher']).total_seconds())

        print("Step 6: Verification - All nodes synchronized")
        print("=== SYNCHRONIZATION COMPLETE ===\n")

    except Exception as e:
        print(f"Berkeley sync failed: {e}")

# Ricart-Agrawala Implementation
def request_critical_section(student_id):
    global requesting_cs, replies_received, logical_clock
    timestamp = increment_clock()
    print(f"[{get_formatted_time()}] [CLIENT-RA] Student {students_info.get(student_id, student_id)} requesting Critical Section at logical time {timestamp}")
    
    requesting_cs = True
    replies_received = 0
    
    try:
        success1 = send_request_with_timeout(teacher_url, "receive_request", my_process_id, timestamp)
        if success1:
            print(f"[{get_formatted_time()}] [CLIENT-RA] Sent REQUEST to teacher with timestamp {timestamp}")
        
        success2 = send_request_with_timeout(server_url, "receive_request", my_process_id, timestamp)
        if success2:
            print(f"[{get_formatted_time()}] [CLIENT-RA] Sent REQUEST to server with timestamp {timestamp}")
            
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-RA] Error sending requests: {e}")
    
    threading.Thread(target=wait_for_replies_timeout, args=(timestamp, student_id), daemon=True).start()

def wait_for_replies_timeout(request_timestamp, student_id, timeout=10):
    global requesting_cs
    start_time = time.time()
    
    while requesting_cs and replies_received < (num_nodes - 1):
        if time.time() - start_time > timeout:
            print(f"[{get_formatted_time()}] [CLIENT-RA] Timeout waiting for replies, entering CS")
            break
        time.sleep(0.1)
    
    if requesting_cs:
        enter_critical_section(student_id)

def receive_request(sender_id, timestamp):
    global logical_clock, deferred_replies, requesting_cs, in_cs
    update_clock(timestamp)
    print(f"[{get_formatted_time()}] [CLIENT-RA] Received REQUEST from {sender_id} with timestamp {timestamp}, my clock: {logical_clock}")
    
    should_reply = True
    if requesting_cs or in_cs:
        if timestamp > logical_clock or (timestamp == logical_clock and sender_id > my_process_id):
            should_reply = False
            deferred_replies.append(sender_id)
            print(f"[{get_formatted_time()}] [CLIENT-RA] Deferring reply to {sender_id}")
    
    if should_reply:
        send_reply(sender_id)
    
    return True

def send_reply(receiver_id):
    global logical_clock
    timestamp = increment_clock()
    print(f"[{get_formatted_time()}] [CLIENT-RA] Sending REPLY to {receiver_id} with timestamp {timestamp}")
    
    try:
        if receiver_id == "teacher":
            success = send_request_with_timeout(teacher_url, "receive_reply", my_process_id, timestamp)
        elif receiver_id == "server":
            success = send_request_with_timeout(server_url, "receive_reply", my_process_id, timestamp)
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-RA] Error sending reply: {e}")

def receive_reply(sender_id, timestamp):
    global replies_received, logical_clock, requesting_cs, in_cs
    update_clock(timestamp)
    print(f"[{get_formatted_time()}] [CLIENT-RA] Received REPLY from {sender_id} with timestamp {timestamp}, my clock: {logical_clock}")
    
    if requesting_cs:
        replies_received += 1
        print(f"[{get_formatted_time()}] [CLIENT-RA] Replies received: {replies_received}/{num_nodes-1}")
    
    return True

def enter_critical_section(student_id=None):
    global in_cs, requesting_cs
    
    if student_id is None:
        student_id = random.choice(copying_students)
    
    print(f"[{get_formatted_time()}] [CLIENT-RA] *** ENTERING CRITICAL SECTION (MARKSHEET ACCESS) ***")
    print(f"[{get_formatted_time()}] [CLIENT-RA] Student {students_info.get(student_id, student_id)} accessing marksheet to copy...")
    
    in_cs = True
    requesting_cs = False
    
    time.sleep(3)  # Simulate copying time
    
    # Update student status
    student_status[student_id] += 1
    status = student_status[student_id]
    
    try:
        msg = server.update_status(student_id, status)
        if status == 1:
            print(f"[{get_formatted_time()}] [CLIENT-RA] Student {student_id} WARNED (marks: 100→50)")
        elif status == 2:
            print(f"[{get_formatted_time()}] [CLIENT-RA] Student {student_id} CAUGHT (marks: 50→0 FAILED)")
        print(f"[{get_formatted_time()}] [CLIENT-RA] Server response: {msg}")
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-RA] Failed to update server: {e}")
    
    exit_critical_section()

def exit_critical_section():
    global in_cs, deferred_replies, logical_clock
    print(f"[{get_formatted_time()}] [CLIENT-RA] *** EXITING CRITICAL SECTION (MARKSHEET ACCESS) ***")
    
    in_cs = False
    
    for receiver_id in deferred_replies:
        send_reply(receiver_id)
        print(f"[{get_formatted_time()}] [CLIENT-RA] Sent deferred reply to {receiver_id}")
    
    deferred_replies.clear()

# Original functions (PRESERVED FROM BASE)
def pick_valid_student():
    valid = [num for num, status in student_status.items() if status < 2]
    return random.choice(valid) if valid else None

def send_student():
    student = pick_valid_student()
    
    if student is None:
        print(f"[{get_formatted_time()}] All students processed. Requesting final marksheet...")
        try:
            teacher.print_results()
        except Exception as e:
            print(f"[{get_formatted_time()}] Failed to get marksheet: {e}")
        return False
    
    # Students in copying_students use critical section
    if student in copying_students:
        print(f"[{get_formatted_time()}] [CLIENT] Student {students_info.get(student, student)} wants to copy - requesting CS")
        request_critical_section(student)
        return True
    else:
        # Regular cheating (non-CS students)
        student_status[student] += 1
        status = student_status[student]
        
        try:
            msg = server.update_status(student, status)
            action = "warned" if status == 1 else "caught"
            print(f"[{get_formatted_time()}] Sent {student} — Status: {status} ({action}) — Server: {msg}")
            return True
        except Exception as e:
            print(f"[{get_formatted_time()}] Failed to send student {student}: {e}")
            return False

def get_time_for_berkeley():
    return get_local_time().isoformat()

def run_client_server():
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    print(f"[{get_formatted_time()}] [CLIENT-RA] Starting XML-RPC server on {local_ip}:{config['client']['port']}")
    
    server_instance = SimpleXMLRPCServer(("0.0.0.0", config['client']['port']), allow_none=True)
    server_instance.register_function(receive_request, "receive_request")
    server_instance.register_function(receive_reply, "receive_reply")
    server_instance.register_function(get_time_for_berkeley, "get_time_for_berkeley")
    server_instance.register_function(adjust_time, "adjust_time")
    
    server_thread = threading.Thread(target=server_instance.serve_forever, daemon=True)
    server_thread.start()
    print(f"[{get_formatted_time()}] [CLIENT-RA] XML-RPC server running...")

def run():
    global clock_offset
    clock_offset = input_initial_time()
    
    print(f"[{get_formatted_time()}] Starting client with Ricart-Agrawala Mutual Exclusion...")
    print(f"[{get_formatted_time()}] [CLIENT-RA] Students S1({copying_students[0]}), S2({copying_students[1]}), S3({copying_students[2]}) will use CS to copy")
    
    run_client_server()
    
    try:
        result = server.register(client_url, teacher_url)
        print(f"[{get_formatted_time()}] Registration result: {result}")
    except Exception as e:
        print(f"[{get_formatted_time()}] Registration failed: {e}")
        exit(1)
    
    # Berkeley Clock Synchronization FIRST
    berkeley_sync()
    
    # Then start test
    try:
        teacher.run_timer(test_duration_sec)
        start_time_str = teacher.get_test_start_time()
        start_time = datetime.fromisoformat(start_time_str).replace(tzinfo=None)
        current_time = get_local_time().replace(tzinfo=None)
        delay = (start_time - current_time).total_seconds()
        
        if delay > 0:
            print(f"[{get_formatted_time()}] Waiting for test to start in {delay:.2f} seconds...")
            time.sleep(delay)
        
        while True:
            try:
                if not teacher.is_test_active():
                    break
                if not send_student():
                    break
                time.sleep(9)
            except Exception as e:
                print(f"[{get_formatted_time()}] Error in main loop: {e}")
                break
        
        print(f"[{get_formatted_time()}] Test finished.")
        
    except Exception as e:
        print(f"[{get_formatted_time()}] Failed to run test: {e}")

if __name__ == '__main__':
    run()
