from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
from datetime import datetime, timedelta, timezone
import queue
import threading
import time
import socket
import random

# Global variables
time_now = None
clients = {}
client_url = ""
teacher_url = ""

# Original baseline functionality (PRESERVED)
student_status = {i: 0 for i in [29, 40, 42, 50, 52]}
teacher_proxy = None

# Mutual Exclusion Variables for Ricart-Agrawala
logical_clock = 0
request_queue = queue.PriorityQueue()
deferred_replies = []
requesting_cs = False
in_cs = False
num_nodes = 3
replies_received = 0
cs_lock = threading.Lock()
my_process_id = "server"

def input_time():
    h, m, s = map(int, input("Enter server time (HH MM SS): ").split())
    now = datetime.now().replace(hour=h, minute=m, second=s, microsecond=0)
    return now

def get_formatted_time():
    global time_now
    if time_now:
        return time_now.strftime("%d/%b/%Y %H:%M:%S")
    return datetime.now().strftime("%d/%b/%Y %H:%M:%S")

def increment_clock():
    global logical_clock
    logical_clock += 1
    return logical_clock

def update_clock(received_timestamp):
    global logical_clock
    logical_clock = max(logical_clock, received_timestamp) + 1

# Registration function (PRESERVED FROM BASE)
def register(client, teacher):
    global client_url, teacher_url, teacher_proxy
    client_url = client
    teacher_url = teacher
    
    print(f"[{get_formatted_time()}] [SERVER] Registering client: {client}")
    print(f"[{get_formatted_time()}] [SERVER] Registering teacher: {teacher}")
    
    try:
        clients["client"] = xmlrpc.client.ServerProxy(client + "/RPC2", allow_none=True)
        clients["teacher"] = xmlrpc.client.ServerProxy(teacher + "/RPC2", allow_none=True)
        teacher_proxy = clients["teacher"]
        print(f"[{get_formatted_time()}] [SERVER] Client and teacher registered successfully")
        return "Registered client and teacher."
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER] Registration failed: {e}")
        return f"Registration failed: {e}"

# Berkeley Clock Sync functions (PRESERVED FROM BASE)
def get_time():
    global time_now
    return time_now.isoformat()

def adjust_time(offset):
    global time_now
    time_now += timedelta(seconds=offset)
    print(f"[{get_formatted_time()}] [SERVER] Time adjusted by {offset:.6f} seconds")
    return True

# Enhanced student status update (PRESERVED FROM BASE LOGIC)
def update_status(roll_no, status):
    global student_status, teacher_proxy
    print(f"[{get_formatted_time()}] [SERVER] Updating status for student {roll_no}: {status}")
    
    if roll_no not in student_status:
        student_status[roll_no] = 0
    
    student_status[roll_no] = status
    
    try:
        if status == 1:
            result = teacher_proxy.warn_student(roll_no)
            print(f"[{get_formatted_time()}] [SERVER] Student {roll_no} warned (first offense)")
            return "Student warned - marks reduced to 50"
        elif status == 2:
            result = teacher_proxy.catch_student(roll_no)
            print(f"[{get_formatted_time()}] [SERVER] Student {roll_no} caught cheating (second offense)")
            return "Student caught - marks set to 0 (FAILED)"
        else:
            return "Status updated"
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER] Error communicating with teacher: {e}")
        return f"Error: {e}"

# Ricart-Agrawala Mutual Exclusion Implementation
def send_request_with_timeout(url, method, *args, timeout=5):
    try:
        proxy = xmlrpc.client.ServerProxy(url + "/RPC2", allow_none=True)
        proxy._ServerProxy__transport.timeout = timeout
        result = getattr(proxy, method)(*args)
        return result
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-RA] Timeout/Error calling {method}: {e}")
        return False

def request_critical_section():
    global requesting_cs, replies_received, logical_clock
    timestamp = increment_clock()
    print(f"[{get_formatted_time()}] [SERVER-RA] Requesting Critical Section at logical time {timestamp}")
    
    requesting_cs = True
    replies_received = 0
    
    # Send requests to all other nodes
    try:
        success1 = send_request_with_timeout(client_url, "receive_request", my_process_id, timestamp)
        if success1:
            print(f"[{get_formatted_time()}] [SERVER-RA] Sent REQUEST to client with timestamp {timestamp}")
        
        success2 = send_request_with_timeout(teacher_url, "receive_request", my_process_id, timestamp)
        if success2:
            print(f"[{get_formatted_time()}] [SERVER-RA] Sent REQUEST to teacher with timestamp {timestamp}")
            
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-RA] Error sending requests: {e}")
    
    # Wait for replies in separate thread
    threading.Thread(target=wait_for_replies_timeout, args=(timestamp,), daemon=True).start()

def wait_for_replies_timeout(request_timestamp, timeout=10):
    global requesting_cs
    start_time = time.time()
    
    while requesting_cs and replies_received < (num_nodes - 1):
        if time.time() - start_time > timeout:
            print(f"[{get_formatted_time()}] [SERVER-RA] Timeout waiting for replies, entering CS anyway")
            break
        time.sleep(0.1)
    
    if requesting_cs:
        enter_critical_section()

def receive_request(sender_id, timestamp):
    global logical_clock, deferred_replies, requesting_cs, in_cs
    update_clock(timestamp)
    print(f"[{get_formatted_time()}] [SERVER-RA] Received REQUEST from {sender_id} with timestamp {timestamp}, my clock: {logical_clock}")
    
    should_reply = True
    if requesting_cs or in_cs:
        if timestamp > logical_clock or (timestamp == logical_clock and sender_id > my_process_id):
            should_reply = False
            deferred_replies.append(sender_id)
            print(f"[{get_formatted_time()}] [SERVER-RA] Deferring reply to {sender_id} (priority conflict)")
    
    if should_reply:
        send_reply(sender_id)
    
    return True

def send_reply(receiver_id):
    global logical_clock
    timestamp = increment_clock()
    print(f"[{get_formatted_time()}] [SERVER-RA] Sending REPLY to {receiver_id} with timestamp {timestamp}")
    
    try:
        if receiver_id == "client":
            success = send_request_with_timeout(client_url, "receive_reply", my_process_id, timestamp)
        elif receiver_id == "teacher":
            success = send_request_with_timeout(teacher_url, "receive_reply", my_process_id, timestamp)
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-RA] Error sending reply: {e}")

def receive_reply(sender_id, timestamp):
    global replies_received, logical_clock, requesting_cs
    update_clock(timestamp)
    print(f"[{get_formatted_time()}] [SERVER-RA] Received REPLY from {sender_id} with timestamp {timestamp}, my clock: {logical_clock}")
    
    if requesting_cs:
        replies_received += 1
        print(f"[{get_formatted_time()}] [SERVER-RA] Replies received: {replies_received}/{num_nodes-1}")
    
    return True

def enter_critical_section():
    global in_cs, requesting_cs
    print(f"[{get_formatted_time()}] [SERVER-RA] *** ENTERING CRITICAL SECTION (MARKSHEET MANAGEMENT) ***")
    print(f"[{get_formatted_time()}] [SERVER-RA] Server managing marksheet database...")
    
    in_cs = True
    requesting_cs = False
    
    # Simulate critical section work
    time.sleep(2)
    print(f"[{get_formatted_time()}] [SERVER-RA] Marksheet database operations completed")
    
    exit_critical_section()

def exit_critical_section():
    global in_cs, deferred_replies
    print(f"[{get_formatted_time()}] [SERVER-RA] *** EXITING CRITICAL SECTION (MARKSHEET MANAGEMENT) ***")
    
    in_cs = False
    
    # Send deferred replies
    for receiver_id in deferred_replies:
        send_reply(receiver_id)
        print(f"[{get_formatted_time()}] [SERVER-RA] Sent deferred reply to {receiver_id}")
    
    deferred_replies.clear()

# Test function to trigger CS access
def trigger_cs_access():
    print(f"[{get_formatted_time()}] [SERVER-RA] Server needs to access marksheet database")
    request_critical_section()
    return "Critical section access initiated"

def run_server():
    global time_now
    time_now = input_time()
    
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print(f"[{get_formatted_time()}] [SERVER] Starting Server with Ricart-Agrawala Mutual Exclusion...")
    print(f"[{get_formatted_time()}] [SERVER] Server hostname: {hostname}")
    print(f"[{get_formatted_time()}] [SERVER] Server IP address: {local_ip}")
    print(f"[{get_formatted_time()}] [SERVER] Server running on port 8000")
    print(f"[{get_formatted_time()}] [SERVER] Server accessible at: http://{local_ip}:8000")
    print(f"[{get_formatted_time()}] [SERVER-RA] Ricart-Agrawala algorithm initialized, process ID: {my_process_id}")
    
    server = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True)
    
    # Register baseline functions (PRESERVED)
    server.register_function(register, "register")
    server.register_function(get_time, "get_time")
    server.register_function(adjust_time, "adjust_time")
    server.register_function(update_status, "update_status")
    
    # Register Ricart-Agrawala functions
    server.register_function(receive_request, "receive_request")
    server.register_function(receive_reply, "receive_reply")
    server.register_function(trigger_cs_access, "trigger_cs_access")
    
    print(f"[{get_formatted_time()}] [SERVER] All functions registered:")
    print(f"[{get_formatted_time()}] [SERVER] - Baseline: register, get_time, adjust_time, update_status")
    print(f"[{get_formatted_time()}] [SERVER] - Ricart-Agrawala: receive_request, receive_reply, trigger_cs_access")
    print(f"[{get_formatted_time()}] [SERVER] Ready to handle requests...")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{get_formatted_time()}] [SERVER] Server shutting down...")

if __name__ == "__main__":
    run_server()
