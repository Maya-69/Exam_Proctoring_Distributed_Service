from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from xmlrpc.client import ServerProxy
import threading
import time
from datetime import datetime, timedelta, timezone
import queue
import socket
import random
import xmlrpc.client

# Student data with enhanced marksheet system (PRESERVED FROM BASE)
students = {
    29: {"name": "Mayuresh", "marks": 100, "reason": "", "status": "ACTIVE"},
    40: {"name": "Ayush", "marks": 100, "reason": "", "status": "ACTIVE"},
    42: {"name": "Aashna", "marks": 100, "reason": "", "status": "ACTIVE"},
    50: {"name": "Rohit", "marks": 100, "reason": "", "status": "ACTIVE"},
    52: {"name": "Rushikesh", "marks": 100, "reason": "", "status": "ACTIVE"},
}

# Global variables (PRESERVED FROM BASE)
clock_offset = None
test_active = False
start_utc = None
client_url = ""
server_url = ""
self_url = ""

# Mutual Exclusion Variables for Ricart-Agrawala
logical_clock = 0
request_queue = queue.PriorityQueue()
deferred_replies = []
requesting_cs = False
in_cs = True  # Teacher initially holds the critical section (marksheet)
num_nodes = 3
replies_received = 0
cs_lock = threading.Lock()
my_process_id = "teacher"

def input_initial_time():
    h, m, s = map(int, input("Enter teacher time (HH MM SS): ").split())
    manual_time = datetime.now(timezone.utc).replace(hour=h, minute=m, second=s, microsecond=0)
    system_utc = datetime.now(timezone.utc).replace(microsecond=0)
    return manual_time - system_utc

def get_formatted_time():
    global clock_offset
    if clock_offset:
        current_time = datetime.now(timezone.utc) + clock_offset
        return current_time.strftime("%d/%b/%Y %H:%M:%S")
    return datetime.now().strftime("%d/%b/%Y %H:%M:%S")

def increment_clock():
    global logical_clock
    logical_clock += 1
    return logical_clock

def update_clock(received_timestamp):
    global logical_clock
    logical_clock = max(logical_clock, received_timestamp) + 1

# Berkeley Clock Sync functions (PRESERVED FROM BASE)
def get_time():
    global clock_offset
    return (datetime.now(timezone.utc) + clock_offset).isoformat()

def adjust_time(offset_sec):
    global clock_offset
    clock_offset += timedelta(seconds=offset_sec)
    print(f"[{get_formatted_time()}] [TEACHER] Time adjusted by {offset_sec:.6f} seconds")
    return True

# Test management functions (PRESERVED FROM BASE)
def get_test_start_time():
    global start_utc
    return start_utc

def run_timer(duration_sec):
    def timer_thread():
        global test_active
        print(f"[{get_formatted_time()}] [TEACHER] Test started for {duration_sec} seconds...")
        time.sleep(duration_sec)
        print(f"[{get_formatted_time()}] [TEACHER] Time's up!")
        test_active = False
        print_results()
    
    global start_utc, test_active
    start_utc = get_time()
    test_active = True
    threading.Thread(target=timer_thread, daemon=True).start()
    return True

def is_test_active():
    return test_active

# Student management functions (PRESERVED FROM BASE WITH ENHANCEMENTS)
def warn_student(roll_no):
    global students
    if roll_no in students:
        print(f"[{get_formatted_time()}] [TEACHER] WARNING: Student {students[roll_no]['name']} (Roll: {roll_no})")
        students[roll_no]["marks"] = 50
        students[roll_no]["reason"] = "Warning"
        students[roll_no]["status"] = "WARNED"
        return True
    else:
        print(f"[{get_formatted_time()}] [TEACHER] Invalid roll number: {roll_no}")
        return False

def catch_student(roll_no):
    global students
    if roll_no in students:
        print(f"[{get_formatted_time()}] [TEACHER] CAUGHT: Student {students[roll_no]['name']} (Roll: {roll_no}) cheating!")
        students[roll_no]["marks"] = 0
        students[roll_no]["reason"] = "Cheating"
        students[roll_no]["status"] = "FAILED"
        return True
    else:
        print(f"[{get_formatted_time()}] [TEACHER] Invalid roll number: {roll_no}")
        return False

# Enhanced Pass/Fail reporting (PRESERVED FROM BASE)
def print_results():
    print(f"\n[{get_formatted_time()}] " + "="*60)
    print(f"[{get_formatted_time()}] FINAL STUDENT MARKSHEET REPORT")
    print(f"[{get_formatted_time()}] " + "="*60)
    header = f"{'Roll No':<8} | {'Name':<12} | {'Marks':<5} | {'Result':<6} | {'Reason':<10}"
    print(f"[{get_formatted_time()}] {header}")
    print(f"[{get_formatted_time()}] " + "-" * len(header))
    
    for roll_no, info in students.items():
        reason = info["reason"] if info["reason"] else "-"
        result = "PASS" if info["marks"] >= 50 else "FAIL"
        print(f"[{get_formatted_time()}] {roll_no:<8} | {info['name']:<12} | {info['marks']:<5} | {result:<6} | {reason:<10}")
    
    print(f"[{get_formatted_time()}] " + "-" * len(header))
    print(f"[{get_formatted_time()}] " + "="*60)
    return True

# Ricart-Agrawala Mutual Exclusion Implementation
def send_request_with_timeout(url, method, *args, timeout=5):
    try:
        proxy = xmlrpc.client.ServerProxy(url + "/RPC2", allow_none=True)
        proxy._ServerProxy__transport.timeout = timeout
        result = getattr(proxy, method)(*args)
        return result
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER-RA] Timeout/Error calling {method}: {e}")
        return False

def request_critical_section():
    global requesting_cs, replies_received, logical_clock
    timestamp = increment_clock()
    print(f"[{get_formatted_time()}] [TEACHER-RA] Requesting Critical Section at logical time {timestamp}")
    
    requesting_cs = True
    replies_received = 0
    
    # Send requests to all other nodes
    try:
        success1 = send_request_with_timeout(client_url, "receive_request", my_process_id, timestamp)
        if success1:
            print(f"[{get_formatted_time()}] [TEACHER-RA] Sent REQUEST to client with timestamp {timestamp}")
        
        success2 = send_request_with_timeout(server_url, "receive_request", my_process_id, timestamp)
        if success2:
            print(f"[{get_formatted_time()}] [TEACHER-RA] Sent REQUEST to server with timestamp {timestamp}")
            
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER-RA] Error sending requests: {e}")
    
    # Wait for replies in separate thread
    threading.Thread(target=wait_for_replies_timeout, args=(timestamp,), daemon=True).start()

def wait_for_replies_timeout(request_timestamp, timeout=10):
    global requesting_cs
    start_time = time.time()
    
    while requesting_cs and replies_received < (num_nodes - 1):
        if time.time() - start_time > timeout:
            print(f"[{get_formatted_time()}] [TEACHER-RA] Timeout waiting for replies, entering CS anyway")
            break
        time.sleep(0.1)
    
    if requesting_cs:
        enter_critical_section()

def receive_request(sender_id, timestamp):
    global logical_clock, deferred_replies, requesting_cs, in_cs
    update_clock(timestamp)
    print(f"[{get_formatted_time()}] [TEACHER-RA] Received REQUEST from {sender_id} with timestamp {timestamp}, my clock: {logical_clock}")
    
    should_reply = True
    if requesting_cs or in_cs:
        if timestamp > logical_clock or (timestamp == logical_clock and sender_id > my_process_id):
            should_reply = False
            deferred_replies.append(sender_id)
            print(f"[{get_formatted_time()}] [TEACHER-RA] Deferring reply to {sender_id} (currently in/requesting CS)")
    
    if should_reply:
        send_reply(sender_id)
    
    return True

def send_reply(receiver_id):
    global logical_clock
    timestamp = increment_clock()
    print(f"[{get_formatted_time()}] [TEACHER-RA] Sending REPLY to {receiver_id} with timestamp {timestamp}")
    
    try:
        if receiver_id == "client":
            success = send_request_with_timeout(client_url, "receive_reply", my_process_id, timestamp)
        elif receiver_id == "server":
            success = send_request_with_timeout(server_url, "receive_reply", my_process_id, timestamp)
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER-RA] Error sending reply: {e}")

def receive_reply(sender_id, timestamp):
    global replies_received, logical_clock, requesting_cs
    update_clock(timestamp)
    print(f"[{get_formatted_time()}] [TEACHER-RA] Received REPLY from {sender_id} with timestamp {timestamp}, my clock: {logical_clock}")
    
    if requesting_cs:
        replies_received += 1
        print(f"[{get_formatted_time()}] [TEACHER-RA] Replies received: {replies_received}/{num_nodes-1}")
    
    return True

def enter_critical_section():
    global in_cs, requesting_cs
    print(f"[{get_formatted_time()}] [TEACHER-RA] *** ENTERING CRITICAL SECTION (MARKSHEET ACCESS) ***")
    print(f"[{get_formatted_time()}] [TEACHER-RA] Teacher accessing marksheet for grading...")
    
    in_cs = True
    requesting_cs = False
    
    # Simulate critical section work
    time.sleep(3)
    print(f"[{get_formatted_time()}] [TEACHER-RA] Marksheet grading operations completed")
    
    exit_critical_section()

def exit_critical_section():
    global in_cs, deferred_replies
    print(f"[{get_formatted_time()}] [TEACHER-RA] *** EXITING CRITICAL SECTION (MARKSHEET ACCESS) ***")
    
    in_cs = False
    
    # Send deferred replies
    for receiver_id in deferred_replies:
        send_reply(receiver_id)
        print(f"[{get_formatted_time()}] [TEACHER-RA] Sent deferred reply to {receiver_id}")
    
    deferred_replies.clear()

# Registration function for URLs
def register_urls(client, server, self_):
    global client_url, server_url, self_url
    client_url = client
    server_url = server
    self_url = self_
    print(f"[{get_formatted_time()}] [TEACHER] URLs registered - Client: {client}, Server: {server}")
    return True

# Test function to release CS (for testing)
def release_critical_section():
    global in_cs
    if in_cs:
        print(f"[{get_formatted_time()}] [TEACHER-RA] Manually releasing critical section...")
        exit_critical_section()
        return "Critical section released"
    else:
        return "Not currently in critical section"

def run_teacher():
    global clock_offset
    clock_offset = input_initial_time()
    
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print(f"[{get_formatted_time()}] [TEACHER] Starting Teacher with Ricart-Agrawala Mutual Exclusion...")
    print(f"[{get_formatted_time()}] [TEACHER] Teacher hostname: {hostname}")
    print(f"[{get_formatted_time()}] [TEACHER] Teacher IP address: {local_ip}")
    print(f"[{get_formatted_time()}] [TEACHER] Teacher server running at port 9001")
    print(f"[{get_formatted_time()}] [TEACHER] Teacher accessible at: http://{local_ip}:9001")
    print(f"[{get_formatted_time()}] [TEACHER-RA] Initially holding Critical Section (marksheet)")
    print(f"[{get_formatted_time()}] [TEACHER-RA] Ricart-Agrawala algorithm initialized, process ID: {my_process_id}")
    
    with SimpleXMLRPCServer(("0.0.0.0", 9001),
                           requestHandler=SimpleXMLRPCRequestHandler,
                           allow_none=True) as server:
        
        # Register baseline functions (PRESERVED)
        server.register_function(get_time, "get_time")
        server.register_function(adjust_time, "adjust_time")
        server.register_function(get_test_start_time, "get_test_start_time")
        server.register_function(warn_student, "warn_student")
        server.register_function(catch_student, "catch_student")
        server.register_function(print_results, "print_results")
        server.register_function(run_timer, "run_timer")
        server.register_function(is_test_active, "is_test_active")
        server.register_function(register_urls, "register_urls")
        
        # Register Ricart-Agrawala functions
        server.register_function(receive_request, "receive_request")
        server.register_function(receive_reply, "receive_reply")
        server.register_function(release_critical_section, "release_critical_section")
        
        print(f"[{get_formatted_time()}] [TEACHER] All functions registered:")
        print(f"[{get_formatted_time()}] [TEACHER] - Baseline: get_time, adjust_time, warn_student, catch_student, print_results")
        print(f"[{get_formatted_time()}] [TEACHER] - Test management: run_timer, is_test_active, get_test_start_time")
        print(f"[{get_formatted_time()}] [TEACHER] - Ricart-Agrawala: receive_request, receive_reply, release_critical_section")
        print(f"[{get_formatted_time()}] [TEACHER] Ready to handle requests...")
        
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print(f"\n[{get_formatted_time()}] [TEACHER] Teacher server shutting down...")

if __name__ == "__main__":
    run_teacher()
