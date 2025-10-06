from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
from datetime import datetime, timedelta, timezone
import queue
import threading
import time
import socket
import random
import sqlite3
import json
import os

# Global variables - PRESERVED FROM BASE
time_now = None
clients = {}
client_url = ""
teacher_url = ""
student_status = {i: 0 for i in [29, 40, 42, 50, 52]}
teacher_proxy = None

# Mutual Exclusion Variables for Ricart-Agrawala - PRESERVED FROM BASE
logical_clock = 0
request_queue = queue.PriorityQueue()
deferred_replies = []
requesting_cs = False
in_cs = False
num_nodes = 3 
replies_received = 0
cs_lock = threading.Lock()
my_process_id = "server"

# TASK 6: NEW Deadlock Exam System Variables
DB_PATH = "exam_system.db"
db_lock = threading.Lock()
exam_timer = 10  # 5 minutes as specified in Task 6

# Task 6 Exam Questions (10 MCQs)
exam_questions = [
    {"id": 1, "q": "What is a distributed system?", "options": ["A) Single processor system", "B) Collection of independent computers", "C) Database system", "D) Network protocol"], "correct": "B"},
    {"id": 2, "q": "Which algorithm ensures mutual exclusion?", "options": ["A) Dijkstra's algorithm", "B) Ricart-Agrawala algorithm", "C) Bubble sort", "D) Linear search"], "correct": "B"},
    {"id": 3, "q": "What is the purpose of Berkeley algorithm?", "options": ["A) Clock synchronization", "B) Process scheduling", "C) Memory management", "D) File system"], "correct": "A"},
    {"id": 4, "q": "What causes deadlock in distributed systems?", "options": ["A) Fast processors", "B) Circular wait for resources", "C) Too much memory", "D) Network speed"], "correct": "B"},
    {"id": 5, "q": "What is a critical section?", "options": ["A) Code that crashes", "B) Code accessed by multiple processes", "C) Fast executing code", "D) Error handling code"], "correct": "B"},
    {"id": 6, "q": "Which is NOT a distributed system characteristic?", "options": ["A) Transparency", "B) Scalability", "C) Single point of failure", "D) Fault tolerance"], "correct": "C"},
    {"id": 7, "q": "What is the main goal of load balancing?", "options": ["A) Reduce system cost", "B) Distribute workload evenly", "C) Increase memory", "D) Faster processors"], "correct": "B"},
    {"id": 8, "q": "Which protocol is used for reliable message delivery?", "options": ["A) UDP", "B) TCP", "C) ICMP", "D) ARP"], "correct": "B"},
    {"id": 9, "q": "What is replication in distributed systems?", "options": ["A) Copying data to multiple locations", "B) Deleting old data", "C) Compressing data", "D) Encrypting data"], "correct": "A"},
    {"id": 10, "q": "What is the CAP theorem about?", "options": ["A) Computer performance", "B) Consistency, Availability, Partition tolerance", "C) Network cables", "D) Database size"], "correct": "B"}
]

# TASK 6: Deadlock Resolution Variables
exam_sessions = {}
submission_lock = threading.Lock()
deadlock_detection_lock = threading.Lock()
status_db = {}      # Status Database for Task 6
submission_db = {}  # Submission Database for Task 6

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

# TASK 6: Database Management System
def init_database():
    with db_lock:
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Status Database (Task 6 requirement)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS status_db (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    student_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    exam_type TEXT DEFAULT 'Interactive'
                )
            ''')
            
            # Submission Database (Task 6 requirement)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS submission_db (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    student_id INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    submission_type TEXT NOT NULL,
                    deadlock_detected BOOLEAN DEFAULT FALSE,
                    resolution_strategy TEXT,
                    submission_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    answers TEXT,
                    exam_duration INTEGER
                )
            ''')
            
            # Deadlock events table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS deadlock_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    student_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    manual_attempt_time TIMESTAMP,
                    auto_trigger_time TIMESTAMP,
                    resolution_strategy TEXT,
                    winner TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            
            print(f"[{get_formatted_time()}] [SERVER-DB] Task 6 databases initialized")
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER-DB] Database initialization failed: {e}")

def attempt_auto_submission(student_id):
    print(f"[{get_formatted_time()}] [SERVER-TASK6] AUTO-SUBMISSION triggered for student {student_id}")
    
    if student_id not in exam_sessions:
        print(f"[{get_formatted_time()}] [SERVER-TASK6] No active session for auto-submission")
        return
    
    session = exam_sessions[student_id]
    
    if session.get("manual_submission_attempted", False):
        print(f"[{get_formatted_time()}] [SERVER-TASK6] Manual submission already attempted - auto-submission cancelled")
        return
    
    try:
        clients["client"].handle_exam_timeout(student_id)
        print(f"[{get_formatted_time()}] [SERVER-TASK6] Notified client of auto-submission")
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK6] Failed to notify client: {e}")
    
    return submit_exam_final(student_id, "auto")



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
        
        # Test connections
        test_client = clients["client"].get_time_for_berkeley()
        test_teacher = clients["teacher"].get_time()
        
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

# TASK 6: Interactive Exam System with Deadlock Resolution
def start_interactive_exam(student_id):
    global exam_sessions, status_db
    
    print(f"[{get_formatted_time()}] [SERVER-TASK6] Starting interactive exam for student {student_id}")
    
    if student_id in exam_sessions:
        return {"success": False, "message": "Exam already in progress for this student"}
    
    # Initialize exam session
    exam_session = {
        "student_id": student_id,
        "questions": exam_questions.copy(),
        "current_question": 0,
        "answers": [],
        "score": 0,
        "start_time": time.time(),
        "duration": exam_timer,  # 5 minutes
        "status": "ACTIVE",
        "auto_submit_scheduled": False,
        "manual_submission_attempted": False,
        "deadlock_detected": False
    }
    
    exam_sessions[student_id] = exam_session
    
    # Update Status DB (Task 6 requirement)
    status_db[student_id] = {
        "status": "taking_exam",
        "timestamp": datetime.now(),
        "student_id": student_id
    }
    
    # Store in database
    with db_lock:
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO status_db (student_id, status, exam_type) 
                VALUES (?, ?, ?)
            ''', (student_id, "taking_exam", "Interactive"))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER-DB] Failed to update status DB: {e}")
    
    # Schedule auto-submission timer
    def auto_submit_timer():
        time.sleep(exam_session["duration"])
        if student_id in exam_sessions and exam_sessions[student_id]["status"] == "ACTIVE":
            print(f"[{get_formatted_time()}] [SERVER-TASK6] Auto-submit timer expired for student {student_id}")
            attempt_auto_submission(student_id)
    
    exam_session["auto_submit_scheduled"] = True
    threading.Thread(target=auto_submit_timer, daemon=True).start()
    
    print(f"[{get_formatted_time()}] [SERVER-TASK6] Interactive exam initialized with 5-minute timer")
    
    return {
        "success": True,
        "duration": exam_session["duration"],
        "total_questions": len(exam_questions),
        "message": "Task 6 Interactive exam started successfully"
    }

def get_question(student_id):
    if student_id not in exam_sessions:
        return {"success": False, "message": "No active exam session"}
    
    session = exam_sessions[student_id]
    elapsed_time = time.time() - session["start_time"]
    remaining_time = max(0, session["duration"] - elapsed_time)
    
    if remaining_time <= 0:
        return {"success": False, "time_expired": True, "message": "Time expired"}
    
    if session["current_question"] >= len(session["questions"]):
        return {"success": False, "completed": True, "message": "All questions answered"}
    
    current_q = session["questions"][session["current_question"]]
    
    return {
        "success": True,
        "question": current_q,
        "question_number": session["current_question"] + 1,
        "total_questions": len(session["questions"]),
        "remaining_time": remaining_time
    }

def submit_answer(student_id, answer):
    if student_id not in exam_sessions:
        return {"success": False, "message": "No active exam session"}
    
    session = exam_sessions[student_id]
    elapsed_time = time.time() - session["start_time"]
    remaining_time = max(0, session["duration"] - elapsed_time)
    
    if remaining_time <= 0:
        return {"success": False, "time_expired": True, "message": "Time expired"}
    
    if session["current_question"] >= len(session["questions"]):
        return {"success": False, "message": "No more questions"}
    
    current_q = session["questions"][session["current_question"]]
    is_correct = (answer == current_q["correct"])
    
    if is_correct:
        session["score"] += 1
    
    session["answers"].append({
        "question_id": current_q["id"],
        "user_answer": answer,
        "correct_answer": current_q["correct"],
        "is_correct": is_correct
    })
    
    session["current_question"] += 1
    
    all_completed = session["current_question"] >= len(session["questions"])
    
    return {
        "success": True,
        "is_correct": is_correct,
        "correct_answer": current_q["correct"],
        "remaining_time": remaining_time,
        "all_completed": all_completed
    }

def submit_exam_final(student_id, submission_source="manual"):
    """TASK 6: Final submission with deadlock detection and resolution"""
    print(f"[{get_formatted_time()}] [SERVER-TASK6] FINAL SUBMISSION ATTEMPT")
    print(f"[{get_formatted_time()}] [SERVER-TASK6] Student: {student_id}, Source: {submission_source}")
    
    with deadlock_detection_lock:
        if student_id not in exam_sessions:
            return {"success": False, "message": "No active exam session"}
        
        session = exam_sessions[student_id]
        elapsed_time = time.time() - session["start_time"]
        remaining_time = max(0, session["duration"] - elapsed_time)
        
        # TASK 6: DEADLOCK DETECTION LOGIC
        deadlock_detected = False
        resolution_strategy = "none"
        
        if submission_source == "manual":
            session["manual_submission_attempted"] = True
            
            # Check if we're very close to auto-submission time
            if remaining_time <= 1.0:  # Within 1 second - potential deadlock
                print(f"[{get_formatted_time()}] [SERVER-TASK6] *** DEADLOCK DETECTED! ***")
                print(f"[{get_formatted_time()}] [SERVER-TASK6] Manual submission attempted with {remaining_time:.2f}s remaining")
                print(f"[{get_formatted_time()}] [SERVER-TASK6] Auto-submission timer about to trigger")
                
                deadlock_detected = True
                session["deadlock_detected"] = True
                
                # TASK 6: Deadlock Resolution Strategy - Manual submission takes priority
                resolution_strategy = "manual_priority"
                print(f"[{get_formatted_time()}] [SERVER-TASK6] RESOLUTION: Manual submission takes priority")
                
                # Log deadlock event
                with db_lock:
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO deadlock_events 
                            (student_id, event_type, manual_attempt_time, resolution_strategy, winner)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (student_id, "simultaneous_submission", datetime.now(), 
                              resolution_strategy, "manual"))
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        print(f"[{get_formatted_time()}] [SERVER-DB] Failed to log deadlock: {e}")
        
        # Process submission
        final_score = session["score"]
        submission_type = "manual" if submission_source == "manual" else "auto"
        
        # Update Status DB and Submission DB (Task 6 requirements)
        update_task6_databases(student_id, final_score, submission_type, deadlock_detected, resolution_strategy, session)
        
        # Clean up session
        del exam_sessions[student_id]
        
        # Notify teacher
        try:
            clients["teacher"].receive_exam_submission(student_id, final_score, submission_type)
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER-TASK6] Failed to notify teacher: {e}")
        
        return {
            "success": True,
            "score": final_score,
            "type": submission_type,
            "deadlock_resolved": deadlock_detected,
            "resolution_strategy": resolution_strategy,
            "message": f"Task 6 exam submitted - {submission_type}"
        }

def attempt_auto_submission(student_id):
    """Handle automatic submission when timer expires"""
    print(f"[{get_formatted_time()}] [SERVER-TASK6] AUTO-SUBMISSION triggered for student {student_id}")
    
    if student_id not in exam_sessions:
        print(f"[{get_formatted_time()}] [SERVER-TASK6] No active session for auto-submission")
        return
    
    session = exam_sessions[student_id]
    
    if session.get("manual_submission_attempted", False):
        print(f"[{get_formatted_time()}] [SERVER-TASK6] Manual submission already attempted - auto-submission cancelled")
        return
    
    # Proceed with auto-submission
    return submit_exam_final(student_id, "auto")

def update_task6_databases(student_id, score, submission_type, deadlock_detected, resolution_strategy, session):
    """Update Task 6 required databases"""
    with db_lock:
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Update Status DB
            cursor.execute('''
                INSERT INTO status_db (student_id, status, exam_type) 
                VALUES (?, ?, ?)
            ''', (student_id, "exam_submitted", "Interactive"))
            
            # Update Submission DB
            answers_json = json.dumps(session["answers"])
            cursor.execute('''
                INSERT INTO submission_db 
                (student_id, score, submission_type, deadlock_detected, resolution_strategy, answers, exam_duration)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (student_id, score, submission_type, deadlock_detected, 
                  resolution_strategy, answers_json, session["duration"]))
            
            conn.commit()
            conn.close()
            
            print(f"[{get_formatted_time()}] [SERVER-TASK6] Status and Submission databases updated")
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER-DB] Failed to update Task 6 databases: {e}")

# Ricart-Agrawala functions (PRESERVED FROM BASE)
def send_request_with_timeout(url, method, *args, timeout=5):
    try:
        proxy = xmlrpc.client.ServerProxy(url + "/RPC2", allow_none=True)
        proxy._ServerProxy__transport.timeout = timeout
        result = getattr(proxy, method)(*args)
        return result
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-RA] Timeout/Error calling {method}: {e}")
        return False

def receive_request(sender_id, timestamp):
    global logical_clock, deferred_replies, requesting_cs, in_cs
    update_clock(timestamp)
    print(f"[{get_formatted_time()}] [SERVER-RA] Received REQUEST from {sender_id} with timestamp {timestamp}, my clock: {logical_clock}")
    
    should_reply = True
    if requesting_cs or in_cs:
        if timestamp > logical_clock or (timestamp == logical_clock and sender_id > my_process_id):
            should_reply = False
            deferred_replies.append(sender_id)
            print(f"[{get_formatted_time()}] [SERVER-RA] Deferring reply to {sender_id}")
    
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

def run_server():
    global time_now
    time_now = input_time()
    
    # Initialize Task 6 databases
    init_database()
    
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print(f"[{get_formatted_time()}] [SERVER] Starting Server with Task 6: Deadlock in Exam Grid...")
    print(f"[{get_formatted_time()}] [SERVER] Features: Berkeley Sync, Ricart-Agrawala, Task 6 Deadlock Resolution")
    print(f"[{get_formatted_time()}] [SERVER] Task 6: Interactive Exam with 5-minute timer and deadlock handling")
    print(f"[{get_formatted_time()}] [SERVER] Databases: Status DB, Submission DB, Deadlock Events")
    print(f"[{get_formatted_time()}] [SERVER] Server hostname: {hostname}")
    print(f"[{get_formatted_time()}] [SERVER] Server IP address: {local_ip}")
    print(f"[{get_formatted_time()}] [SERVER] Server running on port 8000")
    print(f"[{get_formatted_time()}] [SERVER-RA] Ricart-Agrawala algorithm initialized, process ID: {my_process_id}")
    
    server = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True)
    
    # Register baseline functions (PRESERVED)
    server.register_function(register, "register")
    server.register_function(get_time, "get_time")
    server.register_function(adjust_time, "adjust_time")
    server.register_function(update_status, "update_status")
    
    # Register Ricart-Agrawala functions (PRESERVED)
    server.register_function(receive_request, "receive_request")
    server.register_function(receive_reply, "receive_reply")
    
    # Register TASK 6: Deadlock Exam functions
    server.register_function(start_interactive_exam, "start_interactive_exam")
    server.register_function(get_question, "get_question")
    server.register_function(submit_answer, "submit_answer")
    server.register_function(submit_exam_final, "submit_exam_final")
    
    print(f"[{get_formatted_time()}] [SERVER] All functions registered:")
    print(f"[{get_formatted_time()}] [SERVER] - Baseline: register, get_time, adjust_time, update_status")
    print(f"[{get_formatted_time()}] [SERVER] - Ricart-Agrawala: receive_request, receive_reply")
    print(f"[{get_formatted_time()}] [SERVER] - TASK 6 Deadlock Exam: start_interactive_exam, get_question, submit_answer, submit_exam_final")
    print(f"[{get_formatted_time()}] [SERVER] Ready to handle requests...")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{get_formatted_time()}] [SERVER] Server shutting down...")

if __name__ == "__main__":
    run_server()
