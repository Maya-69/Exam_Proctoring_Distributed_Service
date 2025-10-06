from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from xmlrpc.client import ServerProxy
import threading
import time
from datetime import datetime, timedelta, timezone
import queue
import socket
import random
import xmlrpc.client
import sqlite3
import json

students = {
    29: {"name": "Mayuresh", "marks": 100, "reason": "", "status": "ACTIVE"},
    40: {"name": "Ayush", "marks": 100, "reason": "", "status": "ACTIVE"},
    42: {"name": "Aashna", "marks": 100, "reason": "", "status": "ACTIVE"},
    50: {"name": "Rohit", "marks": 100, "reason": "", "status": "ACTIVE"},
    52: {"name": "Rushikesh", "marks": 100, "reason": "", "status": "ACTIVE"},
}

clock_offset = None
test_active = False
start_utc = None
client_url = ""
server_url = ""
self_url = ""

logical_clock = 0
request_queue = queue.PriorityQueue()
deferred_replies = []
requesting_cs = False
in_cs = True
num_nodes = 3
replies_received = 0
cs_lock = threading.Lock()
my_process_id = "teacher"

exam_results = {}
interactive_exam_active = False
DB_PATH = "exam_system.db"
db_lock = threading.Lock()

load_balancer_stats = {}
total_exam_submissions = 0
main_server_submissions = 0
backup_server_submissions = 0

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

def get_time():
    global clock_offset
    return (datetime.now(timezone.utc) + clock_offset).isoformat()

def adjust_time(offset_sec):
    global clock_offset
    clock_offset += timedelta(seconds=offset_sec)
    print(f"[{get_formatted_time()}] [TEACHER] Time adjusted by {offset_sec:.6f} seconds")
    return True

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

def print_results():
    print(f"\n[{get_formatted_time()}] " + "="*120)
    print(f"[{get_formatted_time()}] COMPREHENSIVE EXAM RESULTS - ALL TASKS INCLUDED")
    print(f"[{get_formatted_time()}] " + "="*120)
    
    print(f"[{get_formatted_time()}] TRADITIONAL CHEATING DETECTION RESULTS:")
    print(f"[{get_formatted_time()}] " + "-"*80)
    
    header = f"{'Roll':<6} | {'Name':<12} | {'Marks':<6} | {'Result':<6} | {'Status':<8} | {'Reason':<12}"
    print(f"[{get_formatted_time()}] {header}")
    print(f"[{get_formatted_time()}] " + "-" * len(header))
    
    traditional_students = []
    for roll_no, info in students.items():
        if roll_no not in exam_results:
            traditional_students.append(roll_no)
            result = "PASS" if info["marks"] >= 50 else "FAIL"
            reason = info["reason"] if info["reason"] else "-"
            
            print(f"[{get_formatted_time()}] {roll_no:<6} | {info['name']:<12} | {info['marks']:<6} | {result:<6} | {info['status']:<8} | {reason:<12}")
    
    print(f"[{get_formatted_time()}] " + "-" * len(header))
    
    if exam_results:
        print(f"\n[{get_formatted_time()}] TASK 6: INTERACTIVE DEADLOCK EXAM RESULTS:")
        print(f"[{get_formatted_time()}] " + "-"*100)
        
        interactive_header = f"{'Roll':<6} | {'Name':<12} | {'Score':<6} | {'Marks':<6} | {'Result':<6} | {'Type':<8} | {'Deadlock':<8} | {'Server':<8}"
        print(f"[{get_formatted_time()}] {interactive_header}")
        print(f"[{get_formatted_time()}] " + "-" * len(interactive_header))
        
        for student_id, exam_data in exam_results.items():
            student_name = students.get(student_id, {}).get('name', 'Unknown')
            raw_score = exam_data['score']
            final_marks = raw_score * 10
            result = "PASS" if final_marks >= 50 else "FAIL"
            submission_type = exam_data['type']
            deadlock_status = "YES" if exam_data.get('deadlock_detected', False) else "NO"
            server_used = exam_data.get('server_used', 'main').upper()
            
            print(f"[{get_formatted_time()}] {student_id:<6} | {student_name:<12} | {raw_score:<6}/10 | {final_marks:<6}/100 | {result:<6} | {submission_type:<8} | {deadlock_status:<8} | {server_used:<8}")
        
        print(f"[{get_formatted_time()}] " + "-" * len(interactive_header))
    
    print(f"\n[{get_formatted_time()}] TASK 7: SERVER LOAD BALANCING ANALYSIS:")
    print(f"[{get_formatted_time()}] " + "-"*80)
    
    if load_balancer_stats:
        stats = load_balancer_stats
        total_requests = stats.get('total_requests', 0)
        main_processed = stats.get('main_processed', 0)
        backup_processed = stats.get('backup_processed', 0)
        failed_requests = stats.get('failed_requests', 0)
        
        print(f"[{get_formatted_time()}] Load Balancer Performance:")
        print(f"[{get_formatted_time()}]   Total Requests Received: {total_requests}")
        print(f"[{get_formatted_time()}]   Main Server Processed: {main_processed}")
        print(f"[{get_formatted_time()}]   Backup Server Processed: {backup_processed}")
        print(f"[{get_formatted_time()}]   Failed Requests: {failed_requests}")
        
        if total_requests > 0:
            success_rate = ((main_processed + backup_processed) / total_requests) * 100
            migration_rate = (backup_processed / total_requests) * 100 if total_requests > 0 else 0
            
            print(f"[{get_formatted_time()}]   SUCCESS RATE: {success_rate:.1f}%")
            print(f"[{get_formatted_time()}]   MIGRATION RATE: {migration_rate:.1f}%")
            
            if backup_processed > 0:
                print(f"[{get_formatted_time()}]   LOAD BALANCING STATUS: ACTIVE")
                print(f"[{get_formatted_time()}]   BACKUP SERVER UTILIZATION: {backup_processed} requests")
            else:
                print(f"[{get_formatted_time()}]   LOAD BALANCING STATUS: NOT TRIGGERED")
                print(f"[{get_formatted_time()}]   All requests handled by main server")
        
        buffer_info = f"Buffer: {stats.get('buffer_size', 10)} slots, Threshold: {stats.get('load_threshold', 8)} ({(stats.get('load_threshold', 8)/stats.get('buffer_size', 10))*100:.0f}%)"
        print(f"[{get_formatted_time()}]   {buffer_info}")
    else:
        print(f"[{get_formatted_time()}] No load balancing data available")
        print(f"[{get_formatted_time()}] Run Task 7 load test to see statistics")
    
    if exam_results:
        exam_server_breakdown = {}
        for student_id, exam_data in exam_results.items():
            server_used = exam_data.get('server_used', 'main')
            exam_server_breakdown[server_used] = exam_server_breakdown.get(server_used, 0) + 1
        
        print(f"[{get_formatted_time()}] ")
        print(f"[{get_formatted_time()}] EXAM PROCESSING SERVER BREAKDOWN:")
        for server, count in exam_server_breakdown.items():
            print(f"[{get_formatted_time()}]   {server.upper()} Server: {count} exam{'s' if count != 1 else ''}")
    
    print(f"\n[{get_formatted_time()}] COMPREHENSIVE SUMMARY STATISTICS:")
    print(f"[{get_formatted_time()}] " + "-"*60)
    
    traditional_passed = sum(1 for roll in traditional_students if students[roll]["marks"] >= 50)
    traditional_failed = len(traditional_students) - traditional_passed
    
    interactive_passed = sum(1 for exam_data in exam_results.values() if (exam_data['score'] * 10) >= 50)
    interactive_failed = len(exam_results) - interactive_passed
    
    print(f"[{get_formatted_time()}] TRADITIONAL CHEATING DETECTION:")
    print(f"[{get_formatted_time()}]   Total Students: {len(traditional_students)}")
    print(f"[{get_formatted_time()}]   Passed (>=50): {traditional_passed}")
    print(f"[{get_formatted_time()}]   Failed (<50):  {traditional_failed}")
    
    print(f"[{get_formatted_time()}] TASK 6 INTERACTIVE DEADLOCK EXAM:")
    print(f"[{get_formatted_time()}]   Total Students: {len(exam_results)}")
    print(f"[{get_formatted_time()}]   Passed (>=50): {interactive_passed}")
    print(f"[{get_formatted_time()}]   Failed (<50):  {interactive_failed}")
    
    if exam_results:
        deadlock_count = sum(1 for exam_data in exam_results.values() if exam_data.get('deadlock_detected', False))
        manual_submissions = sum(1 for exam_data in exam_results.values() if exam_data['type'] == 'manual')
        auto_submissions = sum(1 for exam_data in exam_results.values() if exam_data['type'] == 'auto')
        
        print(f"[{get_formatted_time()}] TASK 6 DEADLOCK ANALYSIS:")
        print(f"[{get_formatted_time()}]   Deadlocks Detected: {deadlock_count}")
        print(f"[{get_formatted_time()}]   Manual Submissions: {manual_submissions}")
        print(f"[{get_formatted_time()}]   Auto Submissions: {auto_submissions}")
        print(f"[{get_formatted_time()}]   Deadlock Resolution: Manual Priority Strategy")
    
    total_students = len(traditional_students) + len(exam_results)
    total_passed = traditional_passed + interactive_passed
    
    print(f"[{get_formatted_time()}] OVERALL SYSTEM SUMMARY:")
    print(f"[{get_formatted_time()}]   Total Students Processed: {total_students}")
    print(f"[{get_formatted_time()}]   Overall Passed: {total_passed}")
    print(f"[{get_formatted_time()}]   Overall Failed: {total_students - total_passed}")
    
    if total_students > 0:
        pass_rate = (total_passed / total_students) * 100
        print(f"[{get_formatted_time()}]   Overall Pass Rate: {pass_rate:.1f}%")
    
    print(f"[{get_formatted_time()}] ")
    print(f"[{get_formatted_time()}] SYSTEM FEATURES DEMONSTRATED:")
    print(f"[{get_formatted_time()}]   Berkeley Clock Synchronization: ACTIVE")
    print(f"[{get_formatted_time()}]   Ricart-Agrawala Mutual Exclusion: ACTIVE")
    print(f"[{get_formatted_time()}]   Task 6 Deadlock Resolution: IMPLEMENTED")
    print(f"[{get_formatted_time()}]   Task 7 Load Balancing: IMPLEMENTED")
    
    if load_balancer_stats and load_balancer_stats.get('backup_processed', 0) > 0:
        print(f"[{get_formatted_time()}]   Load Balancer Migration: DEMONSTRATED")
    
    print(f"[{get_formatted_time()}] " + "="*120 + "\n")
    
    return True

def receive_exam_submission(student_id, score, submission_type):
    global exam_results, students, interactive_exam_active, total_exam_submissions, main_server_submissions, backup_server_submissions
    
    total_exam_submissions += 1
    
    print(f"\n[{get_formatted_time()}] " + "="*80)
    print(f"[{get_formatted_time()}] EXAM SUBMISSION #{total_exam_submissions} RECEIVED FROM SERVER")
    print(f"[{get_formatted_time()}] " + "="*80)
    
    server_used = "main"
    
    try:
        from xmlrpc.client import ServerProxy
        server_proxy = ServerProxy(server_url + "/RPC2", allow_none=True)
        server_stats = server_proxy.get_load_balancer_stats()
        
        global load_balancer_stats
        load_balancer_stats = server_stats
        
        if server_stats.get('backup_processed', 0) > backup_server_submissions:
            server_used = "backup"
            backup_server_submissions = server_stats.get('backup_processed', 0)
            print(f"[{get_formatted_time()}] TASK 7: DETECTED BACKUP SERVER PROCESSING")
        else:
            main_server_submissions += 1
            
    except Exception as e:
        print(f"[{get_formatted_time()}] Could not retrieve load balancer stats: {e}")
    
    exam_results[student_id] = {
        "score": score, 
        "type": submission_type,
        "submission_time": datetime.now(),
        "deadlock_detected": False,
        "server_used": server_used
    }
    
    final_marks = score * 10
    
    print(f"[{get_formatted_time()}] Student ID: {student_id}")
    print(f"[{get_formatted_time()}] Student Name: {students.get(student_id, {}).get('name', 'Unknown')}")
    print(f"[{get_formatted_time()}] Raw Score: {score}/10 questions correct")
    print(f"[{get_formatted_time()}] Final Score: {final_marks}/100 marks")
    print(f"[{get_formatted_time()}] Submission Type: {submission_type.upper()}")
    print(f"[{get_formatted_time()}] Processing Server: {server_used.upper()}")
    print(f"[{get_formatted_time()}] Final Grade: {'PASS' if final_marks >= 50 else 'FAIL'}")
    
    if submission_type.upper() == "MANUAL":
        print(f"[{get_formatted_time()}] EXCELLENT! Student completed manual submission")
        print(f"[{get_formatted_time()}] Successfully submitted before timer expired")
        print(f"[{get_formatted_time()}] If deadlock occurred, manual submission took priority")
    elif submission_type.upper() == "AUTO":
        print(f"[{get_formatted_time()}] Auto-submission due to timer expiry")
        print(f"[{get_formatted_time()}] Student did not submit manually within time limit")
        print(f"[{get_formatted_time()}] System processed available answers automatically")
        print(f"[{get_formatted_time()}] Client was notified immediately of time expiry")
    else:
        print(f"[{get_formatted_time()}] Special submission case: {submission_type}")
    
    if server_used == "backup":
        print(f"[{get_formatted_time()}] ")
        print(f"[{get_formatted_time()}] TASK 7: LOAD BALANCING ANALYSIS")
        print(f"[{get_formatted_time()}] Server overload detected - request migrated to backup")
        print(f"[{get_formatted_time()}] Backup server processed exam successfully")
        print(f"[{get_formatted_time()}] Results returned to main server and teacher")
        print(f"[{get_formatted_time()}] Load balancing working correctly!")
    
    print(f"[{get_formatted_time()}] " + "="*80)
    
    if student_id in students:
        students[student_id]["exam_score"] = final_marks
        students[student_id]["exam_type"] = "Interactive"
        students[student_id]["marks"] = final_marks
        students[student_id]["server_used"] = server_used
        print(f"[{get_formatted_time()}] [TEACHER] Local student data updated for roll {student_id}")
    
    print(f"\n[{get_formatted_time()}] " + "="*90)
    print(f"[{get_formatted_time()}] COMPREHENSIVE EXAM RESULTS TABLE")
    print(f"[{get_formatted_time()}] UPDATED AFTER SUBMISSION FROM STUDENT {student_id}")
    print(f"[{get_formatted_time()}] " + "="*90)
    
    print_results()
    
    return True

def get_database_connection():
    try:
        return sqlite3.connect(DB_PATH)
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER-DB] Database connection failed: {e}")
        return None

def view_load_balancer_stats():
    print(f"\n[{get_formatted_time()}] " + "="*80)
    print(f"[{get_formatted_time()}] TASK 7: LOAD BALANCER STATISTICS")
    print(f"[{get_formatted_time()}] " + "="*80)
    
    if load_balancer_stats:
        stats = load_balancer_stats
        print(f"[{get_formatted_time()}] Real-time Load Balancer Status:")
        print(f"[{get_formatted_time()}]   Total Requests: {stats.get('total_requests', 0)}")
        print(f"[{get_formatted_time()}]   Main Server Processed: {stats.get('main_processed', 0)}")
        print(f"[{get_formatted_time()}]   Backup Server Processed: {stats.get('backup_processed', 0)}")
        print(f"[{get_formatted_time()}]   Failed Requests: {stats.get('failed_requests', 0)}")
        print(f"[{get_formatted_time()}]   Load Balancer Active: {stats.get('load_balancer_active', False)}")
        print(f"[{get_formatted_time()}]   Buffer Configuration: {stats.get('buffer_size', 10)} slots")
        print(f"[{get_formatted_time()}]   Current Load: {stats.get('current_load', 0)}")
        print(f"[{get_formatted_time()}]   Load Threshold: {stats.get('load_threshold', 8)} (80%)")
        
        total_requests = stats.get('total_requests', 0)
        if total_requests > 0:
            success_requests = stats.get('main_processed', 0) + stats.get('backup_processed', 0)
            success_rate = (success_requests / total_requests) * 100
            migration_rate = (stats.get('backup_processed', 0) / total_requests) * 100
            
            print(f"[{get_formatted_time()}]   ")
            print(f"[{get_formatted_time()}]   Performance Metrics:")
            print(f"[{get_formatted_time()}]     Success Rate: {success_rate:.1f}%")
            print(f"[{get_formatted_time()}]     Migration Rate: {migration_rate:.1f}%")
            
            if stats.get('backup_processed', 0) > 0:
                print(f"[{get_formatted_time()}]     Load Balancing: ACTIVE")
                print(f"[{get_formatted_time()}]     Backup Utilization: SUCCESSFUL")
            else:
                print(f"[{get_formatted_time()}]     Load Balancing: INACTIVE")
                print(f"[{get_formatted_time()}]     All requests handled by main server")
    else:
        print(f"[{get_formatted_time()}] No load balancer statistics available")
        print(f"[{get_formatted_time()}] Connect to server and run load test to see data")
    
    print(f"[{get_formatted_time()}] " + "="*80)
    return True

def view_deadlock_events():
    print(f"\n[{get_formatted_time()}] " + "="*80)
    print(f"[{get_formatted_time()}] TASK 6: DEADLOCK EVENTS ANALYSIS")
    print(f"[{get_formatted_time()}] " + "="*80)
    
    try:
        conn = get_database_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM deadlock_events 
                ORDER BY timestamp DESC
            ''')
            events = cursor.fetchall()
            
            if events:
                print(f"[{get_formatted_time()}] Found {len(events)} deadlock event(s):")
                for event in events:
                    print(f"[{get_formatted_time()}] Student: {event[1]}, Type: {event[2]}")
                    print(f"[{get_formatted_time()}]   Resolution: {event[5]}, Winner: {event[6]}")
                    print(f"[{get_formatted_time()}]   Time: {event[7]}")
                    print(f"[{get_formatted_time()}]   " + "-"*60)
            else:
                print(f"[{get_formatted_time()}] No deadlock events recorded")
            
            conn.close()
        else:
            print(f"[{get_formatted_time()}] Could not access deadlock events database")
    except Exception as e:
        print(f"[{get_formatted_time()}] Error viewing deadlock events: {e}")
    
    print(f"[{get_formatted_time()}] " + "="*80)
    return True

def view_status_db():
    print(f"\n[{get_formatted_time()}] " + "="*70)
    print(f"[{get_formatted_time()}] TASK 6: STATUS DATABASE ENTRIES")
    print(f"[{get_formatted_time()}] " + "="*70)
    
    try:
        conn = get_database_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM status_db 
                ORDER BY timestamp DESC
            ''')
            statuses = cursor.fetchall()
            
            if statuses:
                for status in statuses:
                    print(f"[{get_formatted_time()}] Student: {status[1]}, Status: {status[2]}")
                    print(f"[{get_formatted_time()}]   Time: {status[3]}, Type: {status[4]}")
                    print(f"[{get_formatted_time()}]   " + "-"*50)
            else:
                print(f"[{get_formatted_time()}] No status entries found")
            
            conn.close()
        else:
            print(f"[{get_formatted_time()}] Could not access status database")
    except Exception as e:
        print(f"[{get_formatted_time()}] Error viewing status database: {e}")
    
    print(f"[{get_formatted_time()}] " + "="*70)
    return True

def view_submission_db():
    print(f"\n[{get_formatted_time()}] " + "="*80)
    print(f"[{get_formatted_time()}] COMBINED TASK 6 & 7: SUBMISSION DATABASE ENTRIES")
    print(f"[{get_formatted_time()}] " + "="*80)
    
    try:
        conn = get_database_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM submission_db 
                ORDER BY submission_time DESC
            ''')
            submissions = cursor.fetchall()
            
            if submissions:
                for sub in submissions:
                    server_used = sub[8] if len(sub) > 8 else 'main'
                    print(f"[{get_formatted_time()}] Student: {sub[1]}, Score: {sub[2]*10}/100")
                    print(f"[{get_formatted_time()}]   Type: {sub[3]}, Deadlock: {sub[4]}")
                    print(f"[{get_formatted_time()}]   Server Used: {server_used.upper()}")
                    print(f"[{get_formatted_time()}]   Resolution: {sub[5]}, Time: {sub[6]}")
                    print(f"[{get_formatted_time()}]   " + "-"*60)
            else:
                print(f"[{get_formatted_time()}] No submission entries found")
            
            conn.close()
        else:
            print(f"[{get_formatted_time()}] Could not access submission database")
    except Exception as e:
        print(f"[{get_formatted_time()}] Error viewing submission database: {e}")
    
    print(f"[{get_formatted_time()}] " + "="*80)
    return True

def send_request_with_timeout(url, method, *args, timeout=5):
    try:
        proxy = xmlrpc.client.ServerProxy(url + "/RPC2", allow_none=True)
        proxy._ServerProxy__transport.timeout = timeout
        result = getattr(proxy, method)(*args)
        return result
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER-RA] Timeout/Error calling {method}: {e}")
        return False

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

def exit_critical_section():
    global in_cs, deferred_replies
    print(f"[{get_formatted_time()}] [TEACHER-RA] *** EXITING CRITICAL SECTION (MARKSHEET ACCESS) ***")
    
    in_cs = False
    
    for receiver_id in deferred_replies:
        send_reply(receiver_id)
        print(f"[{get_formatted_time()}] [TEACHER-RA] Sent deferred reply to {receiver_id}")
    
    deferred_replies.clear()

def release_critical_section():
    global in_cs
    if in_cs:
        print(f"[{get_formatted_time()}] [TEACHER-RA] Manually releasing critical section...")
        exit_critical_section()
        return "Critical section released"
    else:
        return "Not currently in critical section"

def register_urls(client, server, self_):
    global client_url, server_url, self_url
    client_url = client
    server_url = server
    self_url = self_
    
    try:
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        server_ip = server_url.split("//")[1].split(":")[0]
        
        print(f"[{get_formatted_time()}] [TEACHER] URLs registered - Client: {client}, Server: {server}")
        
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER] URL registration warning: {e}")
        
    return True

def run_teacher():
    global clock_offset
    clock_offset = input_initial_time()
    
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print(f"[{get_formatted_time()}] [TEACHER] Starting Enhanced Teacher with ALL TASKS...")
    print(f"[{get_formatted_time()}] [TEACHER] Features: Marksheet, Task 6 Exam Processing, Task 7 Load Balancing Analysis")
    print(f"[{get_formatted_time()}] [TEACHER] Enhanced Reporting: Traditional + Task 6 Interactive + Task 7 Load Balancing")
    print(f"[{get_formatted_time()}] [TEACHER] Task 6: Deadlock detection and resolution monitoring")
    print(f"[{get_formatted_time()}] [TEACHER] Task 7: Load balancing statistics and server utilization tracking")
    print(f"[{get_formatted_time()}] [TEACHER] Scoring: Interactive exams converted to /100 scale (10 points per correct)")
    print(f"[{get_formatted_time()}] [TEACHER] Pass Threshold: 50/100 marks for all exam types")
    print(f"[{get_formatted_time()}] [TEACHER] Teacher hostname: {hostname}")
    print(f"[{get_formatted_time()}] [TEACHER] Teacher IP address: {local_ip}")
    print(f"[{get_formatted_time()}] [TEACHER] Teacher server running at port 9001")
    print(f"[{get_formatted_time()}] [TEACHER] Database: {DB_PATH}")
    print(f"[{get_formatted_time()}] [TEACHER-RA] Initially holding Critical Section (marksheet)")
    print(f"[{get_formatted_time()}] [TEACHER-RA] Ricart-Agrawala algorithm initialized, process ID: {my_process_id}")
    
    with SimpleXMLRPCServer(("0.0.0.0", 9001),
                           requestHandler=SimpleXMLRPCRequestHandler,
                           allow_none=True) as server:
        
        server.register_function(get_time, "get_time")
        server.register_function(adjust_time, "adjust_time")
        server.register_function(get_test_start_time, "get_test_start_time")
        server.register_function(warn_student, "warn_student")
        server.register_function(catch_student, "catch_student")
        server.register_function(print_results, "print_results")
        server.register_function(run_timer, "run_timer")
        server.register_function(is_test_active, "is_test_active")
        server.register_function(register_urls, "register_urls")
        
        server.register_function(receive_request, "receive_request")
        server.register_function(receive_reply, "receive_reply")
        server.register_function(release_critical_section, "release_critical_section")
        
        server.register_function(receive_exam_submission, "receive_exam_submission")
        server.register_function(view_deadlock_events, "view_deadlock_events")
        server.register_function(view_status_db, "view_status_db")
        server.register_function(view_submission_db, "view_submission_db")
        server.register_function(view_load_balancer_stats, "view_load_balancer_stats")
        
        print(f"[{get_formatted_time()}] [TEACHER] All functions registered:")
        print(f"[{get_formatted_time()}] [TEACHER] - Baseline: get_time, adjust_time, warn_student, catch_student")
        print(f"[{get_formatted_time()}] [TEACHER] - Test management: run_timer, is_test_active, get_test_start_time")
        print(f"[{get_formatted_time()}] [TEACHER] - Enhanced Marksheet: print_results (Traditional + Task 6 + Task 7)")
        print(f"[{get_formatted_time()}] [TEACHER] - Ricart-Agrawala: receive_request, receive_reply, release_critical_section")
        print(f"[{get_formatted_time()}] [TEACHER] - TASK 6 Functions: receive_exam_submission, view_deadlock_events")
        print(f"[{get_formatted_time()}] [TEACHER] - TASK 6 Database: view_status_db, view_submission_db")
        print(f"[{get_formatted_time()}] [TEACHER] - TASK 7 NEW: view_load_balancer_stats")
        print(f"[{get_formatted_time()}] [TEACHER] Ready to handle requests...")
        
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print(f"\n[{get_formatted_time()}] [TEACHER] Teacher server shutting down...")

if __name__ == "__main__":
    run_teacher()
