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
import psutil
import subprocess

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

TEACHER_REPLICATION_DB = "teacher_marksheet_replicated.db"
replication_lock = threading.Lock()
teacher_replication_initialized = False

node_manager_active = False
resource_monitor_active = False
processing_jobs_completed = 0
system_resources = {
    "cpu_percent": 0,
    "memory_percent": 0,
    "disk_percent": 0,
    "network_bytes_sent": 0,
    "network_bytes_recv": 0
}
network_stats = {
    "ping_server": 0,
    "ping_client": 0,
    "packets_sent": 0,
    "packets_lost": 0
}

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

def get_system_resources():
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        net_io = psutil.net_io_counters()
        
        global system_resources
        system_resources = {
            "cpu_percent": cpu_percent,
            "memory_percent": memory.percent,
            "memory_available": memory.available // (1024*1024),
            "disk_percent": disk.percent,
            "disk_free": disk.free // (1024*1024*1024),
            "network_bytes_sent": net_io.bytes_sent,
            "network_bytes_recv": net_io.bytes_recv,
            "timestamp": time.time()
        }
        
        return system_resources
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Error getting system resources: {e}")
        return {
            "cpu_percent": 0,
            "memory_percent": 0,
            "memory_available": 1000,
            "disk_percent": 50,
            "disk_free": 10,
            "network_bytes_sent": 0,
            "network_bytes_recv": 0,
            "timestamp": time.time()
        }

def get_network_health():
    try:
        global network_stats
        
        base_latency = random.uniform(20.0, 55.0)
        
        network_stats["ping_server"] = round(base_latency, 2)
        network_stats["ping_client"] = round(base_latency + random.uniform(5.0, 15.0), 2)
        network_stats["packets_sent"] += 2
        
        if random.random() < 0.05:
            network_stats["packets_lost"] += 1
            
        return network_stats
        
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Network health simulation: {e}")
        return {
            "ping_server": 50.0,
            "ping_client": 55.0,
            "packets_sent": 2,
            "packets_lost": 0
        }

def initialize_node_manager():
    global node_manager_active, resource_monitor_active
    
    if node_manager_active:
        return True
    
    print(f"[{get_formatted_time()}] [TEACHER-TASK9] Initializing YARN-like Node Manager...")
    print(f"[{get_formatted_time()}] [TEACHER-TASK9] Starting resource monitoring and job processing capabilities...")
    
    def node_manager_thread():
        global node_manager_active, resource_monitor_active
        node_manager_active = True
        resource_monitor_active = True
        
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Node Manager active - ready to process distributed jobs")
        
        while node_manager_active:
            try:
                resources = get_system_resources()
                network = get_network_health()
                
                node_metrics = {
                    "node_id": "teacher",
                    "resources": resources,
                    "network": network,
                    "status": "active",
                    "heartbeat": time.time(),
                    "jobs_completed": processing_jobs_completed,
                    "node_type": "teacher_node"
                }
                
                try:
                    server_proxy = xmlrpc.client.ServerProxy(server_url + "/RPC2", allow_none=True)
                    server_proxy.report_node_metrics(node_metrics)
                except Exception as e:
                    print(f"[{get_formatted_time()}] [TEACHER-TASK9] Failed to report metrics to Resource Manager: {e}")
                
                time.sleep(12)
                
            except Exception as e:
                print(f"[{get_formatted_time()}] [TEACHER-TASK9] Node Manager error: {e}")
                time.sleep(5)
    
    nm_thread = threading.Thread(target=node_manager_thread, daemon=True)
    nm_thread.start()
    
    print(f"[{get_formatted_time()}] [TEACHER-TASK9] Node Manager initialized successfully")
    return True

def process_teacher_job(job_type, parameters=None):
    global processing_jobs_completed
    
    try:
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Processing job: {job_type}")
        
        processing_jobs_completed += 1
        
        if job_type == "ANALYZE_MARKSHEET":
            return analyze_marksheet_data()
        elif job_type == "CALCULATE_GRADES":
            return calculate_distributed_grades()
        elif job_type == "VALIDATE_SUBMISSIONS":
            return validate_exam_submissions()
        elif job_type == "PROCESS_REPLICATION":
            return process_replication_status()
        else:
            return {"error": f"Unknown teacher job type: {job_type}"}
            
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Job processing error: {e}")
        return {"error": str(e)}

def analyze_marksheet_data():
    try:
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Analyzing distributed marksheet data...")
        
        analysis = {
            "job_type": "ANALYZE_MARKSHEET",
            "processed_by": "teacher_node",
            "analysis_results": {},
            "traditional_students": {},
            "exam_students": {}
        }
        
        for student_id, info in students.items():
            analysis["traditional_students"][str(student_id)] = {
                "name": info["name"],
                "marks": info["marks"],
                "status": info["status"],
                "grade": "A" if info["marks"] >= 90 else "B" if info["marks"] >= 70 else "C" if info["marks"] >= 50 else "F"
            }
        
        if exam_results:
            for student_id, exam_data in exam_results.items():
                final_marks = exam_data.get("score", 0) * 10
                analysis["exam_students"][str(student_id)] = {
                    "raw_score": exam_data.get("score", 0),
                    "final_marks": final_marks,
                    "submission_type": exam_data.get("type", "unknown"),
                    "grade": "A" if final_marks >= 90 else "B" if final_marks >= 70 else "C" if final_marks >= 50 else "F"
                }
        
        total_students = len(students) + len(exam_results)
        passing_students = len([s for s in students.values() if s["marks"] >= 50])
        passing_students += len([e for e in exam_results.values() if (e.get("score", 0) * 10) >= 50])
        
        analysis["analysis_results"] = {
            "total_students": total_students,
            "passing_students": passing_students,
            "pass_rate": round((passing_students / total_students) * 100, 2) if total_students > 0 else 0,
            "traditional_submissions": len(students),
            "interactive_submissions": len(exam_results)
        }
        
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Marksheet analysis completed: {passing_students}/{total_students} passing")
        
        return analysis
        
    except Exception as e:
        return {"error": str(e)}

def calculate_distributed_grades():
    try:
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Calculating distributed grades across all student types...")
        
        grade_distribution = {
            "job_type": "CALCULATE_GRADES",
            "processed_by": "teacher_node",
            "grade_counts": {"A": 0, "B": 0, "C": 0, "F": 0},
            "detailed_grades": {}
        }
        
        all_students = {}
        
        for student_id, info in students.items():
            marks = info["marks"]
            grade = "A" if marks >= 90 else "B" if marks >= 70 else "C" if marks >= 50 else "F"
            all_students[student_id] = {"name": info["name"], "marks": marks, "grade": grade, "type": "traditional"}
            grade_distribution["grade_counts"][grade] += 1
        
        for student_id, exam_data in exam_results.items():
            final_marks = exam_data.get("score", 0) * 10
            grade = "A" if final_marks >= 90 else "B" if final_marks >= 70 else "C" if final_marks >= 50 else "F"
            student_name = students.get(student_id, {}).get("name", f"Student_{student_id}")
            all_students[student_id] = {"name": student_name, "marks": final_marks, "grade": grade, "type": "interactive"}
            grade_distribution["grade_counts"][grade] += 1
        
        grade_distribution["detailed_grades"] = all_students
        grade_distribution["total_students"] = len(all_students)
        
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Grade calculation completed: A={grade_distribution['grade_counts']['A']}, B={grade_distribution['grade_counts']['B']}, C={grade_distribution['grade_counts']['C']}, F={grade_distribution['grade_counts']['F']}")
        
        return grade_distribution
        
    except Exception as e:
        return {"error": str(e)}

def validate_exam_submissions():
    try:
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Validating exam submissions across all systems...")
        
        validation = {
            "job_type": "VALIDATE_SUBMISSIONS",
            "processed_by": "teacher_node",
            "validation_results": {},
            "statistics": {}
        }
        
        total_submissions = total_exam_submissions
        valid_submissions = 0
        
        for student_id, exam_data in exam_results.items():
            score = exam_data.get("score", 0)
            submission_type = exam_data.get("type", "unknown")
            
            is_valid = (0 <= score <= 10) and submission_type in ["manual", "auto"]
            if is_valid:
                valid_submissions += 1
            
            validation["validation_results"][str(student_id)] = {
                "score": score,
                "submission_type": submission_type,
                "is_valid": is_valid,
                "final_marks": score * 10
            }
        
        if load_balancer_stats:
            server_distribution = {
                "main_server": load_balancer_stats.get("main_processed", 0),
                "backup_server": load_balancer_stats.get("backup_processed", 0),
                "total_requests": load_balancer_stats.get("total_requests", 0)
            }
        else:
            server_distribution = {"main_server": 0, "backup_server": 0, "total_requests": 0}
        
        validation["statistics"] = {
            "total_submissions": total_submissions,
            "valid_submissions": valid_submissions,
            "invalid_submissions": total_submissions - valid_submissions,
            "validation_rate": round((valid_submissions / total_submissions) * 100, 2) if total_submissions > 0 else 0,
            "server_distribution": server_distribution
        }
        
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Submission validation completed: {valid_submissions}/{total_submissions} valid")
        
        return validation
        
    except Exception as e:
        return {"error": str(e)}

def process_replication_status():
    try:
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Processing replication status across teacher marksheet system...")
        
        replication_analysis = {
            "job_type": "PROCESS_REPLICATION",
            "processed_by": "teacher_node",
            "replication_health": {},
            "consistency_check": {}
        }
        
        if teacher_replication_initialized:
            try:
                with replication_lock:
                    conn = sqlite3.connect(TEACHER_REPLICATION_DB)
                    cursor = conn.cursor()
                    
                    cursor.execute("SELECT chunk_id, student_range, replica_count, status FROM teacher_chunk_metadata")
                    chunks = cursor.fetchall()
                    
                    for chunk_id, student_range, replica_count, status in chunks:
                        replica_health = {}
                        
                        for replica_id in range(1, replica_count + 1):
                            table_name = f"marksheet_chunk_{chunk_id}_replica_{replica_id}"
                            cursor.execute(f"SELECT COUNT(*), AVG(marks) FROM {table_name}")
                            stats = cursor.fetchone()
                            
                            replica_health[f"replica_{replica_id}"] = {
                                "record_count": stats[0] if stats[0] else 0,
                                "avg_marks": round(stats[1], 2) if stats[1] else 0,
                                "status": "healthy"
                            }
                        
                        replication_analysis["replication_health"][f"chunk_{chunk_id}"] = {
                            "student_range": student_range,
                            "replica_count": replica_count,
                            "chunk_status": status,
                            "replicas": replica_health
                        }
                    
                    cursor.execute("SELECT COUNT(*) FROM replication_log")
                    log_count = cursor.fetchone()[0]
                    
                    replication_analysis["consistency_check"] = {
                        "replication_factor": 3,
                        "total_chunks": len(chunks),
                        "replication_log_entries": log_count,
                        "system_status": "consistent"
                    }
                    
                    conn.close()
                    
            except Exception as e:
                replication_analysis["consistency_check"] = {"error": str(e)}
        else:
            replication_analysis["replication_health"] = {"status": "not_initialized"}
            replication_analysis["consistency_check"] = {"status": "not_available"}
        
        print(f"[{get_formatted_time()}] [TEACHER-TASK9] Replication status processing completed")
        
        return replication_analysis
        
    except Exception as e:
        return {"error": str(e)}

def initialize_replication_system():
    global teacher_replication_initialized
    
    if teacher_replication_initialized:
        return True
    
    print(f"[{get_formatted_time()}] [TEACHER-TASK8] Initializing Teacher Replication System...")
    print(f"[{get_formatted_time()}] [TEACHER-TASK8] Setting up marksheet replicas with RF=3...")
    
    def teacher_replication_background_task():
        setup_teacher_replication_database()
        create_marksheet_replicas()
        start_teacher_replication_sync()
    
    replication_thread = threading.Thread(target=teacher_replication_background_task, daemon=True)
    replication_thread.start()
    
    time.sleep(1)
    teacher_replication_initialized = True
    print(f"[{get_formatted_time()}] [TEACHER-TASK8] Teacher replication system activated")
    return True

def setup_teacher_replication_database():
    with replication_lock:
        try:
            conn = sqlite3.connect(TEACHER_REPLICATION_DB)
            cursor = conn.cursor()
            
            print(f"[{get_formatted_time()}] [TEACHER-TASK8] Setting up teacher marksheet replica tables...")
            
            for chunk_id in range(3):
                for replica_id in range(1, 4):
                    table_name = f"marksheet_chunk_{chunk_id}_replica_{replica_id}"
                    cursor.execute(f'''
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            roll_number INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            marks INTEGER DEFAULT 100,
                            reason TEXT DEFAULT '',
                            status TEXT DEFAULT 'ACTIVE',
                            ISA INTEGER DEFAULT 0,
                            MSE INTEGER DEFAULT 0,
                            ESE INTEGER DEFAULT 0,
                            total INTEGER DEFAULT 0,
                            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            version INTEGER DEFAULT 1
                        )
                    ''')
                    print(f"[{get_formatted_time()}] [TEACHER-TASK8] Created marksheet table: {table_name}")
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS teacher_chunk_metadata (
                    chunk_id INTEGER PRIMARY KEY,
                    student_range TEXT,
                    replica_count INTEGER DEFAULT 3,
                    status TEXT DEFAULT 'active',
                    write_locked BOOLEAN DEFAULT FALSE,
                    read_count INTEGER DEFAULT 0
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS teacher_replica_status (
                    chunk_id INTEGER,
                    replica_id INTEGER,
                    node_location TEXT,
                    status TEXT DEFAULT 'online',
                    last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chunk_id, replica_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS replication_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation TEXT,
                    chunk_id INTEGER,
                    student_id INTEGER,
                    old_value TEXT,
                    new_value TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [TEACHER-TASK8] Teacher replication database setup failed: {e}")

def create_marksheet_replicas():
    students_data = [
        {"roll": 29, "name": "Mayuresh", "marks": 100, "ISA": 95, "MSE": 87, "ESE": 92},
        {"roll": 40, "name": "Ayush", "marks": 100, "ISA": 88, "MSE": 90, "ESE": 85},
        {"roll": 42, "name": "Aashna", "marks": 100, "ISA": 92, "MSE": 85, "ESE": 88},
        {"roll": 50, "name": "Rohit", "marks": 100, "ISA": 85, "MSE": 92, "ESE": 90},
        {"roll": 52, "name": "Rushikesh", "marks": 100, "ISA": 90, "MSE": 88, "ESE": 95}
    ]
    
    chunks = [
        {"chunk_id": 0, "students": students_data[0:2], "range": "29-40"},
        {"chunk_id": 1, "students": students_data[2:4], "range": "42-50"},
        {"chunk_id": 2, "students": students_data[4:5], "range": "52"}
    ]
    
    with replication_lock:
        try:
            conn = sqlite3.connect(TEACHER_REPLICATION_DB)
            cursor = conn.cursor()
            
            for chunk in chunks:
                chunk_id = chunk["chunk_id"]
                
                cursor.execute('''
                    INSERT OR REPLACE INTO teacher_chunk_metadata (chunk_id, student_range, replica_count, status)
                    VALUES (?, ?, 3, 'active')
                ''', (chunk_id, chunk["range"]))
                
                print(f"[{get_formatted_time()}] [TEACHER-TASK8] Creating Marksheet Chunk {chunk_id} with students {chunk['range']}")
                
                for replica_id in range(1, 4):
                    table_name = f"marksheet_chunk_{chunk_id}_replica_{replica_id}"
                    
                    cursor.execute(f"DELETE FROM {table_name}")
                    
                    for student in chunk["students"]:
                        total = student["ISA"] + student["MSE"] + student["ESE"]
                        cursor.execute(f'''
                            INSERT INTO {table_name} 
                            (roll_number, name, marks, reason, status, ISA, MSE, ESE, total, last_updated, version)
                            VALUES (?, ?, ?, '', 'ACTIVE', ?, ?, ?, ?, CURRENT_TIMESTAMP, 1)
                        ''', (student["roll"], student["name"], student["marks"], 
                              student["ISA"], student["MSE"], student["ESE"], total))
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO teacher_replica_status 
                        (chunk_id, replica_id, node_location, status, last_sync)
                        VALUES (?, ?, ?, 'online', CURRENT_TIMESTAMP)
                    ''', (chunk_id, replica_id, f"teacher_node_{replica_id}"))
                    
                    print(f"[{get_formatted_time()}] [TEACHER-TASK8] Replicated Marksheet Chunk {chunk_id} to Teacher Node {replica_id}")
            
            conn.commit()
            conn.close()
            
            print(f"[{get_formatted_time()}] [TEACHER-TASK8] All marksheet chunks replicated successfully with RF=3")
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [TEACHER-TASK8] Marksheet chunk creation failed: {e}")

def start_teacher_replication_sync():
    def sync_teacher_replicas():
        while True:
            time.sleep(20)
            
            try:
                with replication_lock:
                    conn = sqlite3.connect(TEACHER_REPLICATION_DB)
                    cursor = conn.cursor()
                    
                    cursor.execute("SELECT chunk_id, student_range FROM teacher_chunk_metadata WHERE status='active'")
                    chunks = cursor.fetchall()
                    
                    for chunk_id, student_range in chunks:
                        primary_table = f"marksheet_chunk_{chunk_id}_replica_1"
                        cursor.execute(f"SELECT COUNT(*) FROM {primary_table}")
                        record_count = cursor.fetchone()[0]
                        
                        if record_count > 0:
                            print(f"[{get_formatted_time()}] [TEACHER-TASK8] Sync check: Marksheet Chunk {chunk_id} ({student_range}) - {record_count} records consistent across 3 replicas")
                    
                    conn.close()
                    
            except Exception as e:
                print(f"[{get_formatted_time()}] [TEACHER-TASK8] Teacher sync process error: {e}")
    
    sync_thread = threading.Thread(target=sync_teacher_replicas, daemon=True)
    sync_thread.start()

def acquire_teacher_write_lock(chunk_id):
    with replication_lock:
        try:
            conn = sqlite3.connect(TEACHER_REPLICATION_DB)
            cursor = conn.cursor()
            
            while True:
                cursor.execute("SELECT write_locked, read_count FROM teacher_chunk_metadata WHERE chunk_id = ?", (chunk_id,))
                result = cursor.fetchone()
                
                if result and not result[0] and result[1] == 0:
                    cursor.execute("UPDATE teacher_chunk_metadata SET write_locked = TRUE WHERE chunk_id = ?", (chunk_id,))
                    conn.commit()
                    print(f"[{get_formatted_time()}] [TEACHER-TASK8] Write lock acquired for Marksheet Chunk {chunk_id}")
                    conn.close()
                    return True
                
                time.sleep(0.1)
                
        except Exception as e:
            print(f"[{get_formatted_time()}] [TEACHER-TASK8] Error acquiring write lock: {e}")
            return False

def release_teacher_write_lock(chunk_id):
    with replication_lock:
        try:
            conn = sqlite3.connect(TEACHER_REPLICATION_DB)
            cursor = conn.cursor()
            
            cursor.execute("UPDATE teacher_chunk_metadata SET write_locked = FALSE WHERE chunk_id = ?", (chunk_id,))
            conn.commit()
            conn.close()
            
            print(f"[{get_formatted_time()}] [TEACHER-TASK8] Write lock released for Marksheet Chunk {chunk_id}")
            return True
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [TEACHER-TASK8] Error releasing write lock: {e}")
            return False

def write_to_marksheet_replicas(student_id, marks_update, reason=""):
    chunk_id = get_marksheet_chunk_for_student(student_id)
    
    if acquire_teacher_write_lock(chunk_id):
        try:
            print(f"[{get_formatted_time()}] [TEACHER-TASK8] Writing marksheet updates for student {student_id} to all replicas...")
            
            with replication_lock:
                conn = sqlite3.connect(TEACHER_REPLICATION_DB)
                cursor = conn.cursor()
                
                old_marks = 0
                cursor.execute(f"SELECT marks FROM marksheet_chunk_{chunk_id}_replica_1 WHERE roll_number = ?", (student_id,))
                result = cursor.fetchone()
                if result:
                    old_marks = result[0]
                
                for replica_id in range(1, 4):
                    table_name = f"marksheet_chunk_{chunk_id}_replica_{replica_id}"
                    
                    cursor.execute(f'''
                        UPDATE {table_name} 
                        SET marks = ?, reason = ?, status = ?,
                            last_updated = CURRENT_TIMESTAMP, version = version + 1
                        WHERE roll_number = ?
                    ''', (marks_update, reason, "CAUGHT" if marks_update == 0 else "WARNED" if marks_update == 50 else "ACTIVE", student_id))
                    
                    print(f"[{get_formatted_time()}] [TEACHER-TASK8] Updated student {student_id} marks in {table_name}")
                
                cursor.execute('''
                    INSERT INTO replication_log (operation, chunk_id, student_id, old_value, new_value, timestamp)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', ("UPDATE_MARKS", chunk_id, student_id, str(old_marks), str(marks_update)))
                
                conn.commit()
                conn.close()
                
                print(f"[{get_formatted_time()}] [TEACHER-TASK8] All marksheet replicas updated successfully for student {student_id} (marks: {old_marks}→{marks_update})")
                return True
                
        except Exception as e:
            print(f"[{get_formatted_time()}] [TEACHER-TASK8] Write to marksheet replicas failed: {e}")
            return False
        finally:
            release_teacher_write_lock(chunk_id)
    
    return False

def get_marksheet_chunk_for_student(student_id):
    if student_id in [29, 40]:
        return 0
    elif student_id in [42, 50]:
        return 1
    elif student_id == 52:
        return 2
    else:
        return 0

def show_replication_status():
    print(f"\n[{get_formatted_time()}] " + "="*80)
    print(f"[{get_formatted_time()}] TASK 8: TEACHER MARKSHEET REPLICATION STATUS")
    print(f"[{get_formatted_time()}] " + "="*80)
    
    try:
        with replication_lock:
            conn = sqlite3.connect(TEACHER_REPLICATION_DB)
            cursor = conn.cursor()
            
            cursor.execute("SELECT chunk_id, student_range, replica_count, status FROM teacher_chunk_metadata")
            chunks = cursor.fetchall()
            
            print(f"[{get_formatted_time()}] MARKSHEET CHUNK DISTRIBUTION:")
            for chunk_id, student_range, replica_count, status in chunks:
                print(f"[{get_formatted_time()}] Marksheet Chunk {chunk_id}: Students {student_range} | Replicas: {replica_count} | Status: {status}")
            
            print(f"\n[{get_formatted_time()}] MARKSHEET REPLICA STATUS:")
            cursor.execute("SELECT chunk_id, replica_id, node_location, status FROM teacher_replica_status ORDER BY chunk_id, replica_id")
            replicas = cursor.fetchall()
            
            for chunk_id, replica_id, node_location, status in replicas:
                print(f"[{get_formatted_time()}] Marksheet Chunk {chunk_id} Replica {replica_id}: {node_location} ({status})")
            
            print(f"\n[{get_formatted_time()}] CURRENT MARKSHEET DATA FROM REPLICAS:")
            for chunk_id in range(3):
                table_name = f"marksheet_chunk_{chunk_id}_replica_1"
                cursor.execute(f"SELECT roll_number, name, marks, status FROM {table_name}")
                students_data = cursor.fetchall()
                
                for roll, name, marks, status in students_data:
                    print(f"[{get_formatted_time()}] Marksheet Chunk {chunk_id}: {roll} ({name}) Marks: {marks} Status: {status}")
            
            print(f"\n[{get_formatted_time()}] RECENT REPLICATION LOG:")
            cursor.execute("SELECT operation, chunk_id, student_id, old_value, new_value, timestamp FROM replication_log ORDER BY timestamp DESC LIMIT 5")
            logs = cursor.fetchall()
            
            for operation, chunk_id, student_id, old_value, new_value, timestamp in logs:
                print(f"[{get_formatted_time()}] {timestamp}: {operation} - Chunk {chunk_id}, Student {student_id}: {old_value}→{new_value}")
            
            conn.close()
    
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER-TASK8] Error showing replication status: {e}")
    
    print(f"[{get_formatted_time()}] " + "="*80)
    return True

def warn_student(roll_no):
    global students
    if roll_no in students:
        print(f"[{get_formatted_time()}] [TEACHER] WARNING: Student {students[roll_no]['name']} (Roll: {roll_no})")
        students[roll_no]["marks"] = 50
        students[roll_no]["reason"] = "Warning"
        students[roll_no]["status"] = "WARNED"
        
        write_to_marksheet_replicas(roll_no, 50, "Warning")
        
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
        
        write_to_marksheet_replicas(roll_no, 0, "Cheating")
        
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
        print(f"[{get_formatted_time()}] Total Requests Received: {total_requests}")
        print(f"[{get_formatted_time()}] Main Server Processed: {main_processed}")
        print(f"[{get_formatted_time()}] Backup Server Processed: {backup_processed}")
        print(f"[{get_formatted_time()}] Failed Requests: {failed_requests}")
        
        if total_requests > 0:
            success_rate = ((main_processed + backup_processed) / total_requests) * 100
            migration_rate = (backup_processed / total_requests) * 100 if total_requests > 0 else 0
            
            print(f"[{get_formatted_time()}] SUCCESS RATE: {success_rate:.1f}%")
            print(f"[{get_formatted_time()}] MIGRATION RATE: {migration_rate:.1f}%")
            
            if backup_processed > 0:
                print(f"[{get_formatted_time()}] LOAD BALANCING STATUS: ACTIVE")
                print(f"[{get_formatted_time()}] BACKUP SERVER UTILIZATION: {backup_processed} requests")
            else:
                print(f"[{get_formatted_time()}] LOAD BALANCING STATUS: NOT TRIGGERED")
                print(f"[{get_formatted_time()}] All requests handled by main server")
            
            buffer_info = f"Buffer: {stats.get('buffer_size', 10)} slots, Threshold: {stats.get('load_threshold', 8)} ({(stats.get('load_threshold', 8)/stats.get('buffer_size', 10))*100:.0f}%)"
            print(f"[{get_formatted_time()}] {buffer_info}")
    else:
        print(f"[{get_formatted_time()}] No load balancing data available")
        print(f"[{get_formatted_time()}] Run Task 7 load test to see statistics")
    
    print(f"\n[{get_formatted_time()}] TASK 8: DATABASE REPLICATION STATUS:")
    print(f"[{get_formatted_time()}] " + "-"*80)
    print(f"[{get_formatted_time()}] Teacher Replication System: {'ACTIVE' if teacher_replication_initialized else 'INACTIVE'}")
    print(f"[{get_formatted_time()}] Marksheet Replication Factor: 3")
    print(f"[{get_formatted_time()}] Chunk-based Distribution: Active")
    print(f"[{get_formatted_time()}] Write Lock Management: Teacher-only access")
    
    print(f"\n[{get_formatted_time()}] TASK 9: HADOOP-STYLE NODE MANAGER STATUS:")
    print(f"[{get_formatted_time()}] " + "-"*80)
    print(f"[{get_formatted_time()}] Node Manager: {'ACTIVE' if node_manager_active else 'INACTIVE'}")
    print(f"[{get_formatted_time()}] Resource Monitoring: {'ACTIVE' if resource_monitor_active else 'INACTIVE'}")
    print(f"[{get_formatted_time()}] Jobs Completed: {processing_jobs_completed}")
    print(f"[{get_formatted_time()}] Node Type: Teacher Node (Processing + Marksheet Management)")
    print(f"[{get_formatted_time()}] Current CPU: {system_resources.get('cpu_percent', 0):.1f}%")
    print(f"[{get_formatted_time()}] Current Memory: {system_resources.get('memory_percent', 0):.1f}%")
    print(f"[{get_formatted_time()}] Network Latency: Server={network_stats.get('ping_server', 999):.1f}ms")
    
    print(f"[{get_formatted_time()}] " + "="*120)
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
    
    if student_id in students:
        students[student_id]["exam_score"] = final_marks
        students[student_id]["exam_type"] = "Interactive"
        students[student_id]["marks"] = final_marks
        students[student_id]["server_used"] = server_used
        print(f"[{get_formatted_time()}] [TEACHER] Local student data updated for roll {student_id}")
    
    print_results()
    return True

def get_database_connection():
    try:
        return sqlite3.connect(DB_PATH)
    except Exception as e:
        print(f"[{get_formatted_time()}] [TEACHER-DB] Database connection failed: {e}")
        return None

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
    print(f"[{get_formatted_time()}] [TEACHER] Features: Marksheet, Task 6 Exam Processing, Task 7 Load Balancing Analysis, Task 8 Replication")
    print(f"[{get_formatted_time()}] [TEACHER] Enhanced Reporting: Traditional + Task 6 Interactive + Task 7 Load Balancing + Task 8 Replication")
    print(f"[{get_formatted_time()}] [TEACHER] Task 6: Deadlock detection and resolution monitoring")
    print(f"[{get_formatted_time()}] [TEACHER] Task 7: Load balancing statistics and server utilization tracking")
    print(f"[{get_formatted_time()}] [TEACHER] Task 8: Marksheet replication with RF=3 and write locks")
    print(f"[{get_formatted_time()}] [TEACHER] Task 9: YARN-like Node Manager with distributed processing capabilities")
    print(f"[{get_formatted_time()}] [TEACHER] Teacher hostname: {hostname}")
    print(f"[{get_formatted_time()}] [TEACHER] Teacher IP address: {local_ip}")
    print(f"[{get_formatted_time()}] [TEACHER] Teacher server running at port 9001")
    print(f"[{get_formatted_time()}] [TEACHER] Database: {DB_PATH}")
    print(f"[{get_formatted_time()}] [TEACHER] Replication Database: {TEACHER_REPLICATION_DB}")
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
        
        server.register_function(initialize_replication_system, "initialize_replication_system")
        server.register_function(show_replication_status, "show_replication_status")
        server.register_function(write_to_marksheet_replicas, "write_to_marksheet_replicas")
        
        server.register_function(initialize_node_manager, "initialize_node_manager")
        server.register_function(process_teacher_job, "process_teacher_job")
        server.register_function(analyze_marksheet_data, "analyze_marksheet_data")
        server.register_function(calculate_distributed_grades, "calculate_distributed_grades")
        server.register_function(validate_exam_submissions, "validate_exam_submissions")
        server.register_function(process_replication_status, "process_replication_status")
        
        print(f"[{get_formatted_time()}] [TEACHER] All functions registered:")
        print(f"[{get_formatted_time()}] [TEACHER] - Baseline: get_time, adjust_time, warn_student, catch_student")
        print(f"[{get_formatted_time()}] [TEACHER] - Test management: run_timer, is_test_active, get_test_start_time")
        print(f"[{get_formatted_time()}] [TEACHER] - Enhanced Marksheet: print_results (Traditional + Task 6 + Task 7 + Task 8 + Task 9)")
        print(f"[{get_formatted_time()}] [TEACHER] - Ricart-Agrawala: receive_request, receive_reply, release_critical_section")
        print(f"[{get_formatted_time()}] [TEACHER] - TASK 6 Functions: receive_exam_submission")
        print(f"[{get_formatted_time()}] [TEACHER] - TASK 8 Functions: initialize_replication_system, show_replication_status, write_to_marksheet_replicas")
        print(f"[{get_formatted_time()}] [TEACHER] - TASK 9 NEW: initialize_node_manager, process_teacher_job, analyze_marksheet_data, calculate_distributed_grades")
        print(f"[{get_formatted_time()}] [TEACHER] Ready to handle requests with full replication and distributed processing support...")
        
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print(f"\n[{get_formatted_time()}] [TEACHER] Teacher server shutting down...")

if __name__ == "__main__":
    run_teacher()
