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
import psutil

time_now = None
clients = {}
client_url = ""
teacher_url = ""
student_status = {i: 0 for i in [29, 40, 42, 50, 52]}
teacher_proxy = None

logical_clock = 0
request_queue = queue.PriorityQueue()
deferred_replies = []
requesting_cs = False
in_cs = False
num_nodes = 3
replies_received = 0
cs_lock = threading.Lock()
my_process_id = "server"

DB_PATH = "exam_system.db"
REPLICATION_DB_PATH = "marksheet_replicated.db"
db_lock = threading.Lock()
replication_lock = threading.Lock()
exam_timer = 60

BACKUP_PORT = 8001
main_server_buffer = queue.Queue(maxsize=8)
backup_server_buffer = queue.Queue(maxsize=8)
load_threshold = 5

total_requests = 0
main_server_processed = 0
backup_server_processed = 0
failed_requests = 0

replication_initialized = False
chunk_metadata = {}
replica_status = {}
read_locks = {}
write_locks = {}

cluster_resources = {}
job_queue = queue.Queue()
processing_jobs = {}
completed_jobs = []
failed_jobs = []
resource_manager_active = False
job_counter = 0

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

main_exam_sessions = {}
backup_exam_sessions = {}
submission_lock = threading.Lock()
deadlock_detection_lock = threading.Lock()
status_db = {}
submission_db = {}

backup_server_instance = None
backup_running = False

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

def init_database():
    with db_lock:
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS status_db (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    student_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    exam_type TEXT DEFAULT 'Interactive'
                )
            ''')
            
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
                    exam_duration INTEGER,
                    server_used TEXT DEFAULT 'main',
                    final_marks INTEGER DEFAULT 0,
                    result TEXT DEFAULT 'FAIL'
                )
            ''')
            
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
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processing_jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    parameters TEXT,
                    status TEXT DEFAULT 'queued',
                    submitted_by TEXT,
                    submit_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    start_time TIMESTAMP,
                    completion_time TIMESTAMP,
                    result TEXT,
                    error_message TEXT,
                    assigned_node TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            
            print(f"[{get_formatted_time()}] [SERVER-DB] All databases initialized")
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER-DB] Database initialization failed: {e}")

def initialize_resource_manager():
    global resource_manager_active
    
    if resource_manager_active:
        return True
    
    print(f"[{get_formatted_time()}] [SERVER-TASK9] Initializing YARN-like Resource Manager...")
    print(f"[{get_formatted_time()}] [SERVER-TASK9] Starting cluster monitoring and job scheduling...")
    
    def resource_manager_thread():
        global resource_manager_active
        resource_manager_active = True
        
        while resource_manager_active:
            try:
                process_job_queue()
                monitor_cluster_health()
                cleanup_old_jobs()
                time.sleep(5)
            except Exception as e:
                print(f"[{get_formatted_time()}] [SERVER-TASK9] Resource Manager error: {e}")
                time.sleep(2)
    
    rm_thread = threading.Thread(target=resource_manager_thread, daemon=True)
    rm_thread.start()
    
    print(f"[{get_formatted_time()}] [SERVER-TASK9] Resource Manager active - accepting job submissions")
    return True

def report_node_metrics(metrics):
    try:
        node_id = metrics.get("node_id", "unknown")
        global cluster_resources
        
        cluster_resources[node_id] = metrics
        
        resources = metrics.get("resources", {})
        network = metrics.get("network", {})
        
        cpu = resources.get("cpu_percent", 0)
        memory = resources.get("memory_percent", 0)
        ping_server = network.get("ping_server", 999)
        ping_teacher = network.get("ping_teacher", 999)
        
        if cpu > 90 or memory > 90:
            status = "OVERLOADED"
        elif ping_server > 100 or ping_teacher > 100:
            status = "HIGH_LATENCY"
        else:
            status = "HEALTHY"
        
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Node {node_id}: CPU={cpu:.1f}% MEM={memory:.1f}% NET={ping_server:.1f}ms STATUS={status}")
        
        return {"success": True, "status": "metrics_recorded"}
        
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Error recording node metrics: {e}")
        return {"success": False, "message": str(e)}

def get_cluster_resources():
    try:
        return {
            "success": True,
            "resources": cluster_resources,
            "cluster_status": "active",
            "total_nodes": len(cluster_resources)
        }
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Error getting cluster resources: {e}")
        return {"success": False, "message": str(e)}

def submit_processing_job(job):
    try:
        global job_counter
        job_counter += 1
        
        job_id = job.get("job_id", f"job_{job_counter}")
        job_type = job.get("job_type", "UNKNOWN")
        parameters = job.get("parameters", {})
        
        job_record = {
            "job_id": job_id,
            "job_type": job_type,
            "parameters": json.dumps(parameters),
            "status": "queued",
            "submitted_by": job.get("submitted_by", "unknown"),
            "submit_time": time.time()
        }
        
        processing_jobs[job_id] = job_record
        job_queue.put(job_record)
        
        with db_lock:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO processing_jobs 
                (job_id, job_type, parameters, status, submitted_by, submit_time)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (job_id, job_type, json.dumps(parameters), "queued", 
                  job.get("submitted_by", "unknown"), datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
        
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Job {job_id} ({job_type}) queued for processing")
        
        return {
            "success": True,
            "job_id": job_id,
            "message": "Job queued successfully",
            "queue_position": job_queue.qsize()
        }
        
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Error submitting job: {e}")
        return {"success": False, "message": str(e)}

def get_job_status(job_id):
    try:
        if job_id in processing_jobs:
            job = processing_jobs[job_id]
            return {
                "success": True,
                "job": job
            }
        
        for job in completed_jobs:
            if job["job_id"] == job_id:
                return {
                    "success": True,
                    "job": job
                }
        
        for job in failed_jobs:
            if job["job_id"] == job_id:
                return {
                    "success": True,
                    "job": job
                }
        
        return {"success": False, "message": "Job not found"}
        
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Error getting job status: {e}")
        return {"success": False, "message": str(e)}

def get_job_queue_status():
    try:
        queued_jobs = len([j for j in processing_jobs.values() if j["status"] == "queued"])
        running_jobs = len([j for j in processing_jobs.values() if j["status"] == "running"])
        
        recent_jobs = []
        all_jobs = list(processing_jobs.values()) + completed_jobs + failed_jobs
        all_jobs.sort(key=lambda x: x.get("submit_time", 0), reverse=True)
        
        return {
            "success": True,
            "queue": {
                "total_jobs": len(all_jobs),
                "queued_jobs": queued_jobs,
                "running_jobs": running_jobs,
                "completed_jobs": len(completed_jobs),
                "failed_jobs": len(failed_jobs),
                "recent_jobs": all_jobs[:10]
            }
        }
        
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Error getting job queue status: {e}")
        return {"success": False, "message": str(e)}

def process_job_queue():
    try:
        if not job_queue.empty():
            job = job_queue.get_nowait()
            
            if select_best_node_for_job(job):
                execute_processing_job(job)
            else:
                job_queue.put(job)
                
    except queue.Empty:
        pass
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Job processing error: {e}")

def select_best_node_for_job(job):
    try:
        available_nodes = []
        
        for node_id, metrics in cluster_resources.items():
            if metrics:
                resources = metrics.get("resources", {})
                last_update = time.time() - metrics.get("heartbeat", 0)
                
                if last_update < 30:
                    cpu = resources.get("cpu_percent", 100)
                    memory = resources.get("memory_percent", 100)
                    
                    if cpu < 80 and memory < 80:
                        available_nodes.append({
                            "node_id": node_id,
                            "cpu": cpu,
                            "memory": memory,
                            "score": cpu + memory
                        })
        
        if available_nodes:
            best_node = min(available_nodes, key=lambda x: x["score"])
            job["assigned_node"] = best_node["node_id"]
            print(f"[{get_formatted_time()}] [SERVER-TASK9] Job {job['job_id']} assigned to node {best_node['node_id']}")
            return True
        else:
            print(f"[{get_formatted_time()}] [SERVER-TASK9] No available nodes for job {job['job_id']}")
            return False
            
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Node selection error: {e}")
        return False

def execute_processing_job(job):
    def job_execution_thread():
        try:
            job_id = job["job_id"]
            job_type = job["job_type"]
            
            processing_jobs[job_id]["status"] = "running"
            processing_jobs[job_id]["start_time"] = time.time()
            
            print(f"[{get_formatted_time()}] [SERVER-TASK9] Executing job {job_id} ({job_type}) on node {job.get('assigned_node', 'server')}")
            
            result = None
            if job_type == "COUNT_STUDENTS":
                result = execute_count_students_job()
            elif job_type == "AVERAGE_SCORES":
                result = execute_average_scores_job()
            elif job_type == "FILTER_BY_SCORE":
                parameters = json.loads(job.get("parameters", "{}"))
                min_score = parameters.get("min_score", 80)
                result = execute_filter_by_score_job(min_score)
            elif job_type == "GENERATE_REPORT":
                result = execute_generate_report_job()
            elif job_type == "PROCESS_SUBMISSIONS":
                result = execute_process_submissions_job()
            else:
                result = {"error": f"Unknown job type: {job_type}"}
            
            if "error" in result:
                processing_jobs[job_id]["status"] = "failed"
                processing_jobs[job_id]["error_message"] = result["error"]
                failed_jobs.append(processing_jobs[job_id])
                print(f"[{get_formatted_time()}] [SERVER-TASK9] Job {job_id} failed: {result['error']}")
            else:
                processing_jobs[job_id]["status"] = "completed"
                processing_jobs[job_id]["result"] = json.dumps(result)
                processing_jobs[job_id]["completion_time"] = time.time()
                completed_jobs.append(processing_jobs[job_id])
                print(f"[{get_formatted_time()}] [SERVER-TASK9] Job {job_id} completed successfully")
                print(f"[{get_formatted_time()}] [SERVER-TASK9] Result: {result}")
            
            del processing_jobs[job_id]
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER-TASK9] Job execution error: {e}")
            if job_id in processing_jobs:
                processing_jobs[job_id]["status"] = "failed"
                processing_jobs[job_id]["error_message"] = str(e)
                failed_jobs.append(processing_jobs[job_id])
                del processing_jobs[job_id]
    
    job_thread = threading.Thread(target=job_execution_thread, daemon=True)
    job_thread.start()

def execute_count_students_job():
    try:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Processing: COUNT_STUDENTS across all chunks")
        
        total_students = 0
        chunk_counts = {}
        
        with replication_lock:
            conn = sqlite3.connect(REPLICATION_DB_PATH)
            cursor = conn.cursor()
            
            for chunk_id in range(3):
                table_name = f"chunk_{chunk_id}_replica_1"
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                
                chunk_counts[f"chunk_{chunk_id}"] = count
                total_students += count
                
                print(f"[{get_formatted_time()}] [SERVER-TASK9] Chunk {chunk_id}: {count} students")
            
            conn.close()
        
        return {
            "job_type": "COUNT_STUDENTS",
            "total_students": total_students,
            "chunk_distribution": chunk_counts,
            "replication_factor": 3
        }
        
    except Exception as e:
        return {"error": str(e)}

def execute_average_scores_job():
    try:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Processing: AVERAGE_SCORES from replicated data")
        
        all_scores = []
        chunk_averages = {}
        
        with replication_lock:
            conn = sqlite3.connect(REPLICATION_DB_PATH)
            cursor = conn.cursor()
            
            for chunk_id in range(3):
                table_name = f"chunk_{chunk_id}_replica_1"
                cursor.execute(f"SELECT roll_number, total FROM {table_name}")
                students = cursor.fetchall()
                
                chunk_scores = [total for roll, total in students]
                chunk_avg = sum(chunk_scores) / len(chunk_scores) if chunk_scores else 0
                
                chunk_averages[f"chunk_{chunk_id}"] = {
                    "average": round(chunk_avg, 2),
                    "student_count": len(chunk_scores),
                    "students": [{"roll": roll, "total": total} for roll, total in students]
                }
                
                all_scores.extend(chunk_scores)
                
                print(f"[{get_formatted_time()}] [SERVER-TASK9] Chunk {chunk_id}: avg={chunk_avg:.2f}")
            
            conn.close()
        
        overall_average = sum(all_scores) / len(all_scores) if all_scores else 0
        
        return {
            "job_type": "AVERAGE_SCORES",
            "overall_average": round(overall_average, 2),
            "total_students": len(all_scores),
            "chunk_averages": chunk_averages
        }
        
    except Exception as e:
        return {"error": str(e)}

def execute_filter_by_score_job(min_score):
    try:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Processing: FILTER_BY_SCORE (min={min_score})")
        
        high_scorers = []
        chunk_results = {}
        
        with replication_lock:
            conn = sqlite3.connect(REPLICATION_DB_PATH)
            cursor = conn.cursor()
            
            for chunk_id in range(3):
                table_name = f"chunk_{chunk_id}_replica_1"
                cursor.execute(f"SELECT roll_number, name, total FROM {table_name} WHERE total >= ?", (min_score,))
                students = cursor.fetchall()
                
                chunk_high_scorers = [{"roll": roll, "name": name, "total": total} for roll, name, total in students]
                chunk_results[f"chunk_{chunk_id}"] = chunk_high_scorers
                high_scorers.extend(chunk_high_scorers)
                
                print(f"[{get_formatted_time()}] [SERVER-TASK9] Chunk {chunk_id}: {len(chunk_high_scorers)} students above {min_score}")
            
            conn.close()
        
        return {
            "job_type": "FILTER_BY_SCORE",
            "min_score": min_score,
            "total_high_scorers": len(high_scorers),
            "high_scorers": high_scorers,
            "chunk_results": chunk_results
        }
        
    except Exception as e:
        return {"error": str(e)}

def execute_generate_report_job():
    try:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Processing: GENERATE_REPORT distributed analysis")
        
        report = {
            "job_type": "GENERATE_REPORT",
            "generated_at": datetime.now().isoformat(),
            "replication_analysis": {},
            "system_analysis": {}
        }
        
        with replication_lock:
            conn = sqlite3.connect(REPLICATION_DB_PATH)
            cursor = conn.cursor()
            
            for chunk_id in range(3):
                table_name = f"chunk_{chunk_id}_replica_1"
                cursor.execute(f"SELECT COUNT(*), AVG(total), MIN(total), MAX(total) FROM {table_name}")
                stats = cursor.fetchone()
                
                count, avg_score, min_score, max_score = stats
                
                report["replication_analysis"][f"chunk_{chunk_id}"] = {
                    "student_count": count,
                    "average_score": round(avg_score, 2) if avg_score else 0,
                    "min_score": min_score if min_score else 0,
                    "max_score": max_score if max_score else 0
                }
            
            conn.close()
        
        report["system_analysis"] = {
            "total_chunks": 3,
            "replication_factor": 3,
            "cluster_nodes": len(cluster_resources),
            "active_jobs": len(processing_jobs),
            "completed_jobs": len(completed_jobs)
        }
        
        return report
        
    except Exception as e:
        return {"error": str(e)}

def execute_process_submissions_job():
    try:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Processing: PROCESS_SUBMISSIONS analysis")
        
        with db_lock:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM submission_db")
            total_submissions = cursor.fetchone()[0]
            
            cursor.execute("SELECT server_used, COUNT(*) FROM submission_db GROUP BY server_used")
            server_distribution = dict(cursor.fetchall())
            
            cursor.execute("SELECT submission_type, COUNT(*) FROM submission_db GROUP BY submission_type")
            submission_types = dict(cursor.fetchall())
            
            cursor.execute("SELECT AVG(score), AVG(final_marks) FROM submission_db")
            avg_stats = cursor.fetchone()
            
            conn.close()
        
        return {
            "job_type": "PROCESS_SUBMISSIONS",
            "total_submissions": total_submissions,
            "server_distribution": server_distribution,
            "submission_types": submission_types,
            "average_score": round(avg_stats[0], 2) if avg_stats[0] else 0,
            "average_final_marks": round(avg_stats[1], 2) if avg_stats[1] else 0
        }
        
    except Exception as e:
        return {"error": str(e)}

def monitor_cluster_health():
    try:
        current_time = time.time()
        offline_nodes = []
        
        for node_id, metrics in cluster_resources.items():
            last_heartbeat = metrics.get("heartbeat", 0)
            if current_time - last_heartbeat > 60:
                offline_nodes.append(node_id)
        
        if offline_nodes:
            print(f"[{get_formatted_time()}] [SERVER-TASK9] Offline nodes detected: {offline_nodes}")
            
        for node_id in offline_nodes:
            del cluster_resources[node_id]
            
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Cluster health monitoring error: {e}")

def cleanup_old_jobs():
    try:
        current_time = time.time()
        
        global completed_jobs, failed_jobs
        completed_jobs = [job for job in completed_jobs if current_time - job.get("completion_time", 0) < 3600]
        failed_jobs = [job for job in failed_jobs if current_time - job.get("submit_time", 0) < 3600]
        
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK9] Job cleanup error: {e}")

def initialize_replication_system():
    global replication_initialized
    
    if replication_initialized:
        return True
    
    print(f"[{get_formatted_time()}] [SERVER-TASK8] Initializing Database Replication System...")
    print(f"[{get_formatted_time()}] [SERVER-TASK8] Replication Factor: 3")
    print(f"[{get_formatted_time()}] [SERVER-TASK8] Creating chunk-based replicated database...")
    
    def replication_background_task():
        setup_replication_database()
        create_chunks_and_replicas()
        start_replication_sync_process()
    
    replication_thread = threading.Thread(target=replication_background_task, daemon=True)
    replication_thread.start()
    
    time.sleep(1)
    replication_initialized = True
    print(f"[{get_formatted_time()}] [SERVER-TASK8] Replication system activated")
    return True

def setup_replication_database():
    with replication_lock:
        try:
            conn = sqlite3.connect(REPLICATION_DB_PATH)
            cursor = conn.cursor()
            
            print(f"[{get_formatted_time()}] [SERVER-TASK8] Setting up chunk replica tables...")
            
            for chunk_id in range(3):
                for replica_id in range(1, 4):
                    table_name = f"chunk_{chunk_id}_replica_{replica_id}"
                    cursor.execute(f'''
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            roll_number INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            ISA INTEGER DEFAULT 0,
                            MSE INTEGER DEFAULT 0,
                            ESE INTEGER DEFAULT 0,
                            total INTEGER DEFAULT 0,
                            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            version INTEGER DEFAULT 1
                        )
                    ''')
                    print(f"[{get_formatted_time()}] [SERVER-TASK8] Created table: {table_name}")
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS chunk_metadata (
                    chunk_id INTEGER PRIMARY KEY,
                    student_range TEXT,
                    replica_count INTEGER DEFAULT 3,
                    status TEXT DEFAULT 'active'
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS replica_status (
                    chunk_id INTEGER,
                    replica_id INTEGER,
                    node_location TEXT,
                    status TEXT DEFAULT 'online',
                    last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chunk_id, replica_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER-TASK8] Replication database setup failed: {e}")

def create_chunks_and_replicas():
    students_data = [
        {"roll": 29, "name": "Mayuresh", "ISA": 95, "MSE": 87, "ESE": 92},
        {"roll": 40, "name": "Ayush", "ISA": 88, "MSE": 90, "ESE": 85},
        {"roll": 42, "name": "Aashna", "ISA": 92, "MSE": 85, "ESE": 88},
        {"roll": 50, "name": "Rohit", "ISA": 85, "MSE": 92, "ESE": 90},
        {"roll": 52, "name": "Rushikesh", "ISA": 90, "MSE": 88, "ESE": 95}
    ]
    
    chunks = [
        {"chunk_id": 0, "students": students_data[0:2], "range": "29-40"},
        {"chunk_id": 1, "students": students_data[2:4], "range": "42-50"},
        {"chunk_id": 2, "students": students_data[4:5], "range": "52"}
    ]
    
    with replication_lock:
        try:
            conn = sqlite3.connect(REPLICATION_DB_PATH)
            cursor = conn.cursor()
            
            for chunk in chunks:
                chunk_id = chunk["chunk_id"]
                
                cursor.execute('''
                    INSERT OR REPLACE INTO chunk_metadata (chunk_id, student_range, replica_count, status)
                    VALUES (?, ?, 3, 'active')
                ''', (chunk_id, chunk["range"]))
                
                print(f"[{get_formatted_time()}] [SERVER-TASK8] Creating Chunk {chunk_id} with students {chunk['range']}")
                
                for replica_id in range(1, 4):
                    table_name = f"chunk_{chunk_id}_replica_{replica_id}"
                    
                    cursor.execute(f"DELETE FROM {table_name}")
                    
                    for student in chunk["students"]:
                        total = student["ISA"] + student["MSE"] + student["ESE"]
                        cursor.execute(f'''
                            INSERT INTO {table_name} 
                            (roll_number, name, ISA, MSE, ESE, total, last_updated, version)
                            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 1)
                        ''', (student["roll"], student["name"], student["ISA"], 
                              student["MSE"], student["ESE"], total))
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO replica_status 
                        (chunk_id, replica_id, node_location, status, last_sync)
                        VALUES (?, ?, ?, 'online', CURRENT_TIMESTAMP)
                    ''', (chunk_id, replica_id, f"node_{replica_id}"))
                    
                    print(f"[{get_formatted_time()}] [SERVER-TASK8] Replicated Chunk {chunk_id} to Node {replica_id}")
            
            conn.commit()
            conn.close()
            
            print(f"[{get_formatted_time()}] [SERVER-TASK8] All chunks replicated successfully with RF=3")
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER-TASK8] Chunk creation failed: {e}")

def start_replication_sync_process():
    def sync_replicas():
        while True:
            time.sleep(15)
            
            try:
                with replication_lock:
                    conn = sqlite3.connect(REPLICATION_DB_PATH)
                    cursor = conn.cursor()
                    
                    cursor.execute("SELECT chunk_id, student_range FROM chunk_metadata WHERE status='active'")
                    chunks = cursor.fetchall()
                    
                    for chunk_id, student_range in chunks:
                        
                        primary_table = f"chunk_{chunk_id}_replica_1"
                        cursor.execute(f"SELECT COUNT(*) FROM {primary_table}")
                        record_count = cursor.fetchone()[0]
                        
                        if record_count > 0:
                            print(f"[{get_formatted_time()}] [SERVER-TASK8] Sync check: Chunk {chunk_id} ({student_range}) - {record_count} records consistent across 3 replicas")
                    
                    conn.close()
                    
            except Exception as e:
                print(f"[{get_formatted_time()}] [SERVER-TASK8] Sync process error: {e}")
    
    sync_thread = threading.Thread(target=sync_replicas, daemon=True)
    sync_thread.start()

def acquire_read_lock(chunk_id):
    if chunk_id not in read_locks:
        read_locks[chunk_id] = 0
    if chunk_id not in write_locks:
        write_locks[chunk_id] = False
    
    while write_locks[chunk_id]:
        time.sleep(0.1)
    
    read_locks[chunk_id] += 1
    print(f"[{get_formatted_time()}] [SERVER-TASK8] Read lock acquired for Chunk {chunk_id} (readers: {read_locks[chunk_id]})")
    return True

def release_read_lock(chunk_id):
    if chunk_id in read_locks:
        read_locks[chunk_id] = max(0, read_locks[chunk_id] - 1)
        print(f"[{get_formatted_time()}] [SERVER-TASK8] Read lock released for Chunk {chunk_id} (readers: {read_locks[chunk_id]})")

def acquire_write_lock(chunk_id):
    if chunk_id not in read_locks:
        read_locks[chunk_id] = 0
    if chunk_id not in write_locks:
        write_locks[chunk_id] = False
    
    while write_locks[chunk_id] or read_locks[chunk_id] > 0:
        time.sleep(0.1)
    
    write_locks[chunk_id] = True
    print(f"[{get_formatted_time()}] [SERVER-TASK8] Write lock acquired for Chunk {chunk_id} (exclusive access)")
    return True

def release_write_lock(chunk_id):
    if chunk_id in write_locks:
        write_locks[chunk_id] = False
        print(f"[{get_formatted_time()}] [SERVER-TASK8] Write lock released for Chunk {chunk_id}")

def read_from_replicas(student_id):
    chunk_id = get_chunk_for_student(student_id)
    
    if acquire_read_lock(chunk_id):
        try:
            with replication_lock:
                conn = sqlite3.connect(REPLICATION_DB_PATH)
                cursor = conn.cursor()
                
                table_name = f"chunk_{chunk_id}_replica_1"
                cursor.execute(f'''
                    SELECT roll_number, name, ISA, MSE, ESE, total 
                    FROM {table_name} WHERE roll_number = ?
                ''', (student_id,))
                
                result = cursor.fetchone()
                conn.close()
                
                if result:
                    print(f"[{get_formatted_time()}] [SERVER-TASK8] Read student {student_id} from Chunk {chunk_id}")
                    return {
                        "roll": result[0],
                        "name": result[1],
                        "ISA": result[2],
                        "MSE": result[3],
                        "ESE": result[4],
                        "total": result[5]
                    }
                return None
                
        finally:
            release_read_lock(chunk_id)
    
    return None

def write_to_replicas(student_id, updates):
    chunk_id = get_chunk_for_student(student_id)
    
    if acquire_write_lock(chunk_id):
        try:
            print(f"[{get_formatted_time()}] [SERVER-TASK8] Writing updates for student {student_id} to all replicas...")
            
            with replication_lock:
                conn = sqlite3.connect(REPLICATION_DB_PATH)
                cursor = conn.cursor()
                
                for replica_id in range(1, 4):
                    table_name = f"chunk_{chunk_id}_replica_{replica_id}"
                    
                    total = updates.get("ISA", 0) + updates.get("MSE", 0) + updates.get("ESE", 0)
                    
                    cursor.execute(f'''
                        UPDATE {table_name} 
                        SET ISA = ?, MSE = ?, ESE = ?, total = ?, 
                            last_updated = CURRENT_TIMESTAMP, version = version + 1
                        WHERE roll_number = ?
                    ''', (updates.get("ISA", 0), updates.get("MSE", 0), 
                          updates.get("ESE", 0), total, student_id))
                    
                    print(f"[{get_formatted_time()}] [SERVER-TASK8] Updated student {student_id} in {table_name}")
                
                conn.commit()
                conn.close()
                
                print(f"[{get_formatted_time()}] [SERVER-TASK8] All replicas updated successfully for student {student_id}")
                return True
                
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER-TASK8] Write to replicas failed: {e}")
            return False
        finally:
            release_write_lock(chunk_id)
    
    return False

def get_chunk_for_student(student_id):
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
    print(f"[{get_formatted_time()}] TASK 8: DATABASE REPLICATION STATUS")
    print(f"[{get_formatted_time()}] " + "="*80)
    
    try:
        with replication_lock:
            conn = sqlite3.connect(REPLICATION_DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("SELECT chunk_id, student_range, replica_count, status FROM chunk_metadata")
            chunks = cursor.fetchall()
            
            print(f"[{get_formatted_time()}] CHUNK DISTRIBUTION:")
            for chunk_id, student_range, replica_count, status in chunks:
                print(f"[{get_formatted_time()}] Chunk {chunk_id}: Students {student_range} | Replicas: {replica_count} | Status: {status}")
            
            print(f"\n[{get_formatted_time()}] REPLICA STATUS:")
            cursor.execute("SELECT chunk_id, replica_id, node_location, status FROM replica_status ORDER BY chunk_id, replica_id")
            replicas = cursor.fetchall()
            
            for chunk_id, replica_id, node_location, status in replicas:
                print(f"[{get_formatted_time()}] Chunk {chunk_id} Replica {replica_id}: {node_location} ({status})")
            
            print(f"\n[{get_formatted_time()}] SAMPLE DATA FROM REPLICAS:")
            for chunk_id in range(3):
                table_name = f"chunk_{chunk_id}_replica_1"
                cursor.execute(f"SELECT roll_number, name, total FROM {table_name} LIMIT 2")
                students = cursor.fetchall()
                
                for roll, name, total in students:
                    print(f"[{get_formatted_time()}] Chunk {chunk_id}: {roll} ({name}) Total: {total}")
            
            print(f"\n[{get_formatted_time()}] LOCK STATUS:")
            for chunk_id in range(3):
                readers = read_locks.get(chunk_id, 0)
                writers = "LOCKED" if write_locks.get(chunk_id, False) else "FREE"
                print(f"[{get_formatted_time()}] Chunk {chunk_id}: Read locks: {readers} | Write lock: {writers}")
            
            conn.close()
    
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-TASK8] Error showing replication status: {e}")
    
    print(f"[{get_formatted_time()}] " + "="*80)
    return True

def start_backup_server():
    global backup_server_instance, backup_running
    
    def backup_server_thread():
        global backup_running
        backup_running = True
        
        try:
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            
            print(f"[{get_formatted_time()}] [BACKUP-SERVER] Starting on {local_ip}:{BACKUP_PORT}")
            
            backup_server_instance = SimpleXMLRPCServer(("0.0.0.0", BACKUP_PORT), allow_none=True)
            
            backup_server_instance.register_function(backup_start_interactive_exam, "start_interactive_exam")
            backup_server_instance.register_function(backup_get_question, "get_question")
            backup_server_instance.register_function(backup_submit_answer, "submit_answer")
            backup_server_instance.register_function(backup_submit_exam_final, "submit_exam_final")
            
            print(f"[{get_formatted_time()}] [BACKUP-SERVER] Ready to serve on port {BACKUP_PORT}")
            
            backup_server_instance.serve_forever()
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [BACKUP-SERVER] Error: {e}")
            backup_running = False
    
    backup_thread = threading.Thread(target=backup_server_thread, daemon=True)
    backup_thread.start()
    
    time.sleep(1)
    
    if backup_running:
        print(f"[{get_formatted_time()}] [SERVER-TASK7] Backup server started successfully")
    else:
        print(f"[{get_formatted_time()}] [SERVER-TASK7] Failed to start backup server")

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
        
        test_client = clients["client"].get_time_for_berkeley()
        test_teacher = clients["teacher"].get_time()
        
        print(f"[{get_formatted_time()}] [SERVER] Client and teacher registered successfully")
        
        start_backup_server()
        
        return "Registered client and teacher."
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER] Registration failed: {e}")
        return f"Registration failed: {e}"

def get_time():
    global time_now
    return time_now.isoformat()

def adjust_time(offset):
    global time_now
    time_now += timedelta(seconds=offset)
    print(f"[{get_formatted_time()}] [SERVER] Time adjusted by {offset:.6f} seconds")
    return True

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

def check_load_balancing():
    global total_requests, main_server_processed, backup_server_processed
    
    main_load = main_server_buffer.qsize()
    load_percentage = (main_load / main_server_buffer.maxsize) * 100
    
    print(f"[{get_formatted_time()}] [SERVER-TASK7] Main buffer: {main_load}/{main_server_buffer.maxsize} ({load_percentage:.0f}%)")
    print(f"[{get_formatted_time()}] [SERVER-TASK7] Load threshold: {load_threshold}")
    
    if main_load >= load_threshold:
        print(f"[{get_formatted_time()}] [SERVER-TASK7] LOAD THRESHOLD EXCEEDED - Routing to backup")
        return "backup"
    else:
        print(f"[{get_formatted_time()}] [SERVER-TASK7] Processing on main server")
        return "main"

def start_interactive_exam(student_id):
    global total_requests, main_server_processed, backup_server_processed
    
    total_requests += 1
    
    print(f"[{get_formatted_time()}] [SERVER-TASK7] Request #{total_requests} for student {student_id}")
    
    if student_id in main_exam_sessions or student_id in backup_exam_sessions:
        print(f"[{get_formatted_time()}] [SERVER-TASK7] Student {student_id} already has active session")
        return {"success": False, "message": "Student already has active exam session"}
    
    server_choice = check_load_balancing()
    
    if server_choice == "backup":
        if backup_running:
            return process_exam_on_backup_direct(student_id)
        else:
            print(f"[{get_formatted_time()}] [SERVER-TASK7] Backup unavailable, processing on main")
            return process_exam_on_main(student_id)
    else:
        return process_exam_on_main(student_id)

def process_exam_on_backup_direct(student_id):
    global backup_server_processed
    
    backup_server_processed += 1
    
    print(f"[{get_formatted_time()}] [SERVER-BACKUP] Processing exam for student {student_id}")
    
    exam_session = {
        "student_id": student_id,
        "questions": exam_questions.copy(),
        "current_question": 0,
        "answers": [],
        "score": 0,
        "start_time": time.time(),
        "duration": exam_timer,
        "status": "ACTIVE",
        "server": "backup"
    }
    
    backup_exam_sessions[student_id] = exam_session
    
    try:
        backup_server_buffer.put(f"exam_{student_id}", block=False)
        print(f"[{get_formatted_time()}] [SERVER-BACKUP] Added to backup buffer successfully")
    except queue.Full:
        print(f"[{get_formatted_time()}] [SERVER-BACKUP] Backup buffer full but continuing")
    
    def backup_auto_submit_timer():
        time.sleep(exam_session["duration"])
        if student_id in backup_exam_sessions and backup_exam_sessions[student_id]["status"] == "ACTIVE":
            print(f"[{get_formatted_time()}] [SERVER-BACKUP] Auto-submit for student {student_id}")
            attempt_backup_auto_submission(student_id)
    
    threading.Thread(target=backup_auto_submit_timer, daemon=True).start()
    
    return {
        "success": True,
        "duration": exam_session["duration"],
        "total_questions": len(exam_questions),
        "message": "Backup server exam started successfully",
        "server": "backup"
    }

def process_exam_on_main(student_id):
    global main_server_processed
    
    try:
        main_server_processed += 1
        
        print(f"[{get_formatted_time()}] [SERVER-MAIN] Processing exam for student {student_id}")
        
        exam_session = {
            "student_id": student_id,
            "questions": exam_questions.copy(),
            "current_question": 0,
            "answers": [],
            "score": 0,
            "start_time": time.time(),
            "duration": exam_timer,
            "status": "ACTIVE",
            "server": "main"
        }
        
        main_exam_sessions[student_id] = exam_session
        
        try:
            main_server_buffer.put(f"exam_{student_id}", block=False)
            print(f"[{get_formatted_time()}] [SERVER-MAIN] Added to main buffer")
        except queue.Full:
            print(f"[{get_formatted_time()}] [SERVER-MAIN] Buffer full, redirecting to backup")
            del main_exam_sessions[student_id]
            main_server_processed -= 1
            return process_exam_on_backup_direct(student_id)
        
        def main_auto_submit_timer():
            time.sleep(exam_session["duration"])
            if student_id in main_exam_sessions and main_exam_sessions[student_id]["status"] == "ACTIVE":
                attempt_main_auto_submission(student_id)
        
        threading.Thread(target=main_auto_submit_timer, daemon=True).start()
        
        return {
            "success": True,
            "duration": exam_session["duration"],
            "total_questions": len(exam_questions),
            "message": "Main server exam started successfully",
            "server": "main"
        }
        
    except Exception as e:
        print(f"[{get_formatted_time()}] [SERVER-MAIN] Error processing exam: {e}")
        main_server_processed -= 1
        return process_exam_on_backup_direct(student_id)

def backup_start_interactive_exam(student_id):
    print(f"[{get_formatted_time()}] [BACKUP-SERVER] Processing exam for student {student_id}")
    
    if student_id in backup_exam_sessions:
        return {"success": False, "message": "Exam already in progress on backup"}
    
    exam_session = {
        "student_id": student_id,
        "questions": exam_questions.copy(),
        "current_question": 0,
        "answers": [],
        "score": 0,
        "start_time": time.time(),
        "duration": exam_timer,
        "status": "ACTIVE",
        "server": "backup"
    }
    
    backup_exam_sessions[student_id] = exam_session
    
    try:
        backup_server_buffer.put(f"exam_{student_id}")
        print(f"[{get_formatted_time()}] [BACKUP-SERVER] Added to backup buffer")
    except queue.Full:
        print(f"[{get_formatted_time()}] [BACKUP-SERVER] Backup buffer full")
    
    def backup_auto_submit_timer():
        time.sleep(exam_session["duration"])
        if student_id in backup_exam_sessions and backup_exam_sessions[student_id]["status"] == "ACTIVE":
            print(f"[{get_formatted_time()}] [BACKUP-SERVER] Auto-submit for student {student_id}")
            attempt_backup_auto_submission(student_id)
    
    threading.Thread(target=backup_auto_submit_timer, daemon=True).start()
    
    return {
        "success": True,
        "duration": exam_session["duration"],
        "total_questions": len(exam_questions),
        "message": "Backup server exam started successfully",
        "server": "backup"
    }

def get_question(student_id):
    if student_id in main_exam_sessions:
        return get_question_from_main(student_id)
    elif student_id in backup_exam_sessions:
        return backup_get_question(student_id)
    else:
        return {"success": False, "message": "No active exam session"}

def get_question_from_main(student_id):
    session = main_exam_sessions[student_id]
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

def backup_get_question(student_id):
    if student_id not in backup_exam_sessions:
        return {"success": False, "message": "No active backup exam session"}
    
    session = backup_exam_sessions[student_id]
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
    if student_id in main_exam_sessions:
        return submit_answer_to_main(student_id, answer)
    elif student_id in backup_exam_sessions:
        return backup_submit_answer(student_id, answer)
    else:
        return {"success": False, "message": "No active exam session"}

def submit_answer_to_main(student_id, answer):
    session = main_exam_sessions[student_id]
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

def backup_submit_answer(student_id, answer):
    if student_id not in backup_exam_sessions:
        return {"success": False, "message": "No active backup exam session"}
    
    session = backup_exam_sessions[student_id]
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
    if student_id in main_exam_sessions:
        return submit_exam_final_main(student_id, submission_source)
    elif student_id in backup_exam_sessions:
        return backup_submit_exam_final(student_id, submission_source)
    else:
        return {"success": False, "message": "No active exam session"}

def submit_exam_final_main(student_id, submission_source="manual"):
    print(f"[{get_formatted_time()}] [SERVER-TASK6] FINAL SUBMISSION - MAIN SERVER")
    
    with deadlock_detection_lock:
        session = main_exam_sessions[student_id]
        elapsed_time = time.time() - session["start_time"]
        remaining_time = max(0, session["duration"] - elapsed_time)
        
        deadlock_detected = False
        resolution_strategy = "none"
        
        if submission_source == "manual" and remaining_time <= 1.0:
            deadlock_detected = True
            resolution_strategy = "manual_priority"
            print(f"[{get_formatted_time()}] [SERVER-TASK6] DEADLOCK DETECTED AND RESOLVED")
        
        final_score = session["score"]
        submission_type = "manual" if submission_source == "manual" else "auto"
        
        update_task6_databases(student_id, final_score, submission_type, deadlock_detected, resolution_strategy, session, "main")
        
        try:
            main_server_buffer.get(block=False)
        except queue.Empty:
            pass
        
        del main_exam_sessions[student_id]
        
        try:
            clients["teacher"].receive_exam_submission(student_id, final_score, submission_type)
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER] Failed to notify teacher: {e}")
        
        return {
            "success": True,
            "score": final_score,
            "type": submission_type,
            "deadlock_resolved": deadlock_detected,
            "resolution_strategy": resolution_strategy,
            "server_used": "main",
            "message": f"Main server submission - {submission_type}"
        }

def backup_submit_exam_final(student_id, submission_source="manual"):
    print(f"[{get_formatted_time()}] [BACKUP-SERVER] FINAL SUBMISSION")
    
    session = backup_exam_sessions[student_id]
    final_score = session["score"]
    submission_type = "manual" if submission_source == "manual" else "auto"
    
    update_task6_databases(student_id, final_score, submission_type, False, "none", session, "backup")
    
    try:
        backup_server_buffer.get(block=False)
    except queue.Empty:
        pass
    
    del backup_exam_sessions[student_id]
    
    try:
        clients["teacher"].receive_exam_submission(student_id, final_score, submission_type)
    except Exception as e:
        print(f"[{get_formatted_time()}] [BACKUP] Failed to notify teacher: {e}")
    
    return {
        "success": True,
        "score": final_score,
        "type": submission_type,
        "server_used": "backup",
        "message": f"Backup server submission - {submission_type}"
    }

def attempt_main_auto_submission(student_id):
    if student_id not in main_exam_sessions:
        return
    
    session = main_exam_sessions[student_id]
    if session.get("manual_submission_attempted", False):
        return
    
    try:
        clients["client"].handle_exam_timeout(student_id)
    except Exception as e:
        pass
    
    return submit_exam_final_main(student_id, "auto")

def attempt_backup_auto_submission(student_id):
    if student_id not in backup_exam_sessions:
        return
    
    session = backup_exam_sessions[student_id]
    if session.get("manual_submission_attempted", False):
        return
    
    try:
        clients["client"].handle_exam_timeout(student_id)
    except Exception as e:
        pass
    
    return backup_submit_exam_final(student_id, "auto")

def update_task6_databases(student_id, score, submission_type, deadlock_detected, resolution_strategy, session, server_used="main"):
    with db_lock:
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO status_db (student_id, status, exam_type) 
                VALUES (?, ?, ?)
            ''', (student_id, "exam_submitted", "Interactive"))
            
            final_marks = score * 10
            result = "PASS" if final_marks >= 50 else "FAIL"
            
            answers_json = json.dumps(session.get("answers", []))
            cursor.execute('''
                INSERT INTO submission_db 
                (student_id, score, submission_type, deadlock_detected, resolution_strategy, answers, exam_duration, server_used, final_marks, result)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (student_id, score, submission_type, deadlock_detected, 
                  resolution_strategy, answers_json, session.get("duration", exam_timer), server_used, final_marks, result))
            
            conn.commit()
            conn.close()
            
            print(f"[{get_formatted_time()}] [SERVER] Databases updated - server: {server_used}, result: {result}")
            
        except Exception as e:
            print(f"[{get_formatted_time()}] [SERVER-DB] Failed to update databases: {e}")

def get_load_balancer_stats():
    return {
        "total_requests": total_requests,
        "main_processed": main_server_processed,
        "backup_processed": backup_server_processed,
        "failed_requests": failed_requests,
        "buffer_size": main_server_buffer.maxsize,
        "current_main_load": main_server_buffer.qsize(),
        "current_backup_load": backup_server_buffer.qsize(),
        "load_threshold": load_threshold,
        "success_rate": ((main_server_processed + backup_server_processed) / total_requests * 100) if total_requests > 0 else 0,
        "backup_running": backup_running
    }

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
    print(f"[{get_formatted_time()}] [SERVER-RA] Received REQUEST from {sender_id}")
    
    should_reply = True
    if requesting_cs or in_cs:
        if timestamp > logical_clock or (timestamp == logical_clock and sender_id > my_process_id):
            should_reply = False
            deferred_replies.append(sender_id)
    
    if should_reply:
        send_reply(sender_id)
    
    return True

def send_reply(receiver_id):
    global logical_clock
    timestamp = increment_clock()
    
    try:
        if receiver_id == "client":
            success = send_request_with_timeout(client_url, "receive_reply", my_process_id, timestamp)
        elif receiver_id == "teacher":
            success = send_request_with_timeout(teacher_url, "receive_reply", my_process_id, timestamp)
    except Exception as e:
        pass

def receive_reply(sender_id, timestamp):
    global replies_received, logical_clock, requesting_cs
    update_clock(timestamp)
    
    if requesting_cs:
        replies_received += 1
    
    return True

def run_server():
    global time_now
    time_now = input_time()
    
    init_database()
    
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print(f"[{get_formatted_time()}] [SERVER] Starting Enhanced Server with ALL TASKS...")
    print(f"[{get_formatted_time()}] [SERVER] Features: Berkeley, Ricart-Agrawala, Task 6 Deadlock, Task 7 Load Balancing, Task 8 Replication")
    print(f"[{get_formatted_time()}] [SERVER] TASK 7: Internal Load Balancing")
    print(f"[{get_formatted_time()}] [SERVER] Main server: {local_ip}:8000")
    print(f"[{get_formatted_time()}] [SERVER] Backup server: {local_ip}:{BACKUP_PORT}")
    print(f"[{get_formatted_time()}] [SERVER] TASK 8: Database Replication System (RF=3)")
    print(f"[{get_formatted_time()}] [SERVER] TASK 9: Hadoop-style Resource Manager + Distributed Processing")
    print(f"[{get_formatted_time()}] [SERVER] Reduced exam timer: {exam_timer} seconds")
    print(f"[{get_formatted_time()}] [SERVER] Buffer: {main_server_buffer.maxsize} slots, Threshold: {load_threshold}")
    
    server = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True)
    
    server.register_function(register, "register")
    server.register_function(get_time, "get_time")
    server.register_function(adjust_time, "adjust_time")
    server.register_function(update_status, "update_status")
    
    server.register_function(receive_request, "receive_request")
    server.register_function(receive_reply, "receive_reply")
    
    server.register_function(start_interactive_exam, "start_interactive_exam")
    server.register_function(get_question, "get_question")
    server.register_function(submit_answer, "submit_answer")
    server.register_function(submit_exam_final, "submit_exam_final")
    
    server.register_function(get_load_balancer_stats, "get_load_balancer_stats")
    
    server.register_function(initialize_replication_system, "initialize_replication_system")
    server.register_function(show_replication_status, "show_replication_status")
    server.register_function(read_from_replicas, "read_from_replicas")
    server.register_function(write_to_replicas, "write_to_replicas")
    
    server.register_function(initialize_resource_manager, "initialize_resource_manager")
    server.register_function(report_node_metrics, "report_node_metrics")
    server.register_function(get_cluster_resources, "get_cluster_resources")
    server.register_function(submit_processing_job, "submit_processing_job")
    server.register_function(get_job_status, "get_job_status")
    server.register_function(get_job_queue_status, "get_job_queue_status")
    
    print(f"[{get_formatted_time()}] [SERVER] Ready to handle requests with replication, load balancing, and distributed processing...")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{get_formatted_time()}] [SERVER] Server shutting down...")

if __name__ == "__main__":
    run_server()
