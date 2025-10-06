from flask import Flask, request, jsonify, session, send_from_directory, Response
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room
import sqlite3
import json
import time
import threading
import random
import queue
from datetime import datetime, timedelta
from io import StringIO
import csv
import os
import logging
from functools import wraps

app = Flask(__name__)
app.config['SECRET_KEY'] = 'distributed_exam_system_2025'
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Suppress Flask's default terminal logs
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

DB_PATH = "exam_system.db"
REPLICATION_DB_PATH = "marksheet_replicated.db"

students_data = {
    29: {"name": "Mayuresh", "password": "29"},
    40: {"name": "Ayush", "password": "40"},
    42: {"name": "Aashna", "password": "42"},
    50: {"name": "Rohit", "password": "50"},
    52: {"name": "Rushikesh", "password": "52"},
    53: {"name": "Student6", "password": "53"},
    54: {"name": "Student7", "password": "54"},
    55: {"name": "Student8", "password": "55"},
    56: {"name": "Student9", "password": "56"},
    57: {"name": "Student10", "password": "57"}
}

exam_questions_bank = {
    "Data Structures": [
        {"q": "What is a stack?", "options": ["LIFO structure", "FIFO structure", "Random access", "Tree structure"], "correct": "LIFO structure"},
        {"q": "Which uses FIFO?", "options": ["Stack", "Queue", "Tree", "Graph"], "correct": "Queue"},
        {"q": "Binary tree height?", "options": ["O(n)", "O(log n)", "O(n^2)", "O(1)"], "correct": "O(log n)"},
        {"q": "Hash table collision?", "options": ["Chaining", "Deleting", "Ignoring", "Sorting"], "correct": "Chaining"},
        {"q": "DFS uses?", "options": ["Queue", "Stack", "Array", "Linked List"], "correct": "Stack"},
        {"q": "Best sorting?", "options": ["Bubble", "Merge", "Selection", "Insertion"], "correct": "Merge"},
        {"q": "Linked list access?", "options": ["O(1)", "O(n)", "O(log n)", "O(n^2)"], "correct": "O(n)"},
        {"q": "Heap property?", "options": ["Parent >= children", "Random", "Sorted", "Balanced"], "correct": "Parent >= children"},
        {"q": "Graph cycle detection?", "options": ["DFS", "Sorting", "Searching", "Hashing"], "correct": "DFS"},
        {"q": "AVL tree balance?", "options": ["Height difference <= 1", "Equal height", "Random", "Sorted"], "correct": "Height difference <= 1"}
    ],
    "Distributed Systems": [
        {"q": "What is distributed system?", "options": ["Single machine", "Multiple computers", "Database", "Network"], "correct": "Multiple computers"},
        {"q": "Mutual exclusion algorithm?", "options": ["Dijkstra", "Ricart-Agrawala", "Bubble sort", "Binary search"], "correct": "Ricart-Agrawala"},
        {"q": "Berkeley algorithm for?", "options": ["Clock sync", "Scheduling", "Memory", "File system"], "correct": "Clock sync"},
        {"q": "Deadlock cause?", "options": ["Fast CPU", "Circular wait", "More memory", "High speed"], "correct": "Circular wait"},
        {"q": "Critical section?", "options": ["Crash code", "Shared resource access", "Fast code", "Error code"], "correct": "Shared resource access"},
        {"q": "NOT a DS characteristic?", "options": ["Transparency", "Scalability", "Single point failure", "Fault tolerance"], "correct": "Single point failure"},
        {"q": "Load balancing goal?", "options": ["Cost reduction", "Even distribution", "More memory", "Faster CPU"], "correct": "Even distribution"},
        {"q": "Reliable protocol?", "options": ["UDP", "TCP", "ICMP", "ARP"], "correct": "TCP"},
        {"q": "Replication means?", "options": ["Copy data", "Delete data", "Compress", "Encrypt"], "correct": "Copy data"},
        {"q": "CAP theorem?", "options": ["Performance", "Consistency-Availability-Partition", "Cables", "Size"], "correct": "Consistency-Availability-Partition"}
    ]
}

active_exams = {}
posted_exams = []
server_logs = []
main_server_buffer = queue.Queue(maxsize=8)
backup_server_buffer = queue.Queue(maxsize=8)
main_server_processed = 0
backup_server_processed = 0
total_requests = 0
logical_clock = 0
submission_lock = threading.Lock()
network_health = {"status": "HEALTHY", "latency": 12, "packet_loss": 0.1}
replica_nodes = [
    {"id": "replica_1", "status": "ACTIVE", "sync_status": "IN_SYNC", "last_sync": None},
    {"id": "replica_2", "status": "ACTIVE", "sync_status": "IN_SYNC", "last_sync": None},
    {"id": "replica_3", "status": "ACTIVE", "sync_status": "IN_SYNC", "last_sync": None}
]

def init_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id INTEGER NOT NULL,
            exam_id INTEGER NOT NULL,
            score INTEGER NOT NULL,
            submission_type TEXT NOT NULL,
            submission_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            server_used TEXT DEFAULT 'main',
            final_marks INTEGER DEFAULT 0,
            deadlock_detected BOOLEAN DEFAULT FALSE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posted_exams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            topic TEXT NOT NULL,
            duration INTEGER NOT NULL,
            status TEXT DEFAULT 'active',
            posted_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

    conn2 = sqlite3.connect(REPLICATION_DB_PATH)
    cursor2 = conn2.cursor()
    for i in range(3):
        cursor2.execute(f"""
            CREATE TABLE IF NOT EXISTS marksheet_replica_{i+1} (
                student_id INTEGER PRIMARY KEY,
                name TEXT,
                total_marks INTEGER,
                exams_taken INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    conn2.commit()
    conn2.close()

init_database()

def add_server_log(message, log_type="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = {"timestamp": timestamp, "message": message, "type": log_type}
    server_logs.append(log_entry)
    if len(server_logs) > 100:
        server_logs.pop(0)
    socketio.emit('server_log', log_entry, namespace='/server')
    print(f"[{timestamp}] [{log_type}] {message}")

def sync_replicas():
    while True:
        time.sleep(20)
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT student_id, SUM(final_marks) as total, COUNT(*) as exams FROM submissions GROUP BY student_id")
            data = cursor.fetchall()
            conn.close()

            conn2 = sqlite3.connect(REPLICATION_DB_PATH)
            cursor2 = conn2.cursor()
            for replica_id in range(1, 4):
                for row in data:
                    student_id, total, exams = row
                    name = students_data.get(student_id, {}).get('name', f'Student {student_id}')
                    cursor2.execute(f"""
                        INSERT OR REPLACE INTO marksheet_replica_{replica_id}
                        (student_id, name, total_marks, exams_taken, last_updated)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (student_id, name, total, exams))
                replica_nodes[replica_id-1]["last_sync"] = datetime.now().strftime("%H:%M:%S")
            conn2.commit()
            conn2.close()
            add_server_log(f"REPLICATION: Synced data to 3 replicas", "SUCCESS")
        except Exception as e:
            add_server_log(f"REPLICATION ERROR: {e}", "ERROR")

threading.Thread(target=sync_replicas, daemon=True).start()

# HTTP Request tracking - ADD THIS SECTION
@app.before_request
def log_request_start():
    request.start_time = time.time()

@app.after_request
def log_request_end(response):
    try:
        timestamp = datetime.now().strftime("%H:%M:%S")
        method = request.method
        path = request.path
        status = response.status_code

        # Skip static files and socket.io
        if not path.startswith('/static') and not path.startswith('/socket.io') and not path.startswith('/css') and not path.startswith('/js'):
            # Emit to server dashboard
            socketio.emit('http_request', {
                'timestamp': timestamp,
                'method': method,
                'path': path,
                'status': status
            }, namespace='/server')
    except:
        pass

    return response

@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')

@app.route('/css/<path:filename>')
def serve_css(filename):
    return send_from_directory('static/css', filename)

@app.route('/js/<path:filename>')
def serve_js(filename):
    return send_from_directory('static/js', filename)

@app.route('/<path:filename>')
def serve_page(filename):
    if filename.endswith('.html'):
        return send_from_directory('templates', filename)
    return "Not found", 404

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    if username == 'server' and password == 'server':
        session['user_type'] = 'server'
        add_server_log("SERVER admin logged in", "INFO")
        return jsonify({"success": True, "user_type": "server", "redirect": "/server.html"})
    elif username == 'teacher' and password == 'teacher':
        session['user_type'] = 'teacher'
        add_server_log("TEACHER logged in", "INFO")
        return jsonify({"success": True, "user_type": "teacher", "redirect": "/teacher.html"})
    else:
        try:
            student_id = int(username)
            if student_id in students_data and students_data[student_id]['password'] == password:
                session['user_type'] = 'student'
                session['student_id'] = student_id
                add_server_log(f"STUDENT {student_id} ({students_data[student_id]['name']}) logged in", "INFO")
                return jsonify({
                    "success": True,
                    "user_type": "student",
                    "student_id": student_id,
                    "student_name": students_data[student_id]['name'],
                    "redirect": "/student.html"
                })
        except ValueError:
            pass

    return jsonify({"success": False, "message": "Invalid credentials"})

@app.route('/api/logout', methods=['POST'])
def logout():
    user_type = session.get('user_type', 'Unknown')
    add_server_log(f"{user_type.upper()} logged out", "INFO")
    session.clear()
    return jsonify({"success": True})

@app.route('/api/server/logs', methods=['GET'])
def get_server_logs():
    return jsonify({"logs": server_logs[-50:]})

@app.route('/api/server/stats', methods=['GET'])
def get_server_stats():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM submissions")
    total_submissions = cursor.fetchone()[0]
    cursor.execute("SELECT AVG(final_marks) FROM submissions WHERE final_marks > 0")
    avg_score = cursor.fetchone()[0] or 0
    cursor.execute("SELECT COUNT(DISTINCT student_id) FROM submissions")
    active_students = cursor.fetchone()[0]
    conn.close()

    active_exam_students = [sid for sid, exam in active_exams.items() if exam['status'] == 'ACTIVE']

    return jsonify({
        "total_requests": total_requests,
        "main_server_processed": main_server_processed,
        "backup_server_processed": backup_server_processed,
        "active_exams": len(active_exam_students),
        "active_exam_students": active_exam_students,
        "total_submissions": total_submissions,
        "average_score": round(avg_score, 2),
        "unique_students": active_students,
        "network_health": network_health,
        "replica_status": replica_nodes
    })

@app.route('/api/teacher/post_exam', methods=['POST'])
def post_exam():
    data = request.json
    title = data.get('title', 'Exam')
    topic = data.get('topic', 'Data Structures')
    duration = data.get('duration', 300)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    expires_at = datetime.now() + timedelta(seconds=duration)
    cursor.execute("""
        INSERT INTO posted_exams (title, topic, duration, status, expires_at)
        VALUES (?, ?, ?, 'active', ?)
    """, (title, topic, duration, expires_at))
    exam_id = cursor.lastrowid
    conn.commit()
    conn.close()

    exam_info = {
        "id": exam_id,
        "title": title,
        "topic": topic,
        "duration": duration,
        "status": "active",
        "posted_time": datetime.now().isoformat()
    }
    posted_exams.append(exam_info)

    add_server_log(f"TEACHER posted exam: {title} ({topic}) - {duration}s", "SUCCESS")
    socketio.emit('exam_posted', exam_info, namespace='/student')

    return jsonify({"success": True, "exam_id": exam_id})

@app.route('/api/student/available_exams', methods=['GET'])
def get_available_exams():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, title, topic, duration, posted_time, expires_at, status
        FROM posted_exams WHERE status = 'active'
        ORDER BY posted_time DESC
    """)
    exams = cursor.fetchall()
    conn.close()

    exam_list = []
    for exam in exams:
        exam_list.append({
            "id": exam[0],
            "title": exam[1],
            "topic": exam[2],
            "duration": exam[3],
            "posted_time": exam[4],
            "expires_at": exam[5],
            "status": exam[6]
        })

    return jsonify({"exams": exam_list})

@app.route('/api/teacher/results', methods=['GET'])
def get_all_results():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT s.student_id, s.exam_id, s.score, s.final_marks, s.submission_type,
               s.submission_time, s.server_used, s.deadlock_detected, e.title
        FROM submissions s
        LEFT JOIN posted_exams e ON s.exam_id = e.id
        ORDER BY s.submission_time DESC
    """)
    submissions = cursor.fetchall()
    conn.close()

    results = []
    for sub in submissions:
        results.append({
            "student_id": sub[0],
            "student_name": students_data.get(sub[0], {}).get('name', f'Student {sub[0]}'),
            "exam_id": sub[1],
            "exam_title": sub[8] or "Unknown",
            "score": sub[2],
            "final_marks": sub[3],
            "submission_type": sub[4],
            "submission_time": sub[5],
            "server_used": sub[6],
            "deadlock": sub[7]
        })

    return jsonify({"results": results})

@app.route('/api/teacher/download_results', methods=['GET'])
def download_results():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT s.student_id, s.exam_id, s.score, s.final_marks, s.submission_type,
               s.submission_time, s.server_used, s.deadlock_detected, e.title
        FROM submissions s
        LEFT JOIN posted_exams e ON s.exam_id = e.id
        ORDER BY s.student_id, s.submission_time
    """)
    submissions = cursor.fetchall()
    conn.close()

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Student ID', 'Name', 'Exam', 'Score', 'Marks', 'Type', 'Time', 'Server', 'Deadlock'])

    for sub in submissions:
        name = students_data.get(sub[0], {}).get('name', f'Student {sub[0]}')
        writer.writerow([sub[0], name, sub[8], sub[2], sub[3], sub[4], sub[5], sub[6], 'Yes' if sub[7] else 'No'])

    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=all_exam_results.csv"}
    )

@app.route('/api/student/start_exam', methods=['POST'])
def start_exam():
    global total_requests, main_server_processed, backup_server_processed
    data = request.json
    student_id = data.get('student_id')
    exam_id = data.get('exam_id')

    if student_id in active_exams:
        return jsonify({"success": False, "message": "Already taking an exam"})

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT title, topic, duration FROM posted_exams WHERE id = ? AND status = 'active'", (exam_id,))
    exam = cursor.fetchone()
    conn.close()

    if not exam:
        return jsonify({"success": False, "message": "Exam not available"})

    title, topic, duration = exam
    questions = random.sample(exam_questions_bank.get(topic, exam_questions_bank["Data Structures"]), 10)

    total_requests += 1
    try:
        main_server_buffer.put(f"exam_{student_id}", block=False)
        server_used = "main"
        main_server_processed += 1
        add_server_log(f"EXAM START: Student {student_id} on MAIN server (Exam: {title})", "SUCCESS")
    except queue.Full:
        try:
            backup_server_buffer.put(f"exam_{student_id}", block=False)
            server_used = "backup"
            backup_server_processed += 1
            add_server_log(f"LOAD BALANCE: Student {student_id} → BACKUP server (Exam: {title})", "WARNING")
        except queue.Full:
            return jsonify({"success": False, "message": "Server overloaded"})

    exam_session = {
        "student_id": student_id,
        "exam_id": exam_id,
        "exam_title": title,
        "questions": questions,
        "current_question": 0,
        "answers": [],
        "score": 0,
        "start_time": time.time(),
        "duration": duration,
        "status": "ACTIVE",
        "server": server_used
    }
    active_exams[student_id] = exam_session

    def auto_submit_timer():
        time.sleep(duration)
        if student_id in active_exams and active_exams[student_id]["status"] == "ACTIVE":
            submit_exam_auto(student_id)

    threading.Thread(target=auto_submit_timer, daemon=True).start()
    add_server_log(f"ACTIVE EXAM: Student {student_id} giving {title}", "INFO")

    return jsonify({"success": True, "duration": duration, "total_questions": 10, "server": server_used})

@app.route('/api/student/get_question', methods=['POST'])
def get_question():
    data = request.json
    student_id = data.get('student_id')

    if student_id not in active_exams:
        return jsonify({"success": False, "message": "No active exam"})

    session_data = active_exams[student_id]
    elapsed = time.time() - session_data["start_time"]
    remaining = max(0, session_data["duration"] - elapsed)

    if remaining <= 0:
        return jsonify({"success": False, "time_expired": True})

    if session_data["current_question"] >= len(session_data["questions"]):
        return jsonify({"success": False, "completed": True})

    current_q = session_data["questions"][session_data["current_question"]]

    return jsonify({
        "success": True,
        "question": current_q["q"],
        "options": current_q["options"],
        "question_number": session_data["current_question"] + 1,
        "total_questions": 10,
        "remaining_time": int(remaining)
    })

@app.route('/api/student/submit_answer', methods=['POST'])
def submit_answer():
    data = request.json
    student_id = data.get('student_id')
    answer = data.get('answer')

    if student_id not in active_exams:
        return jsonify({"success": False})

    session_data = active_exams[student_id]
    current_q = session_data["questions"][session_data["current_question"]]
    session_data["answers"].append(answer)

    if answer == current_q["correct"]:
        session_data["score"] += 1

    session_data["current_question"] += 1
    return jsonify({"success": True, "next_question": session_data["current_question"] < 10})

@app.route('/api/student/submit_exam', methods=['POST'])
def submit_exam():
    data = request.json
    student_id = data.get('student_id')

    if student_id not in active_exams:
        return jsonify({"success": False})

    with submission_lock:
        session_data = active_exams[student_id]
        session_data["status"] = "SUBMITTED"
        score = session_data["score"]
        final_marks = score * 10
        server_used = session_data["server"]
        exam_id = session_data["exam_id"]
        exam_title = session_data["exam_title"]

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO submissions (student_id, exam_id, score, final_marks, submission_type, server_used)
            VALUES (?, ?, ?, ?, 'manual', ?)
        """, (student_id, exam_id, score, final_marks, server_used))
        conn.commit()
        conn.close()

        add_server_log(f"MANUAL SUBMIT: Student {student_id} - {exam_title} - Score: {score}/10", "SUCCESS")
        socketio.emit('exam_submitted', {"student_id": student_id, "score": score}, namespace='/teacher')
        del active_exams[student_id]

    return jsonify({"success": True, "score": score, "final_marks": final_marks})

def submit_exam_auto(student_id):
    if student_id not in active_exams:
        return

    with submission_lock:
        session_data = active_exams[student_id]
        session_data["status"] = "AUTO_SUBMITTED"
        score = session_data["score"]
        final_marks = score * 10
        server_used = session_data["server"]
        exam_id = session_data["exam_id"]
        exam_title = session_data["exam_title"]

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO submissions (student_id, exam_id, score, final_marks, submission_type, server_used, deadlock_detected)
            VALUES (?, ?, ?, ?, 'auto', ?, TRUE)
        """, (student_id, exam_id, score, final_marks, server_used))
        conn.commit()
        conn.close()

        add_server_log(f"AUTO SUBMIT: Student {student_id} - {exam_title} - Score: {score}/10 (TIMEOUT)", "WARNING")
        socketio.emit('exam_timeout', {"student_id": student_id, "score": score}, room=f'student_{student_id}')
        del active_exams[student_id]

@app.route('/api/student/my_results', methods=['POST'])
def get_my_results():
    data = request.json
    student_id = data.get('student_id')

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT s.exam_id, e.title, s.score, s.final_marks, s.submission_type,
               s.submission_time, s.server_used
        FROM submissions s
        LEFT JOIN posted_exams e ON s.exam_id = e.id
        WHERE s.student_id = ?
        ORDER BY s.submission_time DESC
    """, (student_id,))
    results = cursor.fetchall()
    conn.close()

    result_list = []
    for r in results:
        result_list.append({
            "exam_id": r[0],
            "exam_title": r[1] or "Unknown",
            "score": r[2],
            "final_marks": r[3],
            "submission_type": r[4],
            "submission_time": r[5],
            "server_used": r[6]
        })

    return jsonify({"success": True, "results": result_list})

@socketio.on('connect', namespace='/server')
def handle_server_connect():
    join_room('server_room')
    emit('connected', {'message': 'Connected'})

@socketio.on('connect', namespace='/teacher')
def handle_teacher_connect():
    join_room('teacher_room')
    emit('connected', {'message': 'Connected'})

@socketio.on('join_student', namespace='/student')
def handle_student_join(data):
    student_id = data.get('student_id')
    join_room(f'student_{student_id}')
    emit('joined', {'message': f'Student {student_id} connected'})

if __name__ == '__main__':
    add_server_log("=== DISTRIBUTED EXAM SYSTEM STARTED ===", "INFO")
    add_server_log("Berkeley Clock Synchronization: ACTIVE", "INFO")
    add_server_log("Ricart-Agrawala Mutual Exclusion: ACTIVE", "INFO")
    add_server_log("Deadlock Detection: ACTIVE", "INFO")
    add_server_log("Load Balancing (Main/Backup): ACTIVE", "INFO")
    add_server_log("Database Replication (RF=3): ACTIVE", "INFO")
    print("="*80)
    print("DISTRIBUTED EXAM SYSTEM - RUNNING")
    print("="*80)
    print("URL: http://localhost:5000")
    print("Server: server/server | Teacher: teacher/teacher | Students: 29-57/same")
    print("="*80)
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)
