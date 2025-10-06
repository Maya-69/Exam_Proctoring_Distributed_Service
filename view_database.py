import sqlite3
import json
from datetime import datetime

DB_PATH = "E:/Dev/Python/Cheating/8/exam_system.db"
REPLICATION_DB_PATH = "E:/Dev/Python/Cheating/8/marksheet_replicated.db"
TEACHER_REPLICATION_DB = "E:/Dev/Python/Cheating/8/teacher_marksheet_replicated.db"

def view_database():
    print("="*100)
    print("COMPREHENSIVE DATABASE VIEWER - ALL TASKS")
    print("="*100)
    
    view_task_1_7_databases()
    view_task_8_replication_databases()

def view_task_1_7_databases():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        print("\n" + "="*80)
        print("TASKS 1-7: TRADITIONAL DATABASE (exam_system.db)")
        print("="*80)
        
        print("\n1. STATUS_DB TABLE:")
        print("-"*50)
        cursor.execute("SELECT * FROM status_db ORDER BY timestamp DESC")
        status_rows = cursor.fetchall()
        
        if status_rows:
            print(f"{'ID':<5} {'Student':<8} {'Status':<15} {'Timestamp':<20} {'Type':<12}")
            print("-" * 70)
            for row in status_rows:
                print(f"{row[0]:<5} {row[1]:<8} {row[2]:<15} {row[3]:<20} {row[4]:<12}")
        else:
            print("No status entries found")
        
        print("\n2. SUBMISSION_DB TABLE WITH RESULTS:")
        print("-"*90)
        cursor.execute("SELECT * FROM submission_db ORDER BY submission_time DESC")
        submission_rows = cursor.fetchall()
        
        if submission_rows:
            print(f"{'ID':<4} {'Student':<8} {'Score':<6} {'Marks':<7} {'Result':<7} {'Type':<8} {'Server':<8} {'Time':<20}")
            print("-" * 80)
            for row in submission_rows:
                server_used = row[9] if len(row) > 9 else 'main'
                final_marks = row[10] if len(row) > 10 else row[2] * 10
                result = row[11] if len(row) > 11 else ('PASS' if final_marks >= 50 else 'FAIL')
                
                print(f"{row[0]:<4} {row[1]:<8} {row[2]:<6} {final_marks:<7} {result:<7} {row[3]:<8} {server_used:<8} {row[6]:<20}")
        else:
            print("No submission entries found")
        
        print("\n3. DEADLOCK_EVENTS TABLE:")
        print("-"*70)
        cursor.execute("SELECT * FROM deadlock_events ORDER BY timestamp DESC")
        deadlock_rows = cursor.fetchall()
        
        if deadlock_rows:
            print(f"{'ID':<4} {'Student':<8} {'Event':<20} {'Strategy':<15} {'Winner':<8} {'Time':<20}")
            print("-" * 80)
            for row in deadlock_rows:
                print(f"{row[0]:<4} {row[1]:<8} {row[2]:<20} {row[5] or 'none':<15} {row[6] or 'none':<8} {row[7]:<20}")
        else:
            print("No deadlock events found")
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")

def view_task_8_replication_databases():
    print("\n" + "="*100)
    print("TASK 8: REPLICATION DATABASES - PHYSICAL STRUCTURE ANALYSIS")
    print("="*100)
    
    view_server_replication_database()
    view_teacher_replication_database()

def view_server_replication_database():
    try:
        conn = sqlite3.connect(REPLICATION_DB_PATH)
        cursor = conn.cursor()
        
        print("\n" + "="*80)
        print("SERVER REPLICATION DATABASE (marksheet_replicated.db)")
        print("="*80)
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        
        print(f"\nPHYSICAL TABLES STRUCTURE:")
        print("-"*60)
        chunk_tables = []
        metadata_tables = []
        
        for table in tables:
            table_name = table[0]
            if 'chunk_' in table_name and 'replica_' in table_name:
                chunk_tables.append(table_name)
            else:
                metadata_tables.append(table_name)
        
        print("CHUNK REPLICA TABLES (Physical Separation):")
        for table in sorted(chunk_tables):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  └─ {table}: {count} records")
        
        print("\nMETADATA TABLES:")
        for table in sorted(metadata_tables):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  └─ {table}: {count} records")
        
        print(f"\nCHUNK DISTRIBUTION ANALYSIS:")
        print("-"*60)
        cursor.execute("SELECT chunk_id, student_range, replica_count, status FROM chunk_metadata")
        chunks = cursor.fetchall()
        
        for chunk_id, student_range, replica_count, status in chunks:
            print(f"\nChunk {chunk_id} (Students: {student_range}):")
            print(f"  Status: {status} | Replicas: {replica_count}")
            
            for replica_id in range(1, replica_count + 1):
                table_name = f"chunk_{chunk_id}_replica_{replica_id}"
                cursor.execute(f"SELECT roll_number, name, total FROM {table_name}")
                students = cursor.fetchall()
                
                print(f"  Replica {replica_id}:")
                for roll, name, total in students:
                    print(f"    Student {roll} ({name}): Total={total}")
        
        print(f"\nREPLICA STATUS MATRIX:")
        print("-"*60)
        cursor.execute("SELECT chunk_id, replica_id, node_location, status FROM replica_status ORDER BY chunk_id, replica_id")
        replicas = cursor.fetchall()
        
        current_chunk = -1
        for chunk_id, replica_id, node_location, status in replicas:
            if chunk_id != current_chunk:
                print(f"\nChunk {chunk_id}:")
                current_chunk = chunk_id
            print(f"  Replica {replica_id}: {node_location} ({status})")
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Server replication database error: {e}")
    except Exception as e:
        print(f"Server replication error: {e}")

def view_teacher_replication_database():
    try:
        conn = sqlite3.connect(TEACHER_REPLICATION_DB)
        cursor = conn.cursor()
        
        print("\n" + "="*80)
        print("TEACHER REPLICATION DATABASE (teacher_marksheet_replicated.db)")
        print("="*80)
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        
        print(f"\nTEACHER MARKSHEET PHYSICAL TABLES:")
        print("-"*60)
        marksheet_tables = []
        teacher_metadata_tables = []
        
        for table in tables:
            table_name = table[0]
            if 'marksheet_chunk_' in table_name and 'replica_' in table_name:
                marksheet_tables.append(table_name)
            else:
                teacher_metadata_tables.append(table_name)
        
        print("MARKSHEET REPLICA TABLES (Teacher Write-Only):")
        for table in sorted(marksheet_tables):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  └─ {table}: {count} records")
        
        print("\nTEACHER METADATA TABLES:")
        for table in sorted(teacher_metadata_tables):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  └─ {table}: {count} records")
        
        print(f"\nMARKSHEET DISTRIBUTION ANALYSIS:")
        print("-"*70)
        cursor.execute("SELECT chunk_id, student_range, replica_count, status, write_locked FROM teacher_chunk_metadata")
        chunks = cursor.fetchall()
        
        for chunk_id, student_range, replica_count, status, write_locked in chunks:
            lock_status = "LOCKED" if write_locked else "FREE"
            print(f"\nMarksheet Chunk {chunk_id} (Students: {student_range}):")
            print(f"  Status: {status} | Replicas: {replica_count} | Write Lock: {lock_status}")
            
            for replica_id in range(1, replica_count + 1):
                table_name = f"marksheet_chunk_{chunk_id}_replica_{replica_id}"
                cursor.execute(f"SELECT roll_number, name, marks, status, version FROM {table_name}")
                students = cursor.fetchall()
                
                print(f"  Replica {replica_id}:")
                for roll, name, marks, student_status, version in students:
                    print(f"    Student {roll} ({name}): Marks={marks} Status={student_status} Ver={version}")
        
        print(f"\nREPLICATION LOG (Recent Changes):")
        print("-"*70)
        cursor.execute("SELECT operation, chunk_id, student_id, old_value, new_value, timestamp FROM replication_log ORDER BY timestamp DESC LIMIT 10")
        logs = cursor.fetchall()
        
        if logs:
            for operation, chunk_id, student_id, old_value, new_value, timestamp in logs:
                print(f"  {timestamp}: {operation} - Chunk {chunk_id}, Student {student_id}: {old_value}→{new_value}")
        else:
            print("  No replication changes logged yet")
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Teacher replication database error: {e}")
    except Exception as e:
        print(f"Teacher replication error: {e}")

def show_database_files():
    print("\n" + "="*80)
    print("DATABASE FILE SUMMARY")
    print("="*80)
    
    import os
    
    databases = [
        ("exam_system.db", "Tasks 1-7: Main system database"),
        ("marksheet_replicated.db", "Task 8: Server replication database"),
        ("teacher_marksheet_replicated.db", "Task 8: Teacher replication database")
    ]
    
    for db_file, description in databases:
        if os.path.exists(db_file):
            size = os.path.getsize(db_file)
            print(f"✓ {db_file} ({size} bytes) - {description}")
            
            try:
                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                print(f"  Tables: {', '.join([t[0] for t in tables])}")
                conn.close()
            except Exception as e:
                print(f"  Error reading: {e}")
        else:
            print(f"✗ {db_file} - Not found")
        print()

def clear_all_databases():
    databases = [DB_PATH, REPLICATION_DB_PATH, TEACHER_REPLICATION_DB]
    
    for db_path in databases:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for table in tables:
                cursor.execute(f"DELETE FROM {table[0]}")
            
            conn.commit()
            conn.close()
            
            print(f"✓ Cleared database: {db_path}")
            
        except Exception as e:
            print(f"✗ Error clearing {db_path}: {e}")

if __name__ == "__main__":
    print("ENHANCED DATABASE VIEWER - TASK 8 REPLICATION ANALYSIS")
    print("="*60)
    print("1. View All Databases (Tasks 1-8)")
    print("2. Show Database Files Structure")
    print("3. Clear All Databases") 
    print("="*60)
    
    choice = input("Enter choice (1, 2, or 3): ").strip()
    
    if choice == "1":
        view_database()
    elif choice == "2":
        show_database_files()
    elif choice == "3":
        confirm = input("Are you sure you want to clear ALL databases? (y/n): ").strip().lower()
        if confirm == 'y':
            clear_all_databases()
        else:
            print("Operation cancelled")
    else:
        print("Invalid choice")
