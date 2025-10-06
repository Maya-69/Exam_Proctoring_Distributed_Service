# 🎓 Exam Proctoring Distributed Service

A distributed online exam proctoring system that runs across multiple services (3–9) with a central UI project (10).  
This setup allows distributed monitoring, data processing, and UI control — designed to maintain integrity in remote examinations.

---

## 👥 Contributors
- **Mayuresh Sawant**
- **Ayush Manore**
- **Aashna Gaikwad**
- **Rohit Thatikonda**
- **Rushikesh Gawade**

---

## ⚙️ Overview

### 📁 Project Structure
```plaintext
Exam_Proctoring_Distributed_Service/
├── 3/
├── 4/
├── 5/
├── 6/
├── 7/
├── 8/
├── 9/
│   └── Each folder runs as a distributed service (Python-based)
├── 10/
│   ├── Main UI project
│   └── config.json        # local only for 10
├── config.json            # global for all (1–9), set once and all run on it
└── README.md
```

## How to Run

### 1. Clone the Repository
```bash
git clone https://github.com/<your-username>/Exam_Proctoring_Distributed_Service.git
cd Exam_Proctoring_Distributed_Service
```

### 2. Create a Virtual Environment
```bash
python -m venv venv
venv\Scripts\activate     # On Windows
# or
source venv/bin/activate  # On macOS/Linux
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configuration
Open config.json (in the root folder).

Set "host" to "localhost" or your system’s IP.
Example:
```json
{
    "host": "127.0.0.1",
    "port": 5000
}
```

### 5. Run Services  
## Run 3 - 9  
Each of the service folders (3 to 9) runs independently.  
Make sure to run this in separate terminals or systems if using IPs.  

```bash
python server.py 
python teacher.py 
python client.py
```
## Run 10
Folder 10 has its own `config.json` and runs a single app.

```bash
cd 10
python app.py
```


