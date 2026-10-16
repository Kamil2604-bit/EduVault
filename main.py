from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Form, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import List, Optional, Dict
import asyncio
import zipfile
import io
import random
import string
import os
import shutil

# ==========================================
# 1. SETUP & CONFIGURATION
# ==========================================
ALLOWED_EXTENSIONS = {".pdf", ".docx", ".csv", ".txt", ".jpg", ".jpeg"}
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./rooms.db")

if SQLALCHEMY_DATABASE_URL.startswith("postgres://"):
    SQLALCHEMY_DATABASE_URL = SQLALCHEMY_DATABASE_URL.replace("postgres://", "postgresql://", 1)

if SQLALCHEMY_DATABASE_URL.startswith("sqlite"):
    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ==========================================
# 2. DATABASE MODELS
# ==========================================
class Room(Base):
    __tablename__ = "rooms"
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(6), unique=True, index=True)
    host_message = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class FileRecord(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True, index=True)
    room_code = Column(String(6), ForeignKey("rooms.code"))
    filename = Column(String)
    filepath = Column(String)

Base.metadata.create_all(bind=engine)

# ==========================================
# 3. WEBSOCKET CONNECTION MANAGER
# ==========================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, room_code: str):
        await websocket.accept()
        if room_code not in self.active_connections:
            self.active_connections[room_code] = []
        self.active_connections[room_code].append(websocket)
        await self.broadcast_headcount(room_code)

    def disconnect(self, websocket: WebSocket, room_code: str):
        if room_code in self.active_connections and websocket in self.active_connections[room_code]:
            self.active_connections[room_code].remove(websocket)
            if not self.active_connections[room_code]:
                del self.active_connections[room_code]
        asyncio.create_task(self.broadcast_headcount(room_code))

    async def broadcast_headcount(self, room_code: str):
        if room_code in self.active_connections:
            count = len(self.active_connections[room_code])
            student_count = max(0, count - 1) # Exclude instructor
            await self.broadcast(room_code, {"type": "headcount", "count": student_count})

    async def broadcast(self, room_code: str, message: dict):
        if room_code in self.active_connections:
            for connection in self.active_connections[room_code]:
                try:
                    await connection.send_json(message)
                except:
                    pass

manager = ConnectionManager()

# ==========================================
# 4. BACKGROUND CLEANUP TASK
# ==========================================
async def cleanup_expired_rooms():
    while True:
        await asyncio.sleep(900)  # Check every 15 mins
        db = SessionLocal()
        try:
            expiration_time = datetime.now(timezone.utc) - timedelta(minutes=60)
            expired_rooms = db.query(Room).filter(Room.created_at < expiration_time).all()
            for room in expired_rooms:
                room_dir = os.path.join(UPLOAD_DIR, room.code)
                if os.path.exists(room_dir):
                    shutil.rmtree(room_dir)
                db.query(FileRecord).filter(FileRecord.room_code == room.code).delete()
                db.query(Room).filter(Room.code == room.code).delete()
            db.commit()
        except Exception as e:
            print(f"Error during cleanup: {e}")
        finally:
            db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    cleanup_task = asyncio.create_task(cleanup_expired_rooms())
    yield
    cleanup_task.cancel()

# ==========================================
# 5. FASTAPI APPLICATION
# ==========================================
app = FastAPI(title="EduVault API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def generate_unique_code(db: Session) -> str:
    while True:
        code = ''.join(random.choices(string.digits, k=6))
        if not db.query(Room).filter(Room.code == code).first():
            return code

# ==========================================
# 6. ENDPOINTS
# ==========================================
@app.get("/keep-alive")
def keep_alive():
    return {"status": "awake", "time": datetime.now().isoformat()}

@app.websocket("/ws/{room_code}")
async def websocket_endpoint(websocket: WebSocket, room_code: str):
    await manager.connect(websocket, room_code)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, room_code)

@app.post("/create-room")
def create_room(db: Session = Depends(get_db)):
    new_code = generate_unique_code(db)
    db_room = Room(code=new_code)
    db.add(db_room)
    db.commit()
    db.refresh(db_room)
    return {"status": "success", "room_code": db_room.code}

@app.post("/room/{room_code}/upload")
def upload_to_room(
    room_code: str,
    files: List[UploadFile] = File(default=[]),
    message: Optional[str] = Form(None),
    db: Session = Depends(get_db)
):
    room = db.query(Room).filter(Room.code == room_code).first()
    if not room:
        raise HTTPException(status_code=404, detail="Room not found")
    
    if message:
        room.host_message = message
        
    room_dir = os.path.join(UPLOAD_DIR, room_code)
    os.makedirs(room_dir, exist_ok=True)

    saved_files = []
    for file in files:
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            continue
        
        file_path = os.path.join(room_dir, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        db_file = FileRecord(room_code=room_code, filename=file.filename, filepath=file_path)
        db.add(db_file)
        saved_files.append(file.filename)
    
    db.commit()
    
    # Trigger WebSocket update to students
    asyncio.create_task(manager.broadcast(room_code, {"type": "new_files"}))
    return {"status": "success", "message_saved": bool(message), "files_saved": saved_files}

@app.get("/room/{room_code}")
def get_room(room_code: str, db: Session = Depends(get_db)):
    room = db.query(Room).filter(Room.code == room_code).first()
    if not room:
        raise HTTPException(status_code=404, detail="Room not found")
    
    files = db.query(FileRecord).filter(FileRecord.room_code == room_code).all()
    return {
        "room_code": room.code,
        "host_message": room.host_message,
        "files": [f.filename for f in files]
    }

@app.get("/room/{room_code}/download/{filename}")
def download_file(room_code: str, filename: str, db: Session = Depends(get_db)):
    file_record = db.query(FileRecord).filter(FileRecord.room_code == room_code, FileRecord.filename == filename).first()
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=file_record.filepath, filename=file_record.filename)

@app.get("/room/{room_code}/download-all")
def download_all_files(room_code: str, db: Session = Depends(get_db)):
    files = db.query(FileRecord).filter(FileRecord.room_code == room_code).all()
    if not files:
        raise HTTPException(status_code=404, detail="No files to download")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for file_record in files:
            if os.path.exists(file_record.filepath):
                zip_file.write(file_record.filepath, arcname=file_record.filename)
    
    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer, 
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=EduVault_{room_code}.zip"}
    )

@app.delete("/room/{room_code}")
def delete_room_now(room_code: str, db: Session = Depends(get_db)):
    room = db.query(Room).filter(Room.code == room_code).first()
    if not room:
        raise HTTPException(status_code=404, detail="Room not found")
    
    room_dir = os.path.join(UPLOAD_DIR, room_code)
    if os.path.exists(room_dir):
        shutil.rmtree(room_dir)
        
    db.query(FileRecord).filter(FileRecord.room_code == room_code).delete()
    db.query(Room).filter(Room.code == room_code).delete()
    db.commit()
    
    # Broadcast to all students that the room was destroyed
    asyncio.create_task(manager.broadcast(room_code, {"type": "room_nuked"}))
    return {"status": "success", "message": "Room destroyed"}
