import os, uuid, shutil, subprocess, multiprocessing as mp, time, sqlite3, threading
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, UploadFile, File, Header, HTTPException
from fastapi.responses import FileResponse
from dotenv import load_dotenv

load_dotenv()
API_KEY=os.getenv("IR_SERVICE_API_KEY","")
ROOT=Path(os.getenv("DATA_DIR","/var/lib/ir-styleshkon"))
UPLOADS=ROOT/"uploads"; OUTPUTS=ROOT/"outputs"; DB=ROOT/"tasks.sqlite3"
UPLOADS.mkdir(parents=True,exist_ok=True); OUTPUTS.mkdir(parents=True,exist_ok=True)
CPU_COUNT=len(os.sched_getaffinity(0)) if hasattr(os,"sched_getaffinity") else (os.cpu_count() or 1)
WORKER_COUNT=max(1,CPU_COUNT)
QUEUE_LIMIT=max(1,int(os.getenv("QUEUE_LIMIT","100")))
MAX_UPLOAD_BYTES=max(1,int(os.getenv("MAX_UPLOAD_BYTES","200000000")))
RETENTION_HOURS=max(1,int(os.getenv("RETENTION_HOURS","24")))
FFMPEG_TIMEOUT=max(30,int(os.getenv("FFMPEG_TIMEOUT_SECONDS","600")))
ctx=mp.get_context("spawn")
task_queue=ctx.Queue(maxsize=QUEUE_LIMIT)
workers=[]; manager=None; shared=None
stop_event=threading.Event()

def db():
    c=sqlite3.connect(DB,timeout=30)
    c.row_factory=sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=FULL")
    return c

def init_db():
    with db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS tasks (
          id TEXT PRIMARY KEY, status TEXT NOT NULL, created_at REAL NOT NULL,
          started_at REAL, completed_at REAL, source TEXT NOT NULL, output_dir TEXT NOT NULL,
          interval INTEGER NOT NULL, max_frames INTEGER NOT NULL, frames TEXT NOT NULL DEFAULT '[]',
          error TEXT, worker INTEGER)""")
        # Processing jobs interrupted by a restart are safely requeued.
        c.execute("UPDATE tasks SET status='queued',started_at=NULL,error=NULL WHERE status='processing'")

def worker_loop(index,q,shared_state):
    try:
        if hasattr(os,"sched_setaffinity"):
            allowed=sorted(os.sched_getaffinity(0))
            if allowed: os.sched_setaffinity(0,{allowed[index % len(allowed)]})
    except Exception: pass
    while True:
        item=q.get()
        if item is None: return
        task_id=item
        try:
            with db() as c:
                row=c.execute("SELECT * FROM tasks WHERE id=?",(task_id,)).fetchone()
            if not row or row["status"]!="queued": continue
            source=row["source"]; out=Path(row["output_dir"])
            interval=row["interval"]; max_frames=row["max_frames"]
            out.mkdir(parents=True,exist_ok=True)
            started=time.time()
            with db() as c:
                c.execute("UPDATE tasks SET status='processing',started_at=?,worker=? WHERE id=?",(started,index,task_id))
            vf=f"fps=1/{interval},scale='min(1080,iw)':-2"
            cmd=["ffmpeg","-hide_banner","-loglevel","error","-threads","1","-i",source,"-vf",vf,
                 "-q:v","2","-frames:v",str(max_frames),"-y",str(out/"frame_%04d.jpg")]
            p=subprocess.run(cmd,capture_output=True,text=True,timeout=FFMPEG_TIMEOUT)
            if p.returncode: raise RuntimeError((p.stderr or "ffmpeg failed")[-1500:])
            frames=sorted(out.glob("frame_*.jpg"))
            if not frames:
                fallback=subprocess.run(["ffmpeg","-hide_banner","-loglevel","error","-threads","1","-i",source,
                    "-frames:v","1","-vf","scale='min(1080,iw)':-2","-q:v","2","-y",str(out/"frame_0001.jpg")],
                    capture_output=True,text=True,timeout=120)
                if fallback.returncode: raise RuntimeError((fallback.stderr or "fallback failed")[-1500:])
                frames=sorted(out.glob("frame_*.jpg"))
            with db() as c:
                c.execute("UPDATE tasks SET status='completed',frames=?,completed_at=?,error=NULL WHERE id=?",
                          (json_frames(frames),time.time(),task_id))
        except Exception as e:
            with db() as c:
                c.execute("UPDATE tasks SET status='failed',error=?,completed_at=? WHERE id=?",(str(e)[:2000],time.time(),task_id))

def json_frames(frames):
    import json
    return json.dumps([p.name for p in frames])

def enqueue_pending():
    with db() as c: rows=c.execute("SELECT id FROM tasks WHERE status='queued' ORDER BY created_at").fetchall()
    for r in rows:
        try: task_queue.put_nowait(r["id"])
        except Exception: break

def maintenance():
    while not stop_event.wait(60):
        cutoff=time.time()-RETENTION_HOURS*3600
        try:
            with db() as c:
                old=c.execute("SELECT id,source,output_dir FROM tasks WHERE status IN ('completed','failed') AND completed_at<?",(cutoff,)).fetchall()
                for r in old:
                    shutil.rmtree(r["output_dir"],ignore_errors=True)
                    Path(r["source"]).unlink(missing_ok=True)
                    c.execute("DELETE FROM tasks WHERE id=?",(r["id"],))
            # Also clean orphan files left by interrupted upload/deletion.
            active=set()
            with db() as c: active={r["id"] for r in c.execute("SELECT id FROM tasks").fetchall()}
            for p in OUTPUTS.iterdir():
                if p.is_dir() and p.name not in active: shutil.rmtree(p,ignore_errors=True)
            for p in UPLOADS.iterdir():
                if p.is_file() and p.stem not in active and p.stat().st_mtime<cutoff: p.unlink(missing_ok=True)
        except Exception: pass
        enqueue_pending()

app=FastAPI(title="IR Styleshkon Video Frame Service",version="2.0.0")

@app.on_event("startup")
def startup():
    global workers,manager,shared
    init_db(); manager=ctx.Manager(); shared=manager.dict()
    for i in range(WORKER_COUNT):
        p=ctx.Process(target=worker_loop,args=(i,task_queue,shared),daemon=True); p.start(); workers.append(p)
    enqueue_pending()
    threading.Thread(target=maintenance,daemon=True).start()

@app.on_event("shutdown")
def shutdown():
    stop_event.set()
    for _ in workers:
        try: task_queue.put_nowait(None)
        except Exception: pass
    for p in workers: p.join(timeout=3)
    if manager is not None: manager.shutdown()

def auth(key):
    if not API_KEY or key!=API_KEY: raise HTTPException(401,"Unauthorized")

@app.get("/health")
def health():
    with db() as c:
        queued=c.execute("SELECT count(*) FROM tasks WHERE status='queued'").fetchone()[0]
        running=c.execute("SELECT count(*) FROM tasks WHERE status='processing'").fetchone()[0]
    return {"ok":True,"cpu_count":CPU_COUNT,"workers":WORKER_COUNT,"queue_limit":QUEUE_LIMIT,"queued":queued,"processing":running}

@app.post("/v1/videos")
async def submit_video(file:UploadFile=File(...),x_api_key:Optional[str]=Header(None)):
    auth(x_api_key)
    if not file.filename: raise HTTPException(400,"Missing filename")
    ext=Path(file.filename).suffix.lower()
    if ext not in {".mp4",".mov",".m4v",".webm"}: raise HTTPException(415,"Unsupported video format")
    with db() as c:
        pending=c.execute("SELECT count(*) FROM tasks WHERE status='queued'").fetchone()[0]
    if pending>=QUEUE_LIMIT: raise HTTPException(503,"Queue full")
    task_id=str(uuid.uuid4()); src=UPLOADS/f"{task_id}{ext}"; out=OUTPUTS/task_id
    size=0
    try:
        with src.open("wb") as f:
            while True:
                chunk=await file.read(1024*1024)
                if not chunk: break
                size+=len(chunk)
                if size>MAX_UPLOAD_BYTES: raise HTTPException(413,"Video too large")
                f.write(chunk)
    except Exception:
        src.unlink(missing_ok=True); raise
    interval=max(1,int(os.getenv("FRAME_INTERVAL_SECONDS","1")))
    max_frames=max(1,min(100,int(os.getenv("MAX_FRAMES","12"))))
    with db() as c:
        c.execute("INSERT INTO tasks(id,status,created_at,source,output_dir,interval,max_frames) VALUES(?,?,?,?,?,?,?)",
                  (task_id,"queued",time.time(),str(src),str(out),interval,max_frames))
    try: task_queue.put_nowait(task_id)
    except Exception:
        # DB record remains queued; maintenance will retry enqueueing.
        pass
    return {"task_id":task_id,"status":"queued","status_url":f"/v1/tasks/{task_id}"}

@app.get("/v1/tasks/{task_id}")
def task_status(task_id:str,x_api_key:Optional[str]=Header(None)):
    auth(x_api_key)
    with db() as c: row=c.execute("SELECT * FROM tasks WHERE id=?",(task_id,)).fetchone()
    if not row: raise HTTPException(404,"Task not found")
    result=dict(row)
    import json
    result["frames"]=json.loads(result["frames"])
    if result["status"]=="completed":
        result["frame_urls"]=[f"/v1/tasks/{task_id}/frames/{n}" for n in result["frames"]]
    return result

@app.get("/v1/tasks/{task_id}/frames/{filename}")
def frame(task_id:str,filename:str,x_api_key:Optional[str]=Header(None)):
    auth(x_api_key)
    if "/" in filename or ".." in filename: raise HTTPException(400,"Invalid filename")
    with db() as c: row=c.execute("SELECT status,frames,output_dir FROM tasks WHERE id=?",(task_id,)).fetchone()
    if not row: raise HTTPException(404,"Task not found")
    import json
    if row["status"]!="completed" or filename not in json.loads(row["frames"]): raise HTTPException(404,"Frame not found")
    path=Path(row["output_dir"])/filename
    if not path.is_file(): raise HTTPException(404,"Frame missing")
    return FileResponse(path,media_type="image/jpeg",filename=filename)

@app.delete("/v1/tasks/{task_id}")
def delete_task(task_id:str,x_api_key:Optional[str]=Header(None)):
    auth(x_api_key)
    with db() as c: row=c.execute("SELECT status,source,output_dir FROM tasks WHERE id=?",(task_id,)).fetchone()
    if not row: raise HTTPException(404,"Task not found")
    if row["status"] in {"queued","processing"}: raise HTTPException(409,"Task still running")
    shutil.rmtree(row["output_dir"],ignore_errors=True); Path(row["source"]).unlink(missing_ok=True)
    with db() as c: c.execute("DELETE FROM tasks WHERE id=?",(task_id,))
    return {"deleted":True}
