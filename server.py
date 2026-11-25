import os, uuid, shutil, subprocess, multiprocessing as mp, time, json
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, UploadFile, File, Header, HTTPException
from fastapi.responses import FileResponse
from dotenv import load_dotenv

load_dotenv()
API_KEY=os.getenv("IR_SERVICE_API_KEY","")
ROOT=Path(os.getenv("DATA_DIR","/var/lib/ir-styleshkon"))
UPLOADS=ROOT/"uploads"; OUTPUTS=ROOT/"outputs"
UPLOADS.mkdir(parents=True,exist_ok=True); OUTPUTS.mkdir(parents=True,exist_ok=True)
CPU_COUNT=os.cpu_count() or 1
WORKER_COUNT=max(1,CPU_COUNT)
ctx=mp.get_context("spawn")
task_queue=ctx.Queue(maxsize=int(os.getenv("QUEUE_LIMIT","100")))
manager=ctx.Manager()
tasks=manager.dict()
workers=[]

def worker_loop(worker_index, q, shared):
    # Pin each worker process to a distinct CPU core where supported.
    try:
        if hasattr(os,"sched_setaffinity"):
            os.sched_setaffinity(0,{worker_index % (os.cpu_count() or 1)})
    except Exception:
        pass
    while True:
        item=q.get()
        if item is None: break
        task_id, source, output_dir, interval, max_frames=item
        try:
            shared[task_id]={"status":"processing","worker":worker_index,"started_at":time.time()}
            Path(output_dir).mkdir(parents=True,exist_ok=True)
            # Extract bounded, evenly spaced frames. Output names are predictable.
            cmd=["ffmpeg","-hide_banner","-loglevel","error","-i",source,
                 "-vf",f"fps=1/{interval},scale='min(1080,iw)':-2",
                 "-q:v","2","-frames:v",str(max_frames),"-y",str(Path(output_dir)/"frame_%04d.jpg")]
            p=subprocess.run(cmd,capture_output=True,text=True,timeout=600)
            if p.returncode!=0: raise RuntimeError((p.stderr or "ffmpeg failed")[-2000:])
            frames=sorted(Path(output_dir).glob("frame_*.jpg"))
            if not frames:
                # Short video or sampling issue: attempt first frame.
                fallback=subprocess.run(["ffmpeg","-hide_banner","-loglevel","error","-i",source,
                    "-frames:v","1","-vf","scale='min(1080,iw)':-2","-q:v","2","-y",
                    str(Path(output_dir)/"frame_0001.jpg")],capture_output=True,text=True,timeout=120)
                if fallback.returncode!=0: raise RuntimeError((fallback.stderr or "frame extraction failed")[-2000:])
                frames=sorted(Path(output_dir).glob("frame_*.jpg"))
            shared[task_id]={"status":"completed","frames":[f.name for f in frames],"completed_at":time.time(),"worker":worker_index}
        except Exception as e:
            shared[task_id]={"status":"failed","error":str(e)[:2000],"worker":worker_index,"completed_at":time.time()}

app=FastAPI(title="IR Styleshkon Video Frame Service",version="1.0.0")

@app.on_event("startup")
def startup():
    global workers
    for i in range(WORKER_COUNT):
        p=ctx.Process(target=worker_loop,args=(i,task_queue,tasks),daemon=True)
        p.start(); workers.append(p)

@app.on_event("shutdown")
def shutdown():
    for _ in workers:
        try: task_queue.put_nowait(None)
        except Exception: pass
    for p in workers: p.join(timeout=2)

def auth(key):
    if not API_KEY or key!=API_KEY: raise HTTPException(401,"Unauthorized")

@app.get("/health")
def health():
    return {"ok":True,"cpu_count":CPU_COUNT,"workers":WORKER_COUNT,"queue_limit":task_queue._maxsize}

@app.post("/v1/videos")
async def submit_video(file: UploadFile=File(...), x_api_key: Optional[str]=Header(None)):
    auth(x_api_key)
    if not file.filename: raise HTTPException(400,"Missing filename")
    ext=Path(file.filename).suffix.lower()
    if ext not in {".mp4",".mov",".m4v",".webm"}: raise HTTPException(415,"Unsupported video format")
    task_id=str(uuid.uuid4()); src=UPLOADS/f"{task_id}{ext}"; out=OUTPUTS/task_id
    size=0
    with src.open("wb") as f:
        while True:
            chunk=await file.read(1024*1024)
            if not chunk: break
            size+=len(chunk)
            if size>int(os.getenv("MAX_UPLOAD_BYTES","200000000")):
                src.unlink(missing_ok=True); raise HTTPException(413,"Video too large")
            f.write(chunk)
    interval=max(1,int(os.getenv("FRAME_INTERVAL_SECONDS","1")))
    max_frames=max(1,min(100,int(os.getenv("MAX_FRAMES","12"))))
    tasks[task_id]={"status":"queued","created_at":time.time(),"frames":[]}
    try: task_queue.put_nowait((task_id,str(src),str(out),interval,max_frames))
    except Exception:
        src.unlink(missing_ok=True); tasks.pop(task_id,None); raise HTTPException(503,"Queue full")
    return {"task_id":task_id,"status":"queued","status_url":f"/v1/tasks/{task_id}"}

@app.get("/v1/tasks/{task_id}")
def task_status(task_id:str,x_api_key:Optional[str]=Header(None)):
    auth(x_api_key)
    data=tasks.get(task_id)
    if not data: raise HTTPException(404,"Task not found")
    result=dict(data)
    if result.get("status")=="completed":
        result["frame_urls"]=[f"/v1/tasks/{task_id}/frames/{name}" for name in result.get("frames",[])]
    return result

@app.get("/v1/tasks/{task_id}/frames/{filename}")
def frame(task_id:str,filename:str,x_api_key:Optional[str]=Header(None)):
    auth(x_api_key)
    if "/" in filename or ".." in filename: raise HTTPException(400,"Invalid filename")
    data=tasks.get(task_id)
    if not data or data.get("status")!="completed" or filename not in data.get("frames",[]): raise HTTPException(404,"Frame not found")
    path=OUTPUTS/task_id/filename
    if not path.is_file(): raise HTTPException(404,"Frame missing")
    return FileResponse(path,media_type="image/jpeg",filename=filename)

@app.delete("/v1/tasks/{task_id}")
def delete_task(task_id:str,x_api_key:Optional[str]=Header(None)):
    auth(x_api_key)
    data=tasks.get(task_id)
    if not data: raise HTTPException(404,"Task not found")
    if data.get("status") in {"queued","processing"}: raise HTTPException(409,"Task still running")
    shutil.rmtree(OUTPUTS/task_id,ignore_errors=True)
    for p in UPLOADS.glob(task_id+".*"): p.unlink(missing_ok=True)
    tasks.pop(task_id,None)
    return {"deleted":True}
