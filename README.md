# IR Styleshkon

Queued video-frame extraction service for the Iranian server.

## Features
- SQLite-backed task registry (WAL, durable across service restarts).
- Interrupted processing tasks are requeued after restart.
- Queue capacity limits pending tasks; automatically detects available CPU affinity and starts workers.
- FFmpeg limited to one thread per worker; output bounded by frame count.
- API key on all task, upload, frame and deletion endpoints (health is public).
- Automatic cleanup of completed/failed task data after `RETENTION_HOURS` (default 24).
- Orphaned output folders are periodically removed.
- Accepts video bytes from the external server; does not fetch Instagram/CDN URLs.

## Install/update
Run `install.sh` from a clone, configure `.env`, install the systemd unit and start service.
Do not expose port 8090 broadly; firewall it to the external server IP.

## API
- `GET /health`
- `POST /v1/videos` (multipart field `file`)
- `GET /v1/tasks/{task_id}`
- `GET /v1/tasks/{task_id}/frames/{filename}`
- `DELETE /v1/tasks/{task_id}` (terminal tasks only)

Statuses: queued, processing, completed, failed.
