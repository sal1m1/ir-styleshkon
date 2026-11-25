# IR Styleshkon

Video-frame extraction service for the Iranian server. The foreign app uploads video bytes to this service; this service queues work, extracts frames with FFmpeg, and exposes task status and frame download endpoints.

## Design
- Detects CPU count at startup and launches one worker process per detected logical CPU.
- On Linux, attempts to pin each worker to a distinct CPU affinity core.
- Shared bounded queue (default 100 pending jobs), task status tracking and per-task output directories.
- API-key protection on upload, status, frame download and delete endpoints.
- Upload bytes directly (the Iran host does not need to download Instagram CDN URLs).
- Extracts JPEG frames, default interval 1 second and max 12 frames; environment variables adjust limits.
- Short-video fallback extracts the first frame if sampling yields no frames.

## Install
Clone this repo onto the Iranian server and run:
```bash
chmod +x install.sh
./install.sh
```
Edit `/opt/ir-styleshkon/.env` and set a strong random API key. Then:
```bash
cp /opt/ir-styleshkon/ir-styleshkon.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now ir-styleshkon
systemctl status ir-styleshkon
```
Allow TCP 8090 only from the external server's IP using firewall rules; don't expose it publicly unrestricted.

## API
All API endpoints except health require header `X-API-Key`.

- `GET /health`
- `POST /v1/videos` multipart field `file`; returns task_id
- `GET /v1/tasks/{task_id}` poll status; when completed returns frame_urls
- `GET /v1/tasks/{task_id}/frames/{filename}` retrieve extracted frame JPEG
- `DELETE /v1/tasks/{task_id}` delete completed/failed job and its files

Statuses: queued, processing, completed, failed. Upload limit defaults to 200 MB. Queue capacity defaults to 100.

## External server flow
1. Upload video bytes using multipart `file` and `X-API-Key`.
2. Poll task status every ~1-2 seconds with a deadline.
3. When completed, download frame URLs (also authenticated) and use those frames in the existing media workflow.
4. Delete completed task after frames are consumed.

The API is intentionally not responsible for Instagram CDN fetching or BoxAPI sending.
