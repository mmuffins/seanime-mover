# seanime-mover
Python service for personal use that scans `/source` every 60 seconds, moves eligible files into `/dest`, and leaves a zero-byte placeholder behind at the original path.

## Configuration
The script supports configuration through environment variables. If a variable is not set, the default value below is used.

| Variable | Default | Description |
| --- | --- | --- |
| `SOURCE_DIR` | `/source` | Root directory that is scanned recursively for files to move. |
| `DEST_DIR` | `/dest` | Flat destination directory where eligible files are moved. |
| `LOG_DIR` | `/config` | Directory where log files are written. |
| `SCAN_INTERVAL_SECONDS` | `60` | Delay between scan cycles. |
| `READY_AGE_SECONDS` | `60` | Minimum file age before a file is considered ready to move. |
| `MIN_FILE_SIZE_BYTES` | `0` | Files at or below this size are skipped. |
| `LOG_RETENTION_DAYS` | `30` | Number of daily log files to retain. |
| `CLEAN_QUEUE_INTERVAL_SECONDS` | `86400` | Delay between `clean_queue()` runs. |
| `PUID` | unset | If set, the container process runs as this user ID instead of root. |
| `PGID` | unset | If set, the container process runs with this group ID instead of root. |

When `PUID` and `PGID` are set, files created in mounted volumes are owned by that UID/GID. If they are not set, the container keeps the previous default behavior and runs as root.

## Tests
Run the unit tests locally with:
```powershell
python -m venv .venv
./.venv/Scripts/Activate.ps1
pip install flake8 pytest
```

```bash
python -m flake8 . --exclude=.venv,__pycache__ --count --select=E9,F63,F7,F82 --show-source --statistics
python -m flake8 . --exclude=.venv,__pycache__ --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
python -m pytest -v
```

## Notes
- If a destination filename already exists, the file is left in place and an error is logged.
- Log rotation uses daily files with 30 retained backups.
- The service is intended to run continuously inside Docker with `restart: unless-stopped`.
