# seanime-mover
Python service for personal use that scans `/source` every 60 seconds, moves eligible files into `/dest`, and leaves a zero-byte placeholder behind at the original path.


## Run
```bash
docker compose up -d --build
```

Create nested folders under `./source` and place files there. The container mounts:

- `./source` to `/source`
- `./dest` to `/dest`
- `./logs` to `/logs`

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
