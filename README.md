# Incremental Backup to Cloudflare R2

A simple Python script for incremental PostgreSQL backups with automatic upload to Cloudflare R2 storage.

## Features

- **Incremental backups**: Only uploads when database changes (MD5 hash comparison)
- **Docker support**: Uses `docker exec` to run `pg_dump` inside containers
- **Cloudflare R2**: Automatic upload to R2 with multipart support for large files
- **Auto cleanup**: Removes old backups based on retention policy (local and remote)
- **Lightweight**: Single file, minimal dependencies

## Requirements

- Python 3.8+
- Docker (with PostgreSQL container running)
- Cloudflare R2 bucket

## Installation

```bash
git clone https://github.com/uno0uno/incremental-backup-r2.git
cd incremental-backup-r2
pip install -r requirements.txt
```

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `CONTAINER_NAME` | Docker container name running PostgreSQL |
| `DB_NAME` | Database name to backup |
| `DB_USER` | Database user |
| `R2_ACCOUNT_ID` | Cloudflare account ID |
| `R2_ACCESS_KEY_ID` | R2 API access key |
| `R2_SECRET_ACCESS_KEY` | R2 API secret key |
| `R2_BUCKET_NAME` | R2 bucket name |
| `R2_PREFIX` | Folder prefix in bucket (default: `backups`) |
| `KEEP_LOCAL_DAYS` | Days to keep local backups (default: `7`) |
| `KEEP_REMOTE_DAYS` | Days to keep R2 backups (default: `30`) |

## Usage

```bash
# Run incremental backup
python backup.py

# Force upload even if no changes
python backup.py --force

# List all backups (local and R2)
python backup.py --list

# Show help
python backup.py --help
```

## Cron Setup

Run backups automatically every 6 hours:

```bash
crontab -e
```

Add:

```
0 */6 * * * cd /path/to/incremental-backup-r2 && python backup.py >> /var/log/backup.log 2>&1
```

## How It Works

1. Creates a full SQL dump using `docker exec pg_dump`
2. Calculates MD5 hash of the dump
3. Compares with previous backup hash
4. If changed (or `--force`), uploads to R2
5. Cleans up old backups based on retention policy

## File Structure

```
incremental-backup-r2/
├── backup.py           # Main script
├── .env                # Your configuration (not tracked)
├── .env.example        # Example configuration
├── requirements.txt    # Python dependencies
├── backup_state.json   # Tracks last backup hash (auto-generated)
└── dumps/              # Local backup files (auto-created)
```

## License

MIT
