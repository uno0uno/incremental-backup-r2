#!/usr/bin/env python3
"""
Incremental Backup Script with Cloudflare R2 Storage
Uses docker exec to backup PostgreSQL and uploads to R2.
"""

import subprocess
import os
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BACKUP_DIR = SCRIPT_DIR / "dumps"
STATE_FILE = SCRIPT_DIR / "backup_state.json"


def load_env():
    """Load configuration from .env file."""
    env_file = SCRIPT_DIR / ".env"

    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    value = value.strip().strip('"').strip("'")
                    os.environ.setdefault(key, value)

    return {
        # Docker/DB config
        "container_name": os.environ.get("CONTAINER_NAME", "saifer-postgres-1"),
        "db_name": os.environ.get("DB_NAME"),
        "db_user": os.environ.get("DB_USER"),

        # R2 config
        "r2_account_id": os.environ.get("R2_ACCOUNT_ID"),
        "r2_access_key": os.environ.get("R2_ACCESS_KEY_ID"),
        "r2_secret_key": os.environ.get("R2_SECRET_ACCESS_KEY"),
        "r2_bucket": os.environ.get("R2_BUCKET_NAME"),
        "r2_prefix": os.environ.get("R2_PREFIX", "backups"),

        # Retention
        "keep_local_days": int(os.environ.get("KEEP_LOCAL_DAYS", "7")),
        "keep_remote_days": int(os.environ.get("KEEP_REMOTE_DAYS", "30")),
    }


def get_r2_client(config):
    """Create R2 client using boto3."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=f"https://{config['r2_account_id']}.r2.cloudflarestorage.com",
        aws_access_key_id=config["r2_access_key"],
        aws_secret_access_key=config["r2_secret_key"],
        config=Config(signature_version="s3v4"),
        region_name="auto"
    )


def load_state():
    """Load previous backup state."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_hash": None, "backups": []}


def save_state(state):
    """Save backup state."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def calculate_hash(file_path):
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def create_backup(config):
    """Create backup using docker exec."""
    BACKUP_DIR.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"backup_{config['db_name']}_{timestamp}.sql"
    backup_path = BACKUP_DIR / backup_filename

    print(f"Creating backup...")
    print(f"  Container: {config['container_name']}")
    print(f"  Database: {config['db_name']}")
    print(f"  User: {config['db_user']}")

    cmd = [
        "docker", "exec", config["container_name"],
        "pg_dump",
        "-U", config["db_user"],
        "-d", config["db_name"],
        "--no-owner",
        "--no-acl"
    ]

    try:
        with open(backup_path, 'w') as f:
            result = subprocess.run(
                cmd,
                stdout=f,
                stderr=subprocess.PIPE,
                text=True,
                timeout=600
            )

        if result.returncode == 0 and backup_path.exists() and backup_path.stat().st_size > 0:
            size_mb = backup_path.stat().st_size / (1024 * 1024)
            print(f"  Created: {backup_filename} ({size_mb:.2f} MB)")
            return backup_path
        else:
            print(f"  Failed: {result.stderr}")
            if backup_path.exists():
                backup_path.unlink()
            return None

    except subprocess.TimeoutExpired:
        print("  Timeout after 10 minutes")
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None


def upload_to_r2(config, file_path):
    """Upload backup to Cloudflare R2."""
    if not all([config["r2_account_id"], config["r2_access_key"], config["r2_secret_key"], config["r2_bucket"]]):
        print("\n  R2 not configured, skipping upload")
        return None

    print(f"\nUploading to R2...")

    try:
        client = get_r2_client(config)
        key = f"{config['r2_prefix']}/{file_path.name}"
        file_size = file_path.stat().st_size

        if file_size > 100 * 1024 * 1024:
            print(f"  Multipart upload ({file_size / (1024*1024):.1f} MB)")
            from boto3.s3.transfer import TransferConfig
            transfer_config = TransferConfig(
                multipart_threshold=100 * 1024 * 1024,
                multipart_chunksize=100 * 1024 * 1024,
                max_concurrency=4
            )
            client.upload_file(str(file_path), config["r2_bucket"], key, Config=transfer_config)
        else:
            client.upload_file(str(file_path), config["r2_bucket"], key)

        print(f"  Done: {config['r2_bucket']}/{key}")
        return key

    except Exception as e:
        print(f"  Upload failed: {e}")
        return None


def check_changes(backup_path, state):
    """Check if backup has changed."""
    current_hash = calculate_hash(backup_path)

    if state.get("last_hash") == current_hash:
        print(f"\n  No changes (hash match)")
        return False, current_hash

    print(f"\n  Changes detected")
    return True, current_hash


def cleanup_local(config):
    """Remove old local backups."""
    if not BACKUP_DIR.exists():
        return

    cutoff = datetime.now() - timedelta(days=config["keep_local_days"])
    removed = 0

    for backup in BACKUP_DIR.glob("backup_*.sql"):
        mtime = datetime.fromtimestamp(backup.stat().st_mtime)
        if mtime < cutoff:
            backup.unlink()
            removed += 1

    if removed:
        print(f"  Removed {removed} old local backup(s)")


def cleanup_r2(config):
    """Remove old R2 backups."""
    if not all([config["r2_account_id"], config["r2_access_key"], config["r2_secret_key"], config["r2_bucket"]]):
        return

    try:
        client = get_r2_client(config)
        cutoff = datetime.now() - timedelta(days=config["keep_remote_days"])
        removed = 0

        response = client.list_objects_v2(
            Bucket=config["r2_bucket"],
            Prefix=config["r2_prefix"]
        )

        for obj in response.get("Contents", []):
            obj_time = obj["LastModified"].replace(tzinfo=None)
            if obj_time < cutoff:
                client.delete_object(Bucket=config["r2_bucket"], Key=obj["Key"])
                removed += 1

        if removed:
            print(f"  Removed {removed} old R2 backup(s)")

    except Exception as e:
        print(f"  R2 cleanup error: {e}")


def list_backups(config):
    """List all backups."""
    print("\n" + "=" * 50)
    print("  LOCAL BACKUPS")
    print("=" * 50)

    if BACKUP_DIR.exists():
        backups = sorted(BACKUP_DIR.glob("backup_*.sql"), reverse=True)
        if backups:
            for b in backups:
                size = b.stat().st_size / (1024 * 1024)
                mtime = datetime.fromtimestamp(b.stat().st_mtime)
                print(f"  {b.name} | {size:.2f} MB | {mtime:%Y-%m-%d %H:%M}")
        else:
            print("  (none)")
    else:
        print("  (none)")

    if all([config["r2_account_id"], config["r2_access_key"], config["r2_secret_key"], config["r2_bucket"]]):
        print("\n" + "=" * 50)
        print("  R2 BACKUPS")
        print("=" * 50)

        try:
            client = get_r2_client(config)
            response = client.list_objects_v2(
                Bucket=config["r2_bucket"],
                Prefix=config["r2_prefix"]
            )

            contents = response.get("Contents", [])
            if contents:
                for obj in sorted(contents, key=lambda x: x["LastModified"], reverse=True):
                    size = obj["Size"] / (1024 * 1024)
                    mtime = obj["LastModified"]
                    name = obj["Key"].split("/")[-1]
                    print(f"  {name} | {size:.2f} MB | {mtime:%Y-%m-%d %H:%M}")
            else:
                print("  (none)")

        except Exception as e:
            print(f"  Error: {e}")


def run_backup(force=False):
    """Run incremental backup."""
    print("=" * 50)
    print("  INCREMENTAL BACKUP TO R2")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 50)

    config = load_env()
    state = load_state()

    # Create backup
    backup_path = create_backup(config)
    if not backup_path:
        print("\nBACKUP FAILED!")
        return False

    # Check changes
    needs_upload, current_hash = check_changes(backup_path, state)

    if needs_upload or force:
        r2_key = upload_to_r2(config, backup_path)

        state["last_hash"] = current_hash
        state["last_upload"] = datetime.now().isoformat()

        if r2_key:
            state["backups"].append({
                "file": backup_path.name,
                "hash": current_hash,
                "r2_key": r2_key,
                "timestamp": datetime.now().isoformat()
            })
            state["backups"] = state["backups"][-100:]

        save_state(state)
        print("\nBackup completed!")
    else:
        backup_path.unlink()
        print("\nSkipped (no changes)")

    # Cleanup
    print("\nCleanup:")
    cleanup_local(config)
    cleanup_r2(config)

    return True


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "--list":
            list_backups(load_env())
        elif cmd == "--force":
            run_backup(force=True)
        elif cmd == "--help":
            print("Usage: python backup.py [option]")
            print("")
            print("  (none)   Incremental backup")
            print("  --force  Force upload")
            print("  --list   List backups")
        else:
            print(f"Unknown: {cmd}")
    else:
        run_backup()
