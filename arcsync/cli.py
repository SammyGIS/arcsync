import argparse
from pathlib import Path
from arcsync.scaffolder import create_project_scaffold
from arcsync.sync.sync_manager import SyncManager
from arcsync.config.loader import load_config
from arcsync.utils.logger import get_logger

log = get_logger(__name__)

def main():
    parser = argparse.ArgumentParser(prog="arcsync", description="ArcSync CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Create a new ArcSync project scaffold")
    p_init.add_argument("name", help="Project directory to create")
    p_init.add_argument("--force", action="store_true", help="Overwrite if exists")

    p_sync = sub.add_parser("sync", help="Run a sync using a settings file")
    p_sync.add_argument("--config", "-c", default="settings.yaml", help="Path to settings.yaml")
    p_sync.add_argument("--dry-run", action="store_true", help="Do not write to target; log only")

    args = parser.parse_args()

    if args.cmd == "init":
        target = Path(args.name).resolve()
        create_project_scaffold(target, overwrite=args.force)
        print(f"✅ Project scaffold created at: {target}")
        return

    if args.cmd == "sync":
        cfg = load_config(args.config)
        sm = SyncManager(cfg, dry_run=args.dry_run)
        sm.run()
        return

if __name__ == "__main__":
    main()
