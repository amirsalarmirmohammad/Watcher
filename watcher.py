from pathlib import Path
import logging
import pandas as pd
from datetime import datetime, timedelta



Base_DIR = Path(__file__).resolve().parent
watch_dir = Base_DIR / "watch"
output_dir = Base_DIR / "output"
archive_dir = Base_DIR / "archive"
quarantine_dir = Base_DIR / "quarantine"



for directory in [watch_dir, output_dir, archive_dir, quarantine_dir]:
    directory.mkdir(exist_ok=True)


MAX_AGE_DAYS = 7
MAX_SIZE_MB = 50
ALLOWED_EXTENSIONS = {".csv", ".txt", ".log"}

logging.basicConfig(
    filename=Base_DIR / "watcher.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def main():
    logging.info("Watcher started.")

    now = datetime.now()
    rows = []

    for f in watch_dir.iterdir():
        if f.is_file():
            file_age = now - datetime.fromtimestamp(f.stat().st_mtime)
            file_size_mb = f.stat().st_size / (1024 * 1024)
            file_ext = f.suffix.lower()

            status = "OK"
            if file_age > timedelta(days=MAX_AGE_DAYS):
                status = "Too Old"
                target_dir = archive_dir
            elif file_size_mb > MAX_SIZE_MB:
                status = "Too Large"
                target_dir = quarantine_dir
            elif file_ext not in ALLOWED_EXTENSIONS:
                status = "Invalid Extension"
                target_dir = quarantine_dir
            else:
                target_dir = output_dir

            target_path = target_dir / f.name
            f.rename(target_path)

            logging.info(f"Moved {f.name} to {target_dir} - Status: {status}")

            rows.append({
                "filename": f.name,
                "status": status,
                "size_mb": round(file_size_mb, 2),
                "age_days": file_age.days,
                "moved_to": str(target_dir)
            })


    if not rows:
        logging.info("No files to process.")
        return

    df = pd.DataFrame(rows)
    report_path = Base_DIR / f"report_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(report_path, index=False)
    logging.info(f"Report generated at {report_path}")
        
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise        




