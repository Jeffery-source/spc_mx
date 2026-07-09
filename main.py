from scanner.excel_scanner import scan_excel_files
from scanner.file_watcher import start_file_watcher
from app.services.cmm_service import process_file
from config import load_config

if __name__ == "__main__":
    config = load_config()

    folder = config["folder"]

    scan_excel_files(folder)

    # 2. 开始监听
    start_file_watcher(
        folder,
        process_file
    )