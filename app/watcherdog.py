import time
from app.etl.cmm_parser import scan_excel_files,wait_file_ready,insert_scan_log
from app.services.cmm_service import process_file
from config.db import engine

def start_watcher(folder):

    while True:
        print("🔍 扫描目录:", folder)

        files = scan_excel_files(folder)

        total = len(files)
        new_files  = 0
        failed_files  = 0
        skip_files  = 0
        for file in files:
            if wait_file_ready(file):
                result=process_file(file)
                if result == "SUCCESS":
                    new_files  += 1
                elif result == "SKIP":
                    skip_files += 1
                else :
                    failed_files  += 1
            if file.startswith("~$"):
                continue
         # 🟢 写 scan_log（数据库）
        insert_scan_log(
            engine=engine,
            folder=folder,
            total=total,
            new=new_files,
            skip=skip_files,
            failed=failed_files
        )
        time.sleep(10000)