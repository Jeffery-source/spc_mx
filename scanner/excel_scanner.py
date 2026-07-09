import time
from app.etl.cmm_parser import get_excel_files_by_modified_time,wait_file_ready,insert_scan_log
from app.services.cmm_service import process_file
from database.db import engine
from datetime import datetime

def scan_excel_files(folder):

    print(
    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "全量扫描目录:",
    folder
)

    files = get_excel_files_by_modified_time(folder)

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
        # 写 scan_log（数据库）
    insert_scan_log(
        engine=engine,
        folder=folder,
        total=total,
        new=new_files,
        skip=skip_files,
        failed=failed_files
    )
    print(
        f"""
    [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 扫描完成
    目录: {folder}
    总文件数: {total}
    新增处理: {new_files}
    跳过文件: {skip_files}
    失败文件: {failed_files}
    """
    )