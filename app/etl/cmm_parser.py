import uuid
from datetime import datetime
from sqlalchemy import text
import hashlib
from sqlalchemy import text
import pandas as pd
import os
import time
from datetime import datetime
from sqlalchemy import text

def insert_scan_log(engine, folder, total, new, skip, failed):

    sql = text("""
        INSERT INTO scan_log (
            scan_time,
            total_files,
            new_files,
            skip_files,
            failed_files,
            status,
            folder_path
        )
        VALUES (
            :scan_time,
            :total,
            :new,
            :skip,
            :failed,
            :status,
            :folder
        )
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "scan_time": datetime.now(),
            "total": total,
            "new": new,
            "skip": skip,
            "failed": failed,
            "status": "OK",
            "folder": folder
        })

def wait_file_ready(file_path, wait=2):

    size1 = os.path.getsize(file_path)
    time.sleep(wait)
    size2 = os.path.getsize(file_path)

    return size1 == size2

def scan_excel_files(folder_path):

    files = []

    for root, dirs, filenames in os.walk(folder_path):
        for file in filenames:

            if file.lower().endswith((".xlsx", ".xls")):
                files.append(os.path.join(root, file))

    return files

def load_lines(file_path):

    ext = os.path.splitext(file_path)[1].lower()

    try:
        if ext in [".xlsx", ".xls"]:
            return parse_excel(file_path)

        elif ext == ".csv":
            return parse_csv(file_path)

    except Exception as e:
        print("⚠️ 按类型解析失败，尝试另一种方式")

        # fallback
        try:
            return parse_excel(file_path)
        except:
            return parse_csv(file_path)

    raise ValueError(f"❌ 无法解析文件: {file_path}")

def mark_processed(engine, file_name, file_path, file_hash):
    sql = text("""
        INSERT INTO file_process_log (
            id, file_name, file_path, file_hash, status, process_time
        )
        VALUES (
            UUID(), :file_name, :file_path, :file_hash, 'SUCCESS', NOW()
        )
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "file_name": file_name,
            "file_path": file_path,
            "file_hash": file_hash
        })

def is_processed(engine, file_hash):
    sql = text("""
        SELECT 1 FROM file_process_log
        WHERE file_hash = :file_hash
        LIMIT 1
    """)

    with engine.connect() as conn:
        result = conn.execute(sql, {"file_hash": file_hash}).fetchone()
        return result is not None

def get_file_hash(file_path):
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

def parse_datetime(date_str, time_str):
    try:
        return pd.to_datetime(f"{date_str} {time_str}")
    except:
        return None
    
def build_datetime(date_str, time_str):
    if not date_str or not time_str:
        return None

    # 1️⃣ 解析日期（你的格式：2-Apr-26）
    date_part = datetime.strptime(date_str.strip(), "%d-%b-%y").date()

    # 2️⃣ 解析时间（14:46:53）
    time_part = datetime.strptime(time_str.strip(), "%H:%M:%S").time()

    # 3️⃣ 合并
    return datetime.combine(date_part, time_part)

def parse_excel(file_path):
    df = pd.read_excel(file_path, header=None)

    df = df.fillna("")

    # 👉 转字符串（关键）
    return [[str(cell).strip() for cell in row] for row in df.values.tolist()]

def parse_csv(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return [[cell.strip() for cell in line.split(",")] for line in f]
    except:
        with open(file_path, "r", encoding="gbk") as f:
            return [[cell.strip() for cell in line.split(",")] for line in f]

def extract_header(lines):
    header = {}

    for i, row in enumerate(lines):

        # 转成字符串统一判断（防止Excel类型问题）
        row_str = [str(cell) for cell in row]

        if "Measurement Plan" in row_str:
            next_row = lines[i + 1] if i + 1 < len(lines) else []

            header["measurement_plan"] = next_row[1] if len(next_row) > 1 else None
            header["measure_date"] = next_row[3] if len(next_row) > 3 else None
            header["order_no"] = next_row[5] if len(next_row) > 5 else None

        if "Drawing No." in row_str:
            next_row = lines[i + 1] if i + 1 < len(lines) else []

            header["drawing_no"] = next_row[1] if len(next_row) > 1 else None
            header["part_no"] = next_row[5] if len(next_row) > 5 else None
            header["measure_time"] = next_row[3] if len(next_row) > 3 else None

    return header

def extract_detail(lines, report_id):
    start = False
    rows = []

    for row in lines:
        row_str = [str(cell) for cell in row]

        if row_str and row_str[0] == "Characteristic":
            start = True
            continue

        if start and len(row) >= 6 and row[0]:
            rows.append({
                "id": str(uuid.uuid4()),
                "report_id": report_id,
                "feature_code": row[0],
                "actual": float(row[1]) if row[1] else None,
                "deviation": float(row[5]) if row[5] else None
            })

    return rows

def extract_feature_standard(lines, measuredate):
    start = False
    rows = []

    for row in lines:
        row_str = [str(cell) for cell in row]

        if row_str and row_str[0] == "Characteristic":
            start = True
            continue

        if start and len(row) >= 5 and row[0]:

            rows.append({
                "id": str(uuid.uuid4()),
                "feature_code": row[0],
                "nominal": float(row[2]) if row[2] else None,
                "upper_tol": float(row[3]) if row[3] else None,
                "lower_tol": float(row[4]) if row[4] else None,
                "revision": measuredate,
                "effective_date": datetime.now()
            })

    return rows

# def extract_header(lines):
#     header = {}

#     for i, row in enumerate(lines):

#         # 🟦 找到 Measurement Plan 行
#         if "Measurement Plan" in row:
#             next_row = lines[i + 1] if i + 1 < len(lines) else []

#             header["measurement_plan"] = next_row[1] if len(next_row) > 1 else None
#              # 📅 日期（第5列）
#             header["measure_date"] = next_row[3] if len(next_row) > 3 else None
#             header["order_no"] = next_row[5] if len(next_row) > 5 else None

#         # 🟩 找到 Drawing No 行
#         if "Drawing No." in row:
#             next_row = lines[i + 1] if i + 1 < len(lines) else []

#             header["drawing_no"] = next_row[1] if len(next_row) > 1 else None
#             header["part_no"] = next_row[5] if len(next_row) > 5 else None
#             header["measure_time"] = next_row[3] if len(next_row) > 3 else None

#     return header

# def extract_detail(lines, report_id):
#     start = False
#     rows = []

#     for row in lines:
#         if row and row[0] == "Characteristic":
#             start = True
#             continue

#         if start and len(row) >= 6 and row[0]:
#             rows.append({
#                 "id": str(uuid.uuid4()),
#                 "report_id": report_id,
#                 "feature_code": row[0],
#                 "actual": float(row[1]) if row[1] else None,
#                 "deviation": float(row[5]) if row[5] else None
#             })

#     return rows

# def extract_feature_standard(lines,measuredate):
#     start = False
#     rows = []

#     for row in lines:

#         # 🟦 找到表头
#         if row and row[0] == "Characteristic":
#             start = True
#             continue

#         # 🟩 解析数据区
#         if start and len(row) >= 5 and row[0]:

#             rows.append({
#                 "id": str(uuid.uuid4()), 
#                 "feature_code": row[0],
#                 "nominal": float(row[2]) if row[2] else None,
#                 "upper_tol": float(row[3]) if row[3] else None,
#                 "lower_tol": float(row[4]) if row[4] else None,
#                 "revision":measuredate,
#                 "effective_date":datetime.now()

#             })

#     return rows

def insert_feature_standard(conn, df):

    sql=text("""
    INSERT INTO feature_standard (
        id,
        feature_code,
        nominal,
        upper_tol,
        lower_tol,
        revision,
        effective_date
    )
    VALUES (
        :id,
        :feature_code,
        :nominal,
        :upper_tol,
        :lower_tol,
        :revision,
        :effective_date
    )
    ON DUPLICATE KEY UPDATE
        nominal = VALUES(nominal),
        upper_tol = VALUES(upper_tol),
        lower_tol = VALUES(lower_tol),
        revision = VALUES(revision),
        effective_date = VALUES(effective_date)
    """)

    for _, row in df.iterrows():
        conn.execute(sql, {
            "id": row["id"],
            "feature_code": row["feature_code"],
            "nominal": row["nominal"],
            "upper_tol": row["upper_tol"],
            "lower_tol": row["lower_tol"],
            "revision": row["revision"],
            "effective_date": row["effective_date"]
        })