import uuid
import pandas as pd
from config.db import engine
from app.etl.cmm_parser import parse_csv, extract_header, extract_detail,build_datetime,extract_feature_standard,insert_feature_standard,get_file_hash,is_processed,mark_processed
from sqlalchemy import text

def process_file(file_path):

    file_hash = get_file_hash(file_path)

    # ❌ 已处理直接跳过
    if is_processed(engine, file_hash):
        print("⏭ 文件已处理，跳过:", file_path)
        return


    lines = parse_csv(file_path)
    report_id = str(uuid.uuid4())
    # 🟦 header
    header = extract_header(lines)
    header_df = pd.DataFrame([{
        "report_id": report_id,
        "measurement_plan": header.get("measurement_plan"),
        "order_no": header.get("order_no"),
        "drawing_no": header.get("drawing_no"),
        "part_no": header.get("part_no"),
        # ⭐ 关键：合并后的时间
        "measure_time": build_datetime(
            header.get("measure_date"),
            header.get("measure_time")
        )
    }])

    # 🟩 detail
    detail_rows = extract_detail(lines, report_id)
    detail_df = pd.DataFrame(detail_rows,)

    # 🧩 解析标准公差表
    feature_rows = extract_feature_standard(lines,header.get("measure_date"))
    feature_df = pd.DataFrame(feature_rows)

#    # 💾 入库
#    header_df.to_sql("report_header", engine, if_exists="append", index=False)
#    detail_df.to_sql("report_detail", engine, if_exists="append", index=False)
#    insert_feature_standard(engine, feature_df)
#    # 🟢 标记完成
#    mark_processed(engine, file_path.split("/")[-1], file_path, file_hash)
    try:
        # 🟢 开启事务（关键）
        with engine.begin() as conn:

            # 1️⃣ report_header
            header_df.to_sql(
                "report_header",
                conn,
                if_exists="append",
                index=False
            )

            # 2️⃣ report_detail
            detail_df.to_sql(
                "report_detail",
                conn,
                if_exists="append",
                index=False
            )

            # 3️⃣ feature_standard（你的自定义函数）
            insert_feature_standard(conn, feature_df)

            # 4️⃣ file_process_log（必须最后写）
            conn.execute(text("""
                INSERT INTO file_process_log (
                    id, file_name, file_path, file_hash, status, process_time
                )
                VALUES (
                    UUID(), :file_name, :file_path, :file_hash, 'SUCCESS', NOW()
                )
            """), {
                "file_name": file_path.split("/")[-1],
                "file_path": file_path,
                "file_hash": file_hash
            })

        print("✅ 全部入库成功")

    except Exception as e:
        print("❌ 事务失败，已自动回滚:", e)
        raise