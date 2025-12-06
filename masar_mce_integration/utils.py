from frappe import db, _
from frappe.utils import now
from json import loads, JSONDecodeError
import frappe, os, shutil, ijson, json, pandas as pd, time
from re import sub
from typing import Any, Union
from ast import literal_eval

def process_single_split_file(split_file_name):
    try:
        print(f"Starting processing for split file: {split_file_name}")
        split_doc = frappe.get_doc("Split File", split_file_name)
        split_doc.reload()
        frappe.db.set_value("Split File", split_doc.name, {
            "status": "Processing",
            "status_description": "Processing split file...",
            "start_time": now()
        }, update_modified=False)
        frappe.db.commit()
        
        source_path = os.path.join(split_doc.file_path, split_doc.file_name)
        settings = frappe.get_single("MCE Integration Setting")
        archive_base = getattr(settings, "archive_file_path", None) or settings.get("archive_file_path") or None
        if not archive_base:
            archive_base = os.path.join(os.path.dirname(split_doc.file_path), "archive")
        
        file_base = os.path.splitext(split_doc.file_name)[0]
        archive_dir = os.path.join(archive_base, file_base)
        complete_dir = os.path.join(archive_dir, "complete")
        failed_dir = os.path.join(archive_dir, "failed")
        os.makedirs(complete_dir, exist_ok=True)
        os.makedirs(failed_dir, exist_ok=True)
        
        if not os.path.exists(source_path):
            msg = f"Split file not found: {source_path}"
            frappe.db.set_value("Split File", split_doc.name, {
                "status": "Failed",
                "status_description": msg,
                "end_time": now()
            }, update_modified=False)
            frappe.db.commit()
            frappe.log_error(msg, "Process Split File")
            return
            
        rows = []
        total_items = 0
        try:
            with open(source_path, "rb") as fh:
                for obj in ijson.items(fh, "item"):
                    total_items += 1
                    if "attachment_url" not in obj:
                        if "attachments" in obj and isinstance(obj["attachments"], (list, tuple)) and obj["attachments"]:
                            first = obj["attachments"][0]
                            if isinstance(first, dict):
                                url = first.get("url") or first.get("file_url") or first.get("download_url") or first.get("href")
                            else:
                                url = first
                            obj["attachment_url"] = url
                        elif "attachments" in obj and isinstance(obj["attachments"], str):
                            obj["attachment_url"] = obj["attachments"]
                        else:
                            obj["attachment_url"] = None
                    obj["split_file_name"] = split_doc.file_name
                    rows.append(obj)
        except Exception as e:
            msg = f"Error reading split file {source_path}: {str(e)}"
            frappe.log_error(msg, "Process Split File - Read Error")
            frappe.db.set_value("Split File", split_doc.name, {
                "status": "Failed",
                "status_description": msg,
                "end_time": now()
            }, update_modified=False)
            frappe.db.commit()
            try:
                dest = os.path.join(failed_dir, split_doc.file_name)
                shutil.move(source_path, dest)
            except Exception:
                try:
                    os.remove(source_path)
                except Exception:
                    pass
            return
            
        if not rows:
            frappe.db.set_value("Split File", split_doc.name, {
                "status": "Completed",
                "status_description": "Split file had no rows to process",
                "end_time": now()
            }, update_modified=False)
            frappe.db.commit()
            try:
                shutil.move(source_path, os.path.join(complete_dir, split_doc.file_name))
            except Exception:
                try:
                    os.remove(source_path)
                except Exception:
                    pass
            return
            
        try:
            insert_result = pos_data_execution_enq(
                rows=rows,
                split_file=split_doc.name,
                active_file_income=split_doc.parent_active_file
            )
            status_desc = f"Processed {total_items} rows from split file."
            frappe.db.set_value("Split File", split_doc.name, {
                "status": "Completed",
                "status_description": status_desc,
                "end_time": now()
            }, update_modified=False)
            frappe.db.commit()
            try:
                shutil.move(source_path, os.path.join(complete_dir, split_doc.file_name))
            except Exception as mv_e:
                frappe.log_error(f"Failed to move processed split file to complete dir: {mv_e}", "Process Split File Move Error")
                try:
                    os.remove(source_path)
                except Exception:
                    pass

        except Exception as e:
            msg = f"Failed inserting rows for split file: {str(e)}"
            frappe.log_error(msg, f"Process Split File - Insert Error")
            frappe.db.set_value("Split File", split_doc.name, {
                "status": "Failed",
                "status_description": msg,
                "end_time": now()
            }, update_modified=False)
            frappe.db.commit()
            try:
                shutil.move(source_path, os.path.join(failed_dir, split_doc.file_name))
            except Exception:
                try:
                    os.remove(source_path)
                except Exception:
                    pass
            return

    except Exception as e:
        frappe.log_error(f"Process Single Split File Unexpected Error: {str(e)}:{frappe.get_traceback()}", "Process Single Split File Unexpected Error")
        try:
            frappe.db.set_value("Split File", split_file_name, {
                "status": "Failed",
                "status_description": "Unexpected error during split file processing",
                "end_time": now()
            }, update_modified=False)
            frappe.db.commit()
        except Exception:
            pass

def pos_data_execution_enq(rows=[], split_file="", active_file_income=""):
    try:
        bulk_insert_result = bulk_insert_from_split_to_pos_data_income(
            rows=rows,
            split_file=split_file,
            active_file_income=active_file_income, 
            batch_size=50000
        )
        quality_check_result = check_quality_incoming_data(split_file)
        master_check_result = master_data_check(split_file)
        invoice_result = create_sales_invoice_from_data_import(split_file)
        cleanup_result = cleanup_pos_tables_for_split_file(split_file)
        return {
            "bulk_insert": bulk_insert_result,
            "quality_check": quality_check_result,
            "master_check": master_check_result,
            "invoice_creation": invoice_result,
            "cleanup": cleanup_result
        }
    except Exception as e:
        frappe.log_error(f"POS Data Execution Error: {str(e)}", "POS Data Execution")
        raise

def bulk_insert_from_split_to_pos_data_income(rows, split_file="", active_file_income="", batch_size=50000):
    try:
        if split_file:
            frappe.db.sql("""
                DELETE FROM `tabPOS Data Income`
                WHERE split_file = %s AND status IN ('LOADED', 'DUPLICATE')
            """, (split_file,))
            frappe.db.commit()
    except Exception as e:
        frappe.log_error(f"Error clearing existing data: {str(e)}", "Bulk Insert Clear Error")
    if not rows:
        return {"status": "No data to insert", "count": 0}
    now_str = now()
    df = pd.DataFrame(rows)
    if "invoice_pk" not in df.columns:
        df["invoice_pk"] = None
    primary_keys = tuple(df["invoice_pk"].dropna().unique().tolist())
    existing_keys = set()
    if primary_keys:
        placeholders = ", ".join(["%s"] * len(primary_keys))
        try:
            existing_result = frappe.db.sql(
                f"""
                SELECT custom_invoice_pk
                FROM `tabSales Invoice`
                WHERE custom_invoice_pk IN ({placeholders})
                AND docstatus = 1
                """,
                primary_keys,
                as_list=True
            )
            existing_keys = {x[0] for x in existing_result if x[0]}
        except Exception as e:
            frappe.log_error(f"Error checking existing invoices: {str(e)}", "Bulk Insert Check Error")
    serial_number_result = frappe.db.sql(
        """SELECT COALESCE(MAX(CAST(name AS UNSIGNED)), 0) FROM `tabPOS Data Income`""",
        as_list=True,
    )
    serial_number = int(serial_number_result[0][0] if serial_number_result else 0) + 1
    df["name"] = range(serial_number, serial_number + len(df))
    df["creation"] = now_str
    df["modified"] = now_str
    df["owner"] = frappe.session.user
    df["modified_by"] = frappe.session.user
    df['active_file_income'] = active_file_income
    df['split_file'] = split_file
    df["docstatus"] = 0
    df["status"] = df["invoice_pk"].apply(lambda x: "DUPLICATE" if x in existing_keys else "NEW")
    insert_fields = [
        'name', 'creation', 'modified', 'modified_by', 'owner', 'docstatus', 'status',
        'idx', 'market_id', 'market_description', 'date_timestamp',
        'receipt_no', 'pos_no', 'item_code', 'item_description', 'barcode',
        'quantity', 'discount_percent', 'discount_value', 'total_quantity',
        'payment_method', 'current_year', 'rate', 'amount', 'offers_id',
        'refund_receipt_no', 'refund_receipt_pos_no', 'receipt_type',
        'cashier_no', 'cashier_name', 'total', 'customer_no', 'net_value',
        'pay_value', 'pay_value_check', 'pay_value_check_no', 'pay_value_visa',
        'reminder_value', 'client_name', 'national_id', 'program_id',
        'tid', 'rrn', 'auth', 'customer_type', 'customer_ref',
        'invoice_pk', 'row_pk', 'row_discount_value',
        'active_file_income', 'split_file'
    ]
    for field in insert_fields:
        if field not in df.columns:
            df[field] = None      
    total_rows = len(df)
    batch_counter = 0
    for i in range(0, total_rows, batch_size):
        batch_df = df.iloc[i:i + batch_size]
        values = []
        for _, row in batch_df.iterrows():
            row_values = tuple(row[field] for field in insert_fields)
            values.append(row_values)
        if values:
            placeholders = "(" + ",".join(["%s"] * len(insert_fields)) + ")"
            sql_values = [item for sublist in values for item in sublist] 
            try:
                frappe.db.sql(f"""
                    INSERT INTO `tabPOS Data Income`
                    ({", ".join(insert_fields)})
                    VALUES {", ".join([placeholders] * len(values))}
                """, sql_values)
                frappe.db.commit()
                batch_counter += len(batch_df)
            except Exception as e:
                frappe.log_error(f"POS Data Execution Error in batch {i}: {str(e)}", "POS Bulk Insert Error")           
    frappe.db.commit()
    return {
        "status": "Bulk Insert Completed",
        "total_inserted": batch_counter
    }
def check_quality_incoming_data(split_file=None):
    if split_file:
        data_in_buffer = frappe.db.sql(
            "SELECT IFNULL(COUNT(*), 0) FROM `tabPOS Data Income` WHERE split_file = %s", 
            (split_file,), as_list=True
        )[0][0]
    else:
        data_in_buffer = frappe.db.sql(
            "SELECT IFNULL(COUNT(*), 0) FROM `tabPOS Data Income`", 
            as_list=True
        )[0][0]
    if data_in_buffer == 0:
        return {"status": "No Data in Buffer", "count": data_in_buffer} 
    return data_quality_check_execute(split_file)
def data_quality_check_execute(split_file=None):
    user_ = frappe.session.user
    frappe.db.sql("""
        SET @base := (
            SELECT IFNULL(MAX(CAST(name AS UNSIGNED)), 100000000000000000)
            FROM `tabPOS Data Check`
        );
    """)
    extra_where = ""
    params = [user_, user_]
    if split_file:
        extra_where = " WHERE tipd.split_file = %s"
        params.extend([split_file])
    query = """
        INSERT INTO `tabPOS Data Check` (
            name,
            creation,
            modified,
            modified_by, 
            owner,
            status,
            rejected_reason,
            invoice_pk,
            row_pk,
            market_id,
            market_description,
            idx,
            pos_no,
            receipt_no,
            current_year,
            item_code,
            item_description,
            barcode,
            quantity,
            rate,
            amount,
            row_discount_value,
            offers_id,
            refund_receipt_no,
            refund_receipt_pos_no,
            cashier_no,
            cashier_name,
            date_timestamp,
            receipt_type,
            customer_no,
            customer_ref,
            customer_type, 
            discount_percent,
            discount_value,
            total_quantity,
            total,
            net_value,
            pay_value,
            pay_value_check_no,
            pay_visa_type,
            pay_value_check,
            pay_value_visa,
            reminder_value,
            client_name,
            national_id,
            program_id,
            tid,
            rrn,
            auth,
            payment_method,
            active_file_income,
            split_file,
            imported
        )
        SELECT
            LPAD(@base := @base + 1, 18, '0') AS name,
            NOW() AS creation,
            NOW() AS modified,
            %s AS modified_by,
            %s AS owner,
            CASE
                WHEN tipd.status = 'DUPLICATE' THEN 'DUPLICATE'
                WHEN 
                    (
                        NULLIF(tipd.date_timestamp, '') IS NOT NULL
                        AND tipd.date_timestamp NOT IN (
                            '0000-00-00', 
                            '0000-00-00 00:00:00', 
                            '0000-00-00 00:00:00.000000'
                        )
                        AND (
                            STR_TO_DATE(REPLACE(LEFT(tipd.date_timestamp,19), 'T', ' '), '%%Y-%%m-%%d %%H:%%i:%%s') IS NOT NULL
                            OR STR_TO_DATE(LEFT(tipd.date_timestamp,10), '%%Y-%%m-%%d') IS NOT NULL
                        )
                    )
                    AND tipd.idx REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.quantity REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.rate REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.amount REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.row_discount_value REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.discount_percent REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.discount_value REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.total_quantity REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.total REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.net_value REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.reminder_value REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.pay_value REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.current_year REGEXP '^[0-9]{{4}}$'
                THEN 'Quality Checked'
                ELSE 'Rejected'
            END AS status,
            CONCAT_WS(', ',
                IF(
                    (
                        NULLIF(tipd.date_timestamp, '') IS NULL
                        OR tipd.date_timestamp IN (
                            '0000-00-00', 
                            '0000-00-00 00:00:00', 
                            '0000-00-00 00:00:00.000000'
                        )
                        OR (
                            STR_TO_DATE(REPLACE(LEFT(tipd.date_timestamp,19), 'T', ' '), '%%Y-%%m-%%d %%H:%%i:%%s') IS NULL
                            AND STR_TO_DATE(LEFT(tipd.date_timestamp,10), '%%Y-%%m-%%d') IS NULL
                        )
                    ),
                    'Invalid Date Timestamp',
                    NULL
                ),
                IF(tipd.current_year NOT REGEXP '^[0-9]{{4}}$', 'Invalid Year Format (should be YYYY)', NULL),
                IF(tipd.idx NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid IDX', NULL),
                IF(tipd.quantity NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Quantity', NULL),
                IF(tipd.rate NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Rate', NULL),
                IF(tipd.amount NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Amount', NULL),
                IF(tipd.row_discount_value NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Discount Value', NULL),
                IF(tipd.total_quantity NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Total Quantity', NULL),
                IF(tipd.total NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Total', NULL),
                IF(tipd.net_value NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Net Value', NULL),
                IF(tipd.reminder_value NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Reminder Value', NULL),
                IF(tipd.pay_value NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Pay Value', NULL),
                IF(tipd.status = 'DUPLICATE' , 'DUPLICATE Invoice' , NULL )
            ) AS rejected_reason,
            tipd.invoice_pk,
            tipd.row_pk,
            tipd.market_id,
            tipd.market_description,  
            tipd.idx,
            tipd.pos_no,
            tipd.receipt_no,
            tipd.current_year,
            tipd.item_code,
            tipd.item_description,
            tipd.barcode,
            tipd.quantity,
            tipd.rate,
            tipd.amount,
            tipd.row_discount_value,
            tipd.offers_id,
            tipd.refund_receipt_no,
            tipd.refund_receipt_pos_no,
            tipd.cashier_no,
            tipd.cashier_name,
            tipd.date_timestamp,
            tipd.receipt_type,
            tipd.customer_no,
            tipd.customer_ref,
            tipd.customer_type, 
            tipd.discount_percent,
            tipd.discount_value,
            tipd.total_quantity,
            tipd.total,
            tipd.net_value,
            tipd.pay_value,
            tipd.pay_value_check_no,
            tipd.pay_visa_type,
            tipd.pay_value_check,
            tipd.pay_value_visa,
            tipd.reminder_value,
            tipd.client_name,
            tipd.national_id,
            tipd.program_id,
            tipd.tid,
            tipd.rrn,
            tipd.auth,
            tipd.payment_method,
            tipd.active_file_income,
            tipd.split_file,
            0 AS imported
        FROM `tabPOS Data Income` tipd
        {extra_where}
    """
    query = query.format(extra_where=extra_where)
    frappe.db.sql(query, tuple(params), as_dict=True)
    if split_file:
        frappe.db.sql("""
            UPDATE `tabPOS Data Income`
            SET status = 'LOADED'
            WHERE status = 'NEW' AND split_file = %s
        """, (split_file,))
    else:
        frappe.db.sql("""
            UPDATE `tabPOS Data Income`
            SET status = 'LOADED'
            WHERE status = 'NEW'
        """, ())
    frappe.db.commit()
    if split_file:
        processed_count = frappe.db.sql(
            "SELECT IFNULL(COUNT(*), 0) FROM `tabPOS Data Income` WHERE split_file = %s", 
            (split_file,), as_list=True
        )[0][0]
    else:
        processed_count = frappe.db.sql(
            "SELECT IFNULL(COUNT(*), 0) FROM `tabPOS Data Income`", 
            (), as_list=True
        )[0][0]
    
    return {"status": "Data Quality Check Executed", "count": processed_count}
def master_data_check(split_file=None):
    extra_where = ""
    params = []
    if split_file:
        extra_where = " AND split_file = %s "
        params.append(split_file)
    query = """
        SELECT IFNULL(COUNT(*), 0)
        FROM `tabPOS Data Check`
        WHERE status IN ('Quality Checked', 'Rejected')
        AND imported = 0
        {extra_where}
    """
    query = query.format(extra_where=extra_where)
    no_of_rows = frappe.db.sql(query, tuple(params), as_list=True)[0][0]
    if no_of_rows == 0:
        return {"status": "No Data in Master Data Check With Quality Checked or Rejected Status", "count": no_of_rows}
    return master_data_check_execute(split_file)

def master_data_check_execute(split_file=None):
    frappe.clear_cache()
    frappe.flags.in_import = True
    frappe.flags.mute_emails = True
    frappe.flags.in_migrate = True

    extra_where = ""
    params = []

    if split_file:
        extra_where = " AND c.split_file = %s "
        params.append(split_file)

    # ---------------------------
    # Build the query correctly
    # ---------------------------
    query_template = """
        WITH items AS (
            SELECT name as item_code FROM tabItem
        ), 
        pos_profiles AS (
            SELECT name AS pos_profile FROM `tabPOS Profile`
        ),
        payment_methods AS (
            SELECT name as payment_method FROM `tabMode of Payment`
        ), 
        pos_data_row AS (
            SELECT 
                c.item_code, 
                c.barcode,
                c.item_description,
                c.quantity,
                c.rate,
                c.amount,
                c.row_discount_value,
                c.name as pos_data_check, 
                c.status as pos_data_check_status, 
                c.rejected_reason as pos_data_check_rejected_reason, 
                c.invoice_pk,
                c.row_pk,
                c.idx,
                CASE 
                    WHEN i.item_code IS NULL THEN 'Rejected'
                    ELSE 'Checked'
                END AS row_status, 
                CASE
                    WHEN i.item_code IS NULL THEN 
                    CONCAT(c.idx, '- Item code not found in Item')
                    ELSE NULL
                END AS row_rejected_reason,
                c.market_id, 
                c.market_description, 
                c.pos_no,
                c.receipt_no,
                DATE(c.date_timestamp) AS posting_date,
                TIME(c.date_timestamp) AS posting_time,
                c.current_year, 
                c.discount_percent,
                c.discount_value,
                c.total, 
                c.total_quantity,
                c.net_value,
                c.payment_method, 
                c.client_name, 
                c.national_id, 
                c.program_id, 
                c.tid, 
                c.rrn,
                c.auth,
                c.offers_id,
                c.refund_receipt_no, 
                c.refund_receipt_pos_no,
                c.cashier_no, 
                c.cashier_name,
                c.customer_no,
                c.customer_ref, 
                c.customer_type,
                c.pay_value, 
                c.pay_value_check,
                c.pay_value_check_no,
                c.pay_value_visa,
                c.pay_visa_type,
                c.reminder_value, 
                c.receipt_type,
                c.active_file_income,
                c.split_file
            FROM `tabPOS Data Check` AS c 
            LEFT JOIN items AS i ON i.item_code = c.item_code 
            WHERE c.imported = 0 
                {extra_where}
            ORDER BY c.market_id, c.pos_no, c.current_year, c.receipt_no, c.idx
        ), 
        pos_invoice_collecting AS (
            SELECT 
                r.invoice_pk, 
                r.market_id, 
                r.market_description,
                r.pos_no,
                r.receipt_no,
                pro.pos_profile,
                r.posting_date,
                r.posting_time,
                r.current_year,
                r.discount_percent,
                r.discount_value, 
                r.payment_method,
                r.net_value,
                r.client_name, 
                r.national_id, 
                r.program_id, 
                r.tid, 
                r.rrn,
                r.auth,
                r.offers_id,
                r.refund_receipt_no, 
                r.refund_receipt_pos_no,
                r.cashier_no, 
                r.cashier_name,
                r.customer_no,
                r.customer_ref,
                r.customer_type,
                r.pay_value, 
                r.pay_value_check,
                r.pay_value_check_no,
                r.pay_value_visa,
                r.pay_visa_type,
                r.reminder_value, 
                r.receipt_type,
                SUM(r.quantity) AS sum_of_rows_quantity, 
                SUM(r.amount) AS sum_of_rows_total, 
                MAX(r.total) AS total, 
                MAX(r.total_quantity) AS total_quantity,
                r.row_status, 
                SUM(CASE WHEN r.row_status = 'Rejected' THEN 1 ELSE 0 END) AS rejected_items_count,
                TRIM(
                    BOTH ', ' FROM 
                    GROUP_CONCAT(
                        DISTINCT CASE 
                            WHEN NULLIF(r.row_rejected_reason, '') IS NOT NULL 
                            THEN r.row_rejected_reason 
                            ELSE NULL 
                        END 
                        SEPARATOR ', '
                    )
                ) AS rejected_reason,
                MAX(CASE WHEN pro.pos_profile IS NOT NULL THEN 1 ELSE 0 END) AS profile_exists,
                MAX(CASE WHEN pm.payment_method IS NOT NULL THEN 1 ELSE 0 END) AS payment_method_exists, 
                JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'item_code', r.item_code,
                        'barcode', r.barcode,
                        'item_description', r.item_description,
                        'quantity', r.quantity,
                        'rate', r.rate,
                        'amount', r.amount,
                        'discount_value', r.row_discount_value,
                        'status', r.row_status,
                        'rejected_reason', r.row_rejected_reason,
                        'invoice_pk', r.invoice_pk,
                        'row_pk', r.row_pk , 
                        'active_file_income', r.active_file_income,
                        'split_file', r.split_file
                    )
                ) AS items,
                MAX(r.active_file_income) AS active_file_income,
                MAX(r.split_file) AS split_file
            FROM pos_data_row AS r 
            LEFT JOIN pos_profiles AS pro ON pro.pos_profile LIKE CONCAT('%%', r.market_description, '%%')
                AND pro.pos_profile LIKE CONCAT('%%-', r.pos_no)
            LEFT JOIN payment_methods AS pm ON r.payment_method = pm.payment_method
            GROUP BY r.market_id, r.pos_no, r.current_year, r.receipt_no
        ), 
        pos_invoice AS (
            SELECT 
                p.invoice_pk, 
                p.market_id, 
                p.market_description, 
                p.pos_no, 
                p.receipt_no, 
                p.pos_profile, 
                p.posting_date, 
                p.posting_time, 
                p.current_year, 
                p.discount_percent, 
                p.discount_value, 
                p.payment_method, 
                p.total, 
                p.total_quantity, 
                p.net_value,
                p.client_name, 
                p.national_id, 
                p.program_id, 
                p.tid, 
                p.rrn,
                p.auth,
                p.offers_id,
                p.refund_receipt_no, 
                p.refund_receipt_pos_no,
                p.cashier_no, 
                p.cashier_name,
                p.customer_no,
                p.customer_ref, 
                p.customer_type,
                p.pay_value, 
                p.pay_value_check,
                p.pay_value_check_no,
                p.pay_value_visa,
                p.pay_visa_type,
                p.reminder_value, 
                p.receipt_type,
                p.items, 
                p.active_file_income,
                p.split_file,
                CASE 
                    WHEN p.row_status = 'Rejected' THEN 'Quality Rejected'
                    WHEN p.rejected_items_count > 0 
                        OR ABS(COALESCE(p.total, 0) - COALESCE(p.sum_of_rows_total, 0)) > 0.01 
                        OR COALESCE(p.total_quantity, 0) <> COALESCE(p.sum_of_rows_quantity, 0)
                        OR profile_exists = 0 
                        OR payment_method_exists = 0 
                    THEN 'Master Data Rejected'
                    ELSE 'Master Data Checked'
                END AS status, 
                TRIM(
                    BOTH ', ' FROM
                    NULLIF(
                        CONCAT_WS(
                            ', ', 
                            CASE WHEN p.row_status = 'Rejected' THEN 'Quality Rejected' ELSE NULL END,
                            CASE WHEN NULLIF(p.rejected_reason, '') IS NOT NULL THEN p.rejected_reason ELSE NULL END,
                            CASE WHEN p.profile_exists = 0 THEN CONCAT('POS profile not found: ', p.pos_profile) ELSE NULL END,
                            CASE WHEN payment_method_exists = 0 THEN CONCAT('Payment method not found: ', p.payment_method) ELSE NULL END,
                            CASE WHEN ABS(COALESCE(p.total, 0) - COALESCE(p.sum_of_rows_total, 0)) > 0.01 
                                THEN CONCAT('Invoice amount mismatch: ', ROUND(p.total, 2), ' vs ', ROUND(p.sum_of_rows_total, 2)) 
                                ELSE NULL 
                            END,
                            CASE WHEN ABS(COALESCE(p.total_quantity, 0) - COALESCE(p.sum_of_rows_quantity, 0)) > 0.01
                                THEN CONCAT('Quantity mismatch: ', p.total_quantity, ' vs ', p.sum_of_rows_quantity)
                                ELSE NULL 
                            END
                        ), ''
                    )
                ) AS rejected_reason
            FROM pos_invoice_collecting p
        )
        SELECT JSON_OBJECT(
            'invoice_pk', j.invoice_pk,
            'market_id', j.market_id,
            'market_description', j.market_description,
            'pos_no', j.pos_no,
            'receipt_no', j.receipt_no,
            'pos_profile', j.pos_profile,
            'posting_date', j.posting_date,
            'posting_time', j.posting_time,
            'current_year', j.current_year,
            'discount_percent', j.discount_percent,
            'discount_value', j.discount_value,
            'payment_method', j.payment_method,
            'total', j.total,
            'total_quantity', j.total_quantity,
            'net_value', j.net_value,
            'client_name', j.client_name,
            'national_id', j.national_id,
            'program_id', j.program_id,
            'tid', j.tid,
            'rrn', j.rrn,
            'auth', j.auth,
            'offers_id', j.offers_id,
            'refund_receipt_no', j.refund_receipt_no,
            'refund_receipt_pos_no', j.refund_receipt_pos_no,
            'cashier_no', j.cashier_no,
            'cashier_name', j.cashier_name,
            'customer_no', j.customer_no,
            'customer_ref', j.customer_ref,
            'customer_type', j.customer_type,
            'pay_value', j.pay_value,
            'pay_value_check', j.pay_value_check,
            'pay_value_check_no', j.pay_value_check_no,
            'pay_value_visa', j.pay_value_visa,
            'pay_visa_type', j.pay_visa_type,
            'reminder_value', j.reminder_value,
            'items', j.items,
            'status', j.status,
            'rejected_reason', j.rejected_reason,
            'receipt_type', j.receipt_type,
            'active_file_income', j.active_file_income,
            'split_file', j.split_file
        ) as invoice
        FROM pos_invoice j
    """
    query = query_template.format(extra_where=extra_where)
    sql_params = tuple(params) if params else ()
    
    if params:
        pos_invoice = frappe.db.sql(query, sql_params, as_dict=True)
    else:
        pos_invoice = frappe.db.sql(query, as_dict=True)
    parent_values = []
    child_values = []
    pos_check_names_to_update = set()
    batch_size = 5000
    total_processed = 0
    now_str = now()
    serial_number_result = frappe.db.sql("""
        SELECT COALESCE(MAX(CAST(name AS UNSIGNED)), 0)
        FROM `tabPOS Data Import`
    """, as_list=True) 
    serial_number = int(serial_number_result[0][0]) + 1 if serial_number_result else 1 
    for record in pos_invoice:
        raw = record.invoice
        data = safe_json_loads(raw)
        if not data:
            continue    
        parent_name = f"{serial_number:018d}"
        serial_number += 1
        parent_values.append([
            parent_name,
            now_str, 
            now_str,
            frappe.session.user, 
            frappe.session.user, 
            0,  
            0, 
            data.get("invoice_pk"),
            data.get("status"),
            data.get("rejected_reason"),
            data.get("split_file"),
            data.get("active_file_income"),
            data.get("market_id"),
            data.get("market_description"),
            data.get("pos_no"),
            data.get("pos_profile"),
            data.get("receipt_no"),
            data.get("receipt_type"),
            data.get("posting_date"),
            data.get("posting_time"),
            data.get("current_year"),
            data.get("discount_percent"),
            data.get("discount_value"),
            data.get("payment_method"),
            data.get("total_quantity"),
            data.get("total"),
            data.get("net_value"),
            data.get("client_name"),
            data.get("national_id"),
            data.get("program_id"),
            data.get("tid"),
            data.get("rrn"),
            data.get("auth"),
            data.get("offers_id"),
            data.get("refund_receipt_no"),
            data.get("refund_receipt_pos_no"),
            data.get("cashier_no"),
            data.get("cashier_name"),
            data.get("customer_no"),
            data.get("customer_ref"),
            data.get("customer_type"),
            data.get("pay_value"),
            data.get("pay_value_visa"),
            data.get("reminder_value"),
            data.get("pay_value_check_no"),
            data.get("pay_visa_type"),
            data.get("pay_value_check")
        ])

        # Prepare child values
        items = data.get("items", [])
        for idx, item in enumerate(items, start=1):
            pos_check_name = item.get("pos_data_check")
            if pos_check_name:
                pos_check_names_to_update.add(pos_check_name)
            
            child_values.append([
                frappe.generate_hash(length=20), 
                now_str, 
                now_str, 
                frappe.session.user, 
                frappe.session.user,
                0, 
                idx,  
                item.get("item_code"),
                item.get("barcode"),
                item.get("item_description"),
                item.get("quantity"),
                item.get("rate"),
                item.get("amount"),
                item.get("discount_value"),
                item.get("status"),
                item.get("rejected_reason"),
                item.get("invoice_pk"),
                item.get("row_pk"),
                data.get("active_file_income"), 
                data.get("split_file"), 
                parent_name, 
                "items", 
                "POS Data Import"  
            ])
        total_processed += 1
        if total_processed % batch_size == 0:
            insert_batches(parent_values, child_values)
            if pos_check_names_to_update:
                mark_pos_check_as_imported(pos_check_names_to_update)
                pos_check_names_to_update.clear()
            parent_values.clear()
            child_values.clear()
    if parent_values:
        insert_batches(parent_values, child_values)
    
    if pos_check_names_to_update:
        mark_pos_check_as_imported(pos_check_names_to_update)
    frappe.db.commit()
    frappe.flags.in_import = False
    frappe.flags.mute_emails = False
    frappe.flags.in_migrate = False
    return {"status": "Master Data Check Executed", "count": total_processed}

def safe_json_loads(raw: Any) -> Union[dict, list, None]:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, bytes):
        try:
            raw = raw.decode('utf-8')
        except UnicodeDecodeError:
            try:
                raw = raw.decode('latin-1')
            except UnicodeDecodeError:
                return None
    if not isinstance(raw, str):
        try:
            raw = str(raw)
        except:
            return None
    raw = raw.strip()

    if not raw:
        return None
    raw = sub(r'^\ufeff', '', raw)
    raw = sub(r'^\ufffe', '', raw)
    try:
        return loads(raw)
    except JSONDecodeError:
        pass
    try:
        fixed = sub(r"(?<=\{|\,|\:|\[)\s*'|'\s*(?=\]|\}|\:|\|,)", '"', raw)
        fixed = sub(r"(?<!\\)'", '"', fixed)
        return loads(fixed)
    except:
        pass
    try:
        result = literal_eval(raw)
        if isinstance(result, (dict, list)):
            return result
    except:
        pass
    try:
        fixed = sub(r'(\w+)\s*:', r'"\1":', raw)
        fixed = sub(r':\s*([^"\'\d\[\]{},\s]+)', r': "\1"', fixed)
        return loads(fixed)
    except:
        pass
    try:
        start_idx = min(
            raw.find('{') if '{' in raw else len(raw),
            raw.find('[') if '[' in raw else len(raw)
        )
        if start_idx < len(raw):
            bracket_stack = []
            end_idx = start_idx
            for i in range(start_idx, len(raw)):
                char = raw[i]
                if char in '{[':
                    bracket_stack.append(char)
                elif char in '}]':
                    if bracket_stack and (
                        (char == '}' and bracket_stack[-1] == '{') or
                        (char == ']' and bracket_stack[-1] == '[')
                    ):
                        bracket_stack.pop()

                if not bracket_stack:
                    end_idx = i + 1
                    break

            partial_json = raw[start_idx:end_idx]
            return loads(partial_json)
    except:
        pass
    return None

def mark_pos_check_as_imported(pos_check_names):
    if not pos_check_names:
        return
    names_tuple = tuple(pos_check_names)
    if len(names_tuple) == 1:
        frappe.db.sql("""
            UPDATE `tabPOS Data Check`
            SET imported = 1
            WHERE name = %s
        """, (names_tuple[0],))
    else:
        frappe.db.sql("""
            UPDATE `tabPOS Data Check`
            SET imported = 1
            WHERE name IN %s
        """, (names_tuple,))

def insert_batches(parent_values, child_values):
    if parent_values:
        frappe.db.bulk_insert(
            "POS Data Import",
            [
                "name", "creation", "modified", "owner", "modified_by",
                "docstatus", "idx", "invoice_pk", "status", "rejected_reason",
                "split_file", "active_file_income", "market_id", "market_description",
                "pos_no", "pos_profile", "receipt_no", "receipt_type", "posting_date",
                "posting_time", "current_year", "discount_percent", "discount_value",
                "payment_method", "total_quantity", "total", "net_value", "client_name",
                "national_id", "program_id", "tid", "rrn", "auth", "offers_id",
                "refund_receipt_no", "refund_receipt_pos_no", "cashier_no", "cashier_name",
                "customer_no", "customer_ref", "customer_type", "pay_value", "pay_value_visa",
                "reminder_value", "pay_value_check_no", "pay_visa_type", "pay_value_check"
            ],
            parent_values,
            ignore_duplicates=True,
        )
    if child_values:
        frappe.db.bulk_insert(
            "POS Data Import Item",
            [
                "name", "creation", "modified", "owner", "modified_by",
                "docstatus", "idx", "item_code", "barcode", "item_description",
                "quantity", "rate", "amount", "discount_value", "status",
                "rejected_reason", "invoice_pk","row_pk", "active_file_income", 
                "split_file", "parent", "parentfield", "parenttype"
            ],
            child_values,
            ignore_duplicates=True,
        )
def create_sales_invoice_from_data_import(split_file_name=None):
    conditions = "WHERE docstatus = 0"
    args = []
    
    if split_file_name:
        conditions += " AND split_file = %s"
        args.append(split_file_name)
        
    no_of_rows = frappe.db.sql(f"SELECT IFNULL(COUNT(*), 0) FROM `tabPOS Data Import` {conditions}", (tuple(args),), as_list=True)[0][0]
    if no_of_rows == 0:
        return {"status": "No Data in POS Data Import With Master Data Checked Status", "count": no_of_rows}
    value = create_sales_invoice_from_data_import_execute(split_file_name=split_file_name)
    return value
    
def create_sales_invoice_from_data_import_execute(split_file_name=None, commit_interval=20):
    receipt_type = (1, 2)
    total_processed = 0
    failed = []  
    conditions = "AND tpdi.split_file = %s" if split_file_name else ""
    args = [split_file_name] if split_file_name else []   
    for r in receipt_type:
        query_args = [r] + args
        pos_data_import = frappe.db.sql(f"""
            SELECT name
            FROM `tabPOS Data Import` tpdi 
            WHERE tpdi.docstatus = 0
            AND CAST(tpdi.receipt_type AS CHAR) IN %s
            {conditions}
            ORDER BY tpdi.posting_date, tpdi.posting_time
        """, (receipt_type, *args), as_dict=True)

        processed_since_commit = 0

        for record in pos_data_import:
            try:
                pos_data_import_doc = frappe.get_doc("POS Data Import", record.name)
                pos_data_import_doc.run_method("validate")
                if pos_data_import_doc.status == "Master Data Checked":
                    pos_data_import_doc.run_method("submit")
                total_processed += 1
                processed_since_commit += 1
            except Exception as e:
                frappe.log_error(
                    message=f"POS Data Import {record.name} failed: {str(e)}",
                    title="POS Data Import Execution Error"
                )
                try:
                    frappe.db.set_value(
                        "POS Data Import",
                        record.name,
                        {
                            "status": "Rejected",
                            "rejected_reason": str(e)[:140],
                        },
                        update_modified=False,
                    )
                except Exception as inner_e:
                    frappe.log_error(
                        f"Failed to set Rejected status for {record.name}: {inner_e}"
                    )

                failed.append(record.name)
                processed_since_commit += 1
            
            if processed_since_commit >= commit_interval:
                frappe.db.commit()
                processed_since_commit = 0
        
        frappe.db.commit()
        
    return {
        "status": "Sales Invoice Creation from POS Data Import Executed",
        "processed": total_processed,
        "failed": failed,
    }

def cleanup_pos_tables_for_split_file(split_file):
    if not split_file:
        return
    try:
        frappe.db.sql("DELETE FROM `tabPOS Data Check` WHERE split_file = %s", (split_file,))
        frappe.db.sql("DELETE FROM `tabPOS Data Income` WHERE split_file = %s", (split_file,))
        frappe.db.commit()
    except Exception as e:
        frappe.log_error(f"Cleanup error for split_file {split_file}: {e}", "Cleanup POS Tables")