from frappe import db , _ 
from frappe.utils import now
from json import loads
import frappe ,  time
import pandas as pd

def bulk_insert_pos_data(data, api_doc, batch_size=10000):
    db.sql("""
        DELETE FROM `tabPOS Data Income`
        WHERE status = 'LOADED'
    """)
    db.commit()
    if not data:
        return {"status": "No data to insert", "count": 0}

    now_str = now()
    df = pd.DataFrame(data)
    primary_keys = tuple(df["invoice_pk"].tolist())
    existing_keys = set()
    if primary_keys:
        placeholders = ", ".join(["%s"] * len(primary_keys))
        existing_keys = set(
            x[0]
            for x in db.sql(
                f"""
                SELECT invoice_pk
                FROM `tabPOS Data Import`
                WHERE invoice_pk IN ({placeholders})
                AND docstatus = 1
                AND status = 'SUCCESSFUL'
                """,
                primary_keys,
            )
        )

    df["status"] = df["invoice_pk"].apply(lambda x: "DUPLICATE" if x in existing_keys else "NEW")
    new_df = df[df["status"] == "NEW"].copy()
    duplicate_count = len(df) - len(new_df)
    new_count = len(new_df)
    if new_count > 0:
        serial_number = db.sql(
            """SELECT COALESCE(MAX(CAST(name AS UNSIGNED)), 0) FROM `tabPOS Data Income`""",
            as_list=True,
        )[0][0]
        serial_number = int(serial_number or 0) + 1

        new_df["name"] = range(serial_number, serial_number + len(new_df))
        new_df["creation"] = now_str
        new_df["modified"] = now_str
        new_df["owner"] = frappe.session.user
        new_df["modified_by"] = frappe.session.user
        new_df["api_ref"] = api_doc
    insert_fields = ['market_id', 'market_description', 'pos_no', 'current_year',
       'receipt_no', 'idx', 'item_code', 'item_description', 'quantity',
       'rate', 'amount', 'row_discount_value', 'barcode', 'offers_id',
       'refund_receipt_no', 'refund_receipt_pos_no', 'receipt_type',
       'cashier_no', 'cashier_name', 'date_timestamp', 'customer_no', 'total',
       'discount_value', 'discount_percent', 'net_value', 'pay_value',
       'pay_value_check', 'pay_value_check_no', 'pay_value_visa',
       'pay_visa_type', 'reminder_value', 'total_quantity', 'client_name',
       'national_id', 'program_id', 'tid', 'rrn', 'auth', 'customer_type',
       'customer_ref', 'row_pk', 'invoice_pk', 'payment_method' ,  'status', 'name', 'creation',
       'modified', 'owner', 'modified_by', 'api_ref']

    for field in insert_fields:
        if field not in new_df.columns:
            new_df[field] = None

    placeholders = "(" + ",".join(["%s"] * len(insert_fields)) + ")"
    total_rows = len(new_df)

    for i in range(0, total_rows, batch_size):
        batch_df = new_df.iloc[i:i + batch_size]
        values = [
            tuple(batch_df[field].iloc[j] for field in insert_fields)
            for j in range(len(batch_df))
        ]
        db.sql(f"""
            INSERT INTO `tabPOS Data Income`
            ({", ".join(insert_fields)})
            VALUES {", ".join([placeholders] * len(values))}
        """, [v for row in values for v in row])
        db.commit()
    db.set_value("API Data Income", api_doc, "status", "COMPLETED")
    db.set_value("API Data Income", api_doc, "new_count", new_count)
    db.set_value("API Data Income", api_doc, "duplicate_count", duplicate_count)
    return {
        "status": "Bulk Insert Completed",
        "new_count": new_count,
        "duplicate_count": duplicate_count
    }

def check_quality_incoming_data():
    data_in_buffer = db.sql("SELECT IFNULL(COUNT(*) , 0 ) From `tabPOS Data Income`")[0][0]
    if data_in_buffer == 0:
        return {"status": "No Data in Buffer", "count": data_in_buffer}
    value = data_quality_check_execute()
    
def data_quality_check_execute():
    user_ = frappe.session.user
    data_in_buffer = db.sql("SELECT IFNULL(COUNT(*) , 0 ) From `tabPOS Data Income`")[0][0]
    db.sql("""SET @base := (
        SELECT IFNULL(MAX(CAST(name AS UNSIGNED)), 100000000000000000)
        FROM `tabPOS Data Check`
    );""")
    db.sql("""
        INSERT INTO `tabPOS Data Check` (
            name,
            creation,
            modified,
            modified_by, 
            owner,
            api_ref, 
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
            auth
        )
        SELECT
            LPAD(@base := @base + 1, 18, '0') AS name,
            NOW() AS creation,
            NOW() AS modified,
            %s AS modified_by,
            %s AS owner,
            tipd.api_ref,
            CASE 
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
                    AND tipd.current_year REGEXP '^[0-9]{4}$'
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
                IF(tipd.current_year NOT REGEXP '^[0-9]{4}$', 'Invalid Year Format (should be YYYY)', NULL),
                IF(tipd.idx NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid IDX', NULL),
                IF(tipd.quantity NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Quantity', NULL),
                IF(tipd.rate NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Rate', NULL),
                IF(tipd.amount NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Amount', NULL),
                IF(tipd.row_discount_value NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Discount Value', NULL),
                IF(tipd.total_quantity NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Total Quantity', NULL),
                IF(tipd.total NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Total', NULL),
                IF(tipd.net_value NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Net Value', NULL),
                IF(tipd.reminder_value NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Reminder Value', NULL),
                IF(tipd.pay_value NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 'Invalid Pay Value', NULL)
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
            tipd.auth
        FROM `tabPOS Data Income` tipd
    """, (user_, user_))

    db.sql("""
        UPDATE `tabPOS Data Income`
        SET status = 'LOADED'
        WHERE status = 'NEW'
    """)
    db.commit()

    return {"status": "Data Quality Check Executed", "count": data_in_buffer}




def master_data_check():
    no_of_rows = db.sql("""
        SELECT IFNULL(COUNT(*), 0) 
        FROM `tabPOS Data Check` 
        WHERE status IN ('Quality Checked', 'Rejected') 
        AND imported = 0
    """)[0][0]
    if no_of_rows == 0:
        return {"status": "No Data in Master Data Check With Quality Checked or Rejected Status", "count": no_of_rows}
    value = master_data_check_execute()  
    return value  

 
def master_data_check_execute():
    frappe.clear_cache()
    frappe.flags.in_import = True
    frappe.flags.mute_emails = True
    frappe.flags.in_migrate = True
    start_time = time.time()
    pos_invoice = db.sql(""" 
        WITH items  AS (
            SELECT 
                name as item_code
            FROM 
                tabItem
        ), 
        pos_profiles AS (
            SELECT 
                name AS pos_profile
            FROM 
                `tabPOS Profile`
        ),
        payment_methods AS (
            SELECT 
                name as payment_method
            FROM
                `tabMode of Payment`
        ), 
        pos_data_row AS (
            SELECT 
                c.item_code , 
                c.barcode ,
                c.item_description ,
                c.quantity ,
                c.rate ,
                c.amount ,
                c.row_discount_value ,
                c.api_ref ,
                c.name as pos_data_check , 
                c.status as pos_data_check_status , 
                c.rejected_reason as pos_data_check_rejected_reason , 
                c.invoice_pk ,
                c.row_pk ,
                c.idx,
                CASE 
                    WHEN i.item_code IS NULL THEN 'Rejected'
                    ELSE 'Checked'
                END AS row_status , 
                CASE
                    WHEN i.item_code IS NULL THEN 
                    CONCAT(
                        c.idx ,  '- Item code not found in Item'
                    )
                END AS row_rejected_reason,
                c.market_id , 
                c.market_description , 
                c.pos_no ,
                c.receipt_no ,
                CAST( c.date_timestamp AS  	DATE ) AS posting_date,
                CAST( c.date_timestamp AS 	TIME ) AS posting_time,
                c.current_year , 
                c.discount_percent ,
                c.discount_value ,
                c.total , 
                c.total_quantity ,
                c.net_value ,
                c.payment_method , 
                c.client_name , 
                c.national_id , 
                c.program_id , 
                c.tid , 
                c.rrn ,
                c.auth ,
                c.offers_id ,
                c.refund_receipt_no , 
                c.refund_receipt_pos_no ,
                c.cashier_no , 
                c.cashier_name ,
                c.customer_no ,
                c.customer_ref , 
                c.customer_type ,
                c.pay_value , 
                c.pay_value_check ,
                c.pay_value_check_no ,
                c.pay_value_visa ,
                c.pay_visa_type ,
                c.reminder_value 
            FROM 
                `tabPOS Data Check`  AS c 
            LEFT JOIN 
                items AS i 
            ON 
                i.item_code = c.item_code 
            WHERE 
                c.imported = 0 
            ORDER BY
                c.market_id , c.pos_no , c.current_year , c.receipt_no , c.idx 
        ), 
        pos_invoice_collecting AS (
            SELECT 
                r.invoice_pk , 
                r.market_id , 
                r.market_description ,
                r.pos_no ,
                r.receipt_no ,
                pro.pos_profile,
                r.posting_date ,
                r.posting_time ,
                r.current_year ,
                r.discount_percent ,
                r.discount_value , 
                r.payment_method ,
                r.net_value ,
                r.client_name , 
                r.national_id , 
                r.program_id , 
                r.tid , 
                r.rrn ,
                r.auth ,
                r.offers_id ,
                r.refund_receipt_no , 
                r.refund_receipt_pos_no ,
                r.cashier_no , 
                r.cashier_name ,
                r.customer_no ,
                r.customer_ref , 
                r.customer_type ,
                r.pay_value , 
                r.pay_value_check ,
                r.pay_value_check_no ,
                r.pay_value_visa ,
                r.pay_visa_type ,
                r.reminder_value , 
                SUM(r.quantity ) AS sum_of_rows_quantity , 
                SUM(r.amount ) AS sum_of_rows_total , 
                MAX(r.total) AS total , 
                MAX(r.total_quantity) AS total_quantity ,
                r.row_status, 
                SUM(CASE WHEN r.row_status  = 'Rejected'  THEN 1 ELSE 0 END ) AS rejected_items_count,
                TRIM(
                    BOTH 
                        ', ' 
                    FROM 
                        GROUP_CONCAT(
                            DISTINCT CASE 
                                WHEN NULLIF(r.row_rejected_reason, '') IS NOT NULL 
                                THEN r.row_rejected_reason 
                                ELSE NULL 
                            END 
                            SEPARATOR ', '
                        )
                ) AS  rejected_reason,
                MAX(CASE WHEN pro.pos_profile IS NOT NULL THEN 1 ELSE 0 END) AS profile_exists,
                MAX(CASE WHEN pm.payment_method IS NOT NULL THEN 1 ELSE 0 END) AS payment_method_exists , 
                JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'item_code' , r.item_code , 
                        'barcode' , r.barcode , 
                        'item_description' , r.item_description , 
                        'quantity' , r.quantity , 
                        'rate' , r.rate , 
                        'amount' , r.amount , 
                        'discount_value' , r.row_discount_value , 
                        'status' , r.row_status , 
                        'pos_data_check' , r.pos_data_check , 
                        'api_ref' , r.api_ref , 
                        'pos_data_check_status' , r.pos_data_check_status , 
                        'pos_data_check_rejected_reason' , r.pos_data_check_rejected_reason , 
                        'rejected_reason' , r.row_rejected_reason , 
                        'invoice_pk' , r.invoice_pk , 
                        'row_pk' , r.row_pk
                    )
                ) AS items
            FROM 
                pos_data_row AS r 
            LEFT JOIN 
                pos_profiles AS pro
            ON  
                pro.pos_profile LIKE CONCAT('%' , r.market_description , '%')
                AND pro.pos_profile LIKE CONCAT( '%-' , r.pos_no )
            LEFT JOIN
                payment_methods AS pm 
            ON
                r.payment_method = pm.payment_method
            GROUP BY 
                r.market_id , r.pos_no , r.current_year , r.receipt_no
        ) , 
        pos_invoice AS (
            SELECT 
                p.invoice_pk , 
                p.market_id , 
                p.market_description , 
                p.pos_no , 
                p.receipt_no , 
                p.pos_profile , 
                p.posting_date , 
                p.posting_time , 
                p.current_year , 
                p.discount_percent , 
                p.discount_value , 
                p.payment_method , 
                p.total , 
                p.total_quantity , 
                p.net_value ,
                p.client_name , 
                p.national_id , 
                p.program_id , 
                p.tid , 
                p.rrn ,
                p.auth ,
                p.offers_id ,
                p.refund_receipt_no , 
                p.refund_receipt_pos_no ,
                p.cashier_no , 
                p.cashier_name ,
                p.customer_no ,
                p.customer_ref , 
                p.customer_type ,
                p.pay_value , 
                p.pay_value_check ,
                p.pay_value_check_no ,
                p.pay_value_visa ,
                p.pay_visa_type ,
                p.reminder_value , 
                p.items , 
                CASE 
                    WHEN p.row_status =  'Rejected' THEN 'Quality Rejected'
                    WHEN p.rejected_items_count > 0 
                        OR  ABS( COALESCE(p.total , 0 ) - COALESCE(p.sum_of_rows_total , 0 )) >0.01 
                        OR 	COALESCE(p.total_quantity , 0 ) <> COALESCE(p.sum_of_rows_quantity , 0 )
                        OR profile_exists = 0 
                        OR payment_method_exists = 0 
                    THEN 
                        'Master Data Rejected'
                    ELSE 
                        'Master Data Checked'
                END AS status , 
                TRIM(
                    BOTH 
                        ', ' 
                    FROM
                        NULLIF(
                            CONCAT_WS(
                                ', ' , 
                                CASE 
                                    WHEN 
                                        p.row_status =  'Rejected' 
                                    THEN 
                                        'Quality Rejected' 
                                    ELSE 
                                        NULL 
                                END,
                                CASE
                                    WHEN
                                        NULLIF(p.rejected_reason , '') IS NOT NULL
                                    THEN
                                        p.rejected_reason
                                    ELSE
                                        NULL
                                END,
                                CASE 
                                    WHEN 
                                        p.profile_exists = 0 
                                    THEN 
                                        CONCAT('POS profile not found: ', p.pos_profile) 
                                    ELSE 
                                        NULL 
                                END,
                                CASE 
                                    WHEN 
                                        payment_method_exists = 0 
                                    THEN 
                                        CONCAT('Payment method not found: ', p.payment_method) 
                                    ELSE 
                                        NULL 
                                END,
                                CASE 
                                    WHEN 
                                        ABS(COALESCE(p.total , 0 ) - COALESCE(p.sum_of_rows_total , 0 )) > 0.01 
                                    THEN 
                                        CONCAT('Invoice amount mismatch: ', ROUND(p.total, 2), ' vs ', ROUND(p.sum_of_rows_total, 2)) 
                                    ELSE NULL 
                                END,
                                CASE 
                                    WHEN 
                                        COALESCE(p.total_quantity , 0 ) <> COALESCE(p.sum_of_rows_quantity , 0 )
                                    THEN 
                                        CONCAT('Quantity mismatch: ', p.total_quantity, ' vs ', (p.sum_of_rows_quantity))
                                    ELSE 
                                        NULL 
                                END  	
                            )
                        , '')
                ) AS rejected_reason
            FROM 
                pos_invoice_collecting p
        )
        SELECT JSON_OBJECT(
                'invoice_pk' , j.invoice_pk , 
                'market_id' , j.market_id , 
                'market_description' , j.market_description , 
                'pos_no' , j.pos_no , 
                'receipt_no' , j.receipt_no , 
                'pos_profile' , j.pos_profile , 
                'posting_date' , j.posting_date , 
                'posting_time' , j.posting_time , 
                'current_year' , j.current_year , 
                'discount_percent' , j.discount_percent, 
                'discount_value' , j.discount_value , 
                'payment_method' , j.payment_method , 
                'total' , j.total , 
                'total_quantity' , j.total_quantity , 
                'net_value' , j.net_value , 
                'client_name' , j.client_name , 
                'national_id' , j.national_id , 
                'program_id' , j.program_id , 
                'tid' , j.tid , 
                'rrn' , j.rrn ,
                'auth' , j.auth , 
                'offers_id' , j.offers_id , 
                'refund_receipt_no' , j.refund_receipt_no , 
                'refund_receipt_pos_no'  , j.refund_receipt_pos_no , 
                'cashier_no' , j.cashier_no , 
                'cashier_name' , j.cashier_name , 
                'customer_no' , j.customer_no , 
                'customer_ref' , j.customer_ref , 
                'customer_type' , j.customer_type , 
                'pay_value' , j.pay_value, 
                'pay_value_check' , j.pay_value_check , 
                'pay_value_check_no' , j.pay_value_check_no , 
                'pay_value_visa' , j.pay_value_visa , 
                'pay_visa_type' , j.pay_visa_type , 
                'reminder_value' , j.reminder_value , 
                'items' , j.items , 
                'status' , j.status , 
                'Rejected_reason' , rejected_reason  
            ) as invoice
        FROM pos_invoice j
    """, as_dict=True)
    parent_values = []
    child_values = []
    batch_size = 5000
    total_processed = 0
    now_str = now()
    serial_number_result = db.sql("""
                SELECT COALESCE(MAX(CAST(name AS UNSIGNED)), 0)
                FROM `tabPOS Data Import`
            """)
    serial_number = int(serial_number_result[0][0]) + 1 if serial_number_result else 1
    for record in pos_invoice:
        data = loads(record.invoice)
        parent_name = f"{serial_number:018d}"
        serial_number += 1
        parent_values.append([
            parent_name,
            now_str, now_str, frappe.session.user, frappe.session.user,
            0,
            data.get("invoice_pk"),
            data.get("status"),
            data.get("rejected_reason"),
            data.get("market_id"),
            data.get("market_description"),
            data.get("pos_no"),
            data.get("pos_profile"),
            data.get("receipt_no"),
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
        for idx, item in enumerate(data.get("items", []), start=1):
            pos_check_name = item.get("pos_data_check")
            child_values.append([
                frappe.generate_hash(length=20),
                now_str, now_str, frappe.session.user, frappe.session.user,
                parent_name, "items", "POS Data Import",
                idx,
                item.get("item_code"),
                item.get("barcode"),
                item.get("item_description"),
                item.get("quantity"),
                item.get("amount"),
                item.get("discount_value"),
                item.get("status"),
                item.get("api_ref"),
                item.get("pos_data_check_status"),
                item.get("pos_data_check_rejected_reason"),
                item.get("rejected_reason"),
                pos_check_name,
                item.get("invoice_pk"),
                item.get("row_pk")
            ])

        total_processed += 1
        if total_processed % batch_size == 0:
            insert_batches(parent_values, child_values)      
            parent_values.clear()
            child_values.clear()   
    if parent_values:
        insert_batches(parent_values, child_values)
    db.commit()
    frappe.flags.in_import = False
    frappe.flags.mute_emails = False
    frappe.flags.in_migrate = False 
    return {"status": "Master Data Check Executed", "count": total_processed}
def mark_pos_check_as_imported(pos_check_names):
    if not pos_check_names:
        return
    names_tuple = tuple(pos_check_names)
    if len(names_tuple) == 1:
        db.sql("""
            UPDATE `tabPOS Data Check`
            SET imported = 1
            WHERE name = %s
        """, names_tuple[0])
    else:
        db.sql(f"""
            UPDATE `tabPOS Data Check`
            SET imported = 1
            WHERE name IN {names_tuple}
        """)
        
def insert_batches(parent_values, child_values):
    if parent_values:
        db.bulk_insert(
            "POS Data Import",
            [
                "name", 
                "creation", 
                "modified", 
                "owner", 
                "modified_by",
                "docstatus",
                "invoice_pk" ,
                "status" ,
                "rejected_reason",
                "market_id", 
                "market_description", 
                "pos_no" ,
                "pos_profile" ,
                "receipt_no" ,
                "posting_date", 
                "posting_time", 
                "current_year",
                "discount_percent", 
                "discount_value", 
                "payment_method", 
                "total_quantity" ,
                "total", 
                "net_value", 
                "client_name", 
                "national_id", 
                "program_id",
                "tid", 
                "rrn", 
                "auth", 
                "offers_id", 
                "refund_receipt_no", 
                "refund_receipt_pos_no",
                "cashier_no", 
                "cashier_name", 
                "customer_no", 
                "customer_ref", 
                "customer_type",
                "pay_value", 
                "pay_value_visa", 
                "reminder_value", 
                "pay_value_check_no",
                "pay_visa_type", 
                "pay_value_check"
            ],
            parent_values,
            ignore_duplicates=True,
        )

    if child_values:
        db.bulk_insert(
            "POS Data Import Item",
            [
                "name", 
                "creation", 
                "modified", 
                "owner", 
                "modified_by",
                "parent", 
                "parentfield", 
                "parenttype", 
                "idx",
                "item_code" , 
                "barcode" , 
                "item_description" , 
                "quantity",
                "amount" , 
                "discount_value" , 
                "status" , 
                "api_ref",
                "pos_data_check_status" , 
                "pos_data_check_rejected_reason" , 
                "rejected_reason",
                "pos_data_check" , 
                "invoice_pk" , 
                "row_pk"
            ],
            child_values,
            ignore_duplicates=True,
        )
        
def create_sales_invoice_from_data_import():
    no_of_rows = db.sql("SELECT IFNULL(COUNT(*), 0) FROM `tabPOS Data Import` WHERE  docstatus = 0")[0][0]
    if no_of_rows == 0:
        return {"status": "No Data in POS Data Import With Master Data Checked Status", "count": no_of_rows}
    value = create_sales_invoice_from_data_import_execute()
    return value
    
def create_sales_invoice_from_data_import_execute():
    pos_data_import = frappe.db.sql("""
        SELECT 
            name
        FROM 
            `tabPOS Data Import` tpdi 
        WHERE 
            tpdi.docstatus = 0 
        ORDER BY 
            tpdi.posting_date, 
            tpdi.posting_time
    """, as_dict=True)

    total_processed = 0
    failed = []

    for record in pos_data_import:
        try:
            pos_data_import_doc = frappe.get_doc("POS Data Import", record.name)
            pos_data_import_doc.run_method("validate")
            if pos_data_import_doc.status == "Master Data Checked":
                pos_data_import_doc.run_method("submit")
            total_processed += 1

        except Exception as e:
            frappe.log_error(
                message=f"POS Data Import {record.name} failed: {str(e)}",
                title="POS Data Import Execution Error"
            )
            try:
                doc = frappe.get_doc("POS Data Import", record.name)
                doc.db_set("status", "Rejected")
                doc.db_set("rejected_reason", str(e))
            except Exception as inner_e:
                frappe.log_error(f"Failed to set Rejected status for {record.name}: {inner_e}")

            failed.append(record.name)
            continue 
    frappe.db.commit() 
    return {
        "status": "Sales Invoice Creation from POS Data Import Executed",
        "processed": total_processed,
        "failed": failed,
        "count": len(pos_data_import)
    }