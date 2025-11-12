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
    for field in ["pos_no", "receipt_no", "market_id", "date_timestamp"]:
        if field not in df.columns:
            df[field] = None

    df["year"] = pd.to_datetime(df["date_timestamp"], errors="coerce").dt.year.fillna(0).astype(int)
    df["primary_key"] = ( df["pos_no"].astype(str)+ "-"+ df["receipt_no"].astype(str)+ "-"+ df["year"].astype(str)+ "-"+ df["market_id"].astype(str))
    primary_keys = tuple(df["primary_key"].tolist())
    existing_keys = set()
    if primary_keys:
        placeholders = ", ".join(["%s"] * len(primary_keys))
        existing_keys = set(
            x[0]
            for x in db.sql(
                f"""
                SELECT primary_key
                FROM `tabPOS Data Import`
                WHERE primary_key IN ({placeholders})
                AND docstatus = 1
                AND status = 'SUCCESSFUL'
                """,
                primary_keys,
            )
        )

    df["status"] = df["primary_key"].apply(lambda x: "DUPLICATE" if x in existing_keys else "NEW")

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
        insert_fields = [
            "name", "creation", "modified", "owner", "modified_by",
            "market_id", "nielsen_code", "market_description", "date_timestamp",
            "day", "receipt_no", "pos_no", "item_code", "barcode", "item_description",
            "sales_price", "quantity", "discount_percent", "discount_value", "total_price",
            "invoice_total", "total_quantity", "payment_method", "date_description",
            "billing_type", "primary_key", "status", "api_ref"
        ]

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
    db.sql(f"""
        INSERT INTO `tabPOS Data Check` (
            name,
            creation,
            modified,
            modified_by, 
            owner,
            market_id,
            market_description,
            status,
            rejected_reason,
            nielsen_code,
            day,
            date_timestamp,
            receipt_no,
            pos_no,
            item_code,
            item_description,
            barcode,
            sales_price,
            quantity,
            discount_percent,
            discount_value,
            invoice_total,
            total_price,
            date_description,
            total_quantity,
            billing_type,
            payment_method,
            api_ref,
            primary_key
        )
        SELECT
            LPAD(@base := @base + 1, 18, '0') AS name,
            NOW() AS creation,
            NOW() AS modified,
            '{user_}' AS modified_by,
            '{user_}' AS owner,
            tipd.market_id,
            tipd.market_description,
            CASE 
                WHEN 
                    (
                        NULLIF(
                            tipd.date_timestamp, '') IS NOT NULL
                        AND 
                            tipd.date_timestamp NOT IN (
                                '0000-00-00', 
                                '0000-00-00 00:00:00', 
                                '0000-00-00 00:00:00.000000'
                            )
                        AND (
                            STR_TO_DATE(
                                LEFT(tipd.date_timestamp,19), '%Y-%m-%d %H:%i:%s'
                            ) IS NOT NULL
                            OR 
                            STR_TO_DATE(
                                LEFT(tipd.date_timestamp,10), '%Y-%m-%d'
                            ) IS NOT NULL
                        )
                    )
                    AND tipd.sales_price REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.quantity REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.discount_percent REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.discount_value REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.invoice_total REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.total_price REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                    AND tipd.total_quantity REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
                THEN 'Quality Checked'
                ELSE 'Rejected'
            END AS status,
            CONCAT_WS(', ',
                IF(
                    (
                        NULLIF(
                            tipd.date_timestamp, '') IS NULL
                        OR tipd.date_timestamp IN (
                            '0000-00-00', 
                            '0000-00-00 00:00:00', 
                            '0000-00-00 00:00:00.000000'
                            )
                        OR (
                            STR_TO_DATE(
                                LEFT(tipd.date_timestamp,19), '%Y-%m-%d %H:%i:%s'
                            ) IS NULL
                            AND 
                            STR_TO_DATE(
                            LEFT(tipd.date_timestamp,10), '%Y-%m-%d'
                            ) IS NULL
                        )
                    ),
                    'Invalid Date Timestamp',
                    NULL
                ),
                IF(
                    tipd.sales_price NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 
                    'Invalid Sales Price', NULL),
                IF(
                    tipd.quantity NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 
                    'Invalid Quantity', NULL),
                IF(
                    tipd.discount_percent NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 
                    'Invalid Discount Percent', NULL),
                IF(
                    tipd.discount_value NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 
                    'Invalid Discount Value', NULL),
                IF(
                    tipd.invoice_total NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 
                    'Invalid Invoice Total', NULL),
                IF(
                    tipd.total_price NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 
                    'Invalid Total Price', NULL),
                IF(
                    tipd.total_quantity NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$', 
                    'Invalid Total Quantity', NULL)
            ) AS rejected_reason,
            tipd.nielsen_code,
            tipd.day,
            tipd.date_timestamp,
            tipd.receipt_no,
            tipd.pos_no,
            tipd.item_code,
            tipd.item_description,
            tipd.barcode,
            tipd.sales_price,
            tipd.quantity,
            tipd.discount_percent,
            tipd.discount_value,
            tipd.invoice_total,
            tipd.total_price,
            tipd.date_description,
            tipd.total_quantity,
            tipd.billing_type,
            tipd.payment_method, 
            tipd.api_ref,
            tipd.primary_key
        FROM `tabPOS Data Income` tipd""")
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
        WITH dedup_item AS (
            SELECT 
                item_code
            FROM 
                tabItem
            GROUP BY 
                item_code
        ),
        valid_profiles AS (
            SELECT 
                name AS pos_profile
            FROM 
                `tabPOS Profile`
        ),
        valid_payments AS (
            SELECT 
                name AS payment_method
            FROM 
                `tabMode of Payment`
        ),
        pos_with_status AS (
            SELECT 
                t.*,
                CASE 
                    WHEN i.item_code IS NULL THEN 'Rejected'
                    ELSE 'Checked'
                END AS item_status,
                CASE
                    WHEN i.item_code IS NULL 
                    THEN CONCAT(
                        ROW_NUMBER() OVER (
                            PARTITION BY t.pos_no, t.receipt_no ORDER BY t.name
                            ),
                        '- Item code not found in Item'
                    )
                    ELSE ''
                END AS item_rejected_reason
            FROM 
                `tabPOS Data Check` t
            LEFT JOIN 
                dedup_item i 
            ON 
                t.item_code = i.item_code
            WHERE 
                t.status IN ('Quality Checked', 'Rejected')
                AND t.imported = 0
        ),
        aggregated_pos AS (
            SELECT
                t.primary_key,
                t.pos_no,
                t.market_id,
                t.market_description,
                t.receipt_no,
                t.nielsen_code,
                CONCAT(
                    t.pos_no, '-', t.market_description
                ) AS pos_profile,
                CAST(
                    t.date_timestamp AS DATE
                ) AS posting_date,
                CAST(
                    t.date_timestamp AS TIME
                ) AS posting_time,
                MAX(
                    t.total_quantity
                ) AS total_quantity,
                MAX(
                    t.invoice_total
                ) AS invoice_total,
                SUM(
                    t.quantity
                ) AS actual_quantity,
                SUM(
                    t.total_price
                ) AS invoice_amount,
                SUM(
                    CASE WHEN t.item_status = 'Rejected' 
                    THEN 1 ELSE 0 END
                ) AS rejected_items_count,
                t.billing_type,
                t.payment_method,
                MAX(t.status) as original_quality_status,
                MAX(t.rejected_reason) as quality_rejected_reason,
                TRIM(BOTH ', ' FROM 
                    GROUP_CONCAT(
                        DISTINCT 
                        CASE 
                            WHEN NULLIF(t.item_rejected_reason, '') IS NOT NULL 
                            THEN t.item_rejected_reason 
                            ELSE NULL 
                        END 
                        SEPARATOR ', '
                    )
                ) AS item_rejected_reasons,
                GROUP_CONCAT(
                    DISTINCT t.payment_method SEPARATOR ', '
                ) AS payment_methods,
                JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'pos_check_name', t.name,
                        'item_code', t.item_code,
                        'item_description', t.item_description,
                        'barcode', t.barcode,
                        'quantity', t.quantity,
                        'sales_price', t.sales_price,
                        'discount_percent', t.discount_percent,
                        'discount_value', t.discount_value,
                        'status', t.item_status,
                        'rejected_reason', t.item_rejected_reason,
                        'quality_status', t.status,
                        'quality_rejected_reason', t.rejected_reason
                    )
                ) AS items,
                MAX(CASE WHEN vp.pos_profile IS NOT NULL THEN 1 ELSE 0 END) AS profile_exists,
                MAX(CASE WHEN pm.payment_method IS NOT NULL THEN 1 ELSE 0 END) AS payment_method_exists
            FROM 
                pos_with_status t
            LEFT JOIN 
                valid_profiles vp 
            ON 
                vp.pos_profile = CONCAT(t.market_description, '-', t.pos_no)
            LEFT JOIN 
                valid_payments pm 
            ON 
                t.payment_method = pm.payment_method
            GROUP BY 
                t.receipt_no, t.pos_no
            ),
        pos_json AS (
            SELECT
                primary_key, 
                pos_no,
                market_id,
                pos_profile,
                market_description,
                nielsen_code,
                receipt_no AS invoice,
                posting_date,
                posting_time,
                total_quantity,
                actual_quantity,
                invoice_amount,
                invoice_total,
                billing_type,
                payment_method,
                CASE 
                    WHEN original_quality_status = 'Rejected' THEN 'Quality Rejected'
                    WHEN rejected_items_count > 0
                        OR ABS(COALESCE(invoice_amount, 0) - COALESCE(invoice_total, 0)) > 0.01
                        OR COALESCE(total_quantity, 0) <> COALESCE(actual_quantity, 0)
                        OR profile_exists = 0
                        OR payment_method_exists = 0
                    THEN 'Master Data Rejected'
                    ELSE 'Master Data Checked'
                END AS status,
                TRIM(BOTH ', ' FROM 
                    NULLIF(
                        CONCAT_WS(', ',
                            CASE WHEN original_quality_status = 'Rejected' THEN quality_rejected_reason ELSE NULL END,
                            CASE WHEN NULLIF(item_rejected_reasons, '') IS NOT NULL THEN item_rejected_reasons ELSE NULL END,
                            CASE WHEN profile_exists = 0 THEN CONCAT('POS profile not found: ', pos_profile) ELSE NULL END,
                            CASE WHEN payment_method_exists = 0 THEN CONCAT('Payment method not found: ', payment_methods) ELSE NULL END,
                            CASE WHEN ABS(COALESCE(invoice_amount, 0) - COALESCE(invoice_total, 0)) > 0.01 
                                THEN CONCAT('Invoice amount mismatch: ', ROUND(invoice_amount, 2), ' vs ', ROUND(invoice_total, 2)) 
                                ELSE NULL END,
                            CASE WHEN COALESCE(total_quantity, 0) <> COALESCE(actual_quantity, 0) 
                                THEN CONCAT('Quantity mismatch: ', actual_quantity, ' vs ', total_quantity) 
                                ELSE NULL END
                        ),
                        ''
                    )
                ) AS rejected_reason,
                items
            FROM aggregated_pos
        )
        SELECT 
            JSON_OBJECT(
                'primary_key; , primary_key, 
                'pos_no, pos_no , 
                'market_id', market_id,
                'market_description', market_description,
                'nielsen_code', nielsen_code,
                'pos_profile', pos_profile,
                'posting_date', posting_date,
                'posting_time', posting_time,
                'total_quantity', total_quantity,
                'invoice_total', invoice_total,
                'billing_type', billing_type,
                'payment_method', payment_method, 
                'items', items,
                'status', status, 
                'rejected_reason', rejected_reason
            ) AS row_data
        FROM pos_json
    """, as_dict=True)
    parent_values = []
    child_values = []
    pos_check_names_to_update = set()  
    batch_size = 5000
    total_processed = 0
    now_str = now()
    serial_number_result = db.sql("SELECT COALESCE(MAX(CAST(name AS UNSIGNED)), 0) FROM `tabPOS Data Import`")
    serial_number = int(serial_number_result[0][0]) + 1 if serial_number_result else 1
    for record in pos_invoice:
        data = loads(record.row_data)
        parent_name = f"{serial_number:018d}"
        serial_number += 1
        parent_values.append([
            parent_name,
            now_str, now_str, frappe.session.user, frappe.session.user,
            0,
            data.get("status"),
            data.get("market_id"),
            data.get("market_description"),
            data.get("pos_no"),
            data.get("primary_key"),
            data.get("nielsen_code"),
            data.get("pos_profile"),
            data.get("posting_date"),
            data.get("posting_time"),
            data.get("total_quantity"),
            data.get("invoice_total"),
            data.get("billing_type"),
            data.get("payment_method"),
            data.get("rejected_reason"),
        ])

        for idx, item in enumerate(data.get("items", []), start=1):
            pos_check_name = item.get("pos_check_name")
            if pos_check_name:
                pos_check_names_to_update.add(pos_check_name)
            rejected_reason = item.get('quality_rejected_reason') or item.get('rejected_reason') or "" 
            child_values.append([
                frappe.generate_hash(length=20),
                now_str, now_str, frappe.session.user, frappe.session.user,
                parent_name, "items", "POS Data Import",
                idx,
                item.get("item_code"),
                item.get("item_description"),
                item.get("barcode"),
                item.get("quantity"),
                item.get("sales_price"),
                item.get("discount_percent"),
                item.get("discount_value"),
                item.get("status"),
                rejected_reason,
                pos_check_name
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
    db.commit()
    frappe.flags.in_import = False
    frappe.flags.mute_emails = False
    frappe.flags.in_migrate = False 
    print(f"Done — Inserted {total_processed} parents in {round(time.time() - start_time, 2)} seconds")
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
                "name", "creation", "modified", "owner", "modified_by",
                "docstatus", "status", "market_id", "market_description", "pos_no", "primary_key",
                "nielsen_code", "pos_profile", "posting_date", "posting_time",
                "total_quantity", "invoice_total", "billing_type",
                "payment_method", "rejected_reason",
            ],
            parent_values,
            ignore_duplicates=True,
        )

    if child_values:
        db.bulk_insert(
            "POS Data Import Item",
            [
                "name", "creation", "modified", "owner", "modified_by",
                "parent", "parentfield", "parenttype", "idx",
                "item_code", "item_description", "barcode",
                "quantity", "sales_price", "discount_percent",
                "discount_value", "status", "rejected_reason",
                "pos_data_check"
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
            tpdi.docstatus =0 
        ORDER BY 
            tpdi.posting_date  , 
            tpdi.posting_time 
        """ , as_dict=True)
    for record in pos_data_import:
        pos_data_import_doc = frappe.get_doc("POS Data Import", record.name)
        pos_data_import_doc.run_method("validate")
        if pos_data_import_doc.status == "Master Data Checked":
            pos_data_import_doc.run_method("submit")
        else:
            continue
    total_processed = len(pos_data_import)
    return {"status": "Sales Invoice Creation from POS Data Import Executed", "count": total_processed}
    