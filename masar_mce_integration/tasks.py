import frappe
import os
import json
import shutil
import time
import ijson

from datetime import datetime
from collections import defaultdict, OrderedDict

def check_active_paths():
    try:
        settings = frappe.get_single("MCE Integration Setting")
        if getattr(settings, "disabled", 0) == 1:
            return
        active_path = settings.active_file_path
        if not active_path or not os.path.exists(active_path):
            frappe.log_error(f"Active path does not exist: {active_path}", "MCE Integration")
            return
        files = [f for f in os.listdir(active_path) if f.lower().endswith(".json")]
        if not files:
            return
        batch_size = int(getattr(settings, "batch_size", 1000) or 1000)
        for file in files:
            try:
                file_path = os.path.join(active_path, file)
                if not is_file_stable(file_path):
                    continue
                doc = frappe.get_doc({
                    "doctype": "Active File Income",
                    "file_name": file,
                    "file_path": active_path,
                    "batch_size": batch_size,
                    "status": "Reading",
                    "status_description": "File detected, waiting to be processed"
                })
                doc.insert(ignore_permissions=True).submit()
                frappe.db.commit()
            except Exception:
                frappe.log_error(frappe.get_traceback(), f"MCE File Processing Error: {file}")
                continue
    except Exception:
        frappe.log_error(frappe.get_traceback(), "MCE Integration Check Error")


def is_file_stable(file_path, check_interval=2, max_attempts=3):
    try:
        if not os.path.exists(file_path):
            return False

        sizes = []
        for _ in range(max_attempts):
            sizes.append(os.path.getsize(file_path))
            time.sleep(check_interval)
        return all(size == sizes[0] for size in sizes)
    except Exception:
        return False

def process_active_file_income_into_progress(file_income):
    try:
        doc = frappe.get_doc("Active File Income", file_income)
        settings = frappe.get_single("MCE Integration Setting")
        frappe.db.set_value("Active File Income", doc.name, {
            "status": "Processing",
            "status_description": "Starting invoice-based file processing...",
            "start_time": frappe.utils.now()
        })
        source_file = os.path.join(doc.file_path, doc.file_name)
        if not os.path.exists(source_file):
            error_msg = f"Source file not found: {source_file}"
            frappe.db.set_value("Active File Income", doc.name, {
                "status": "Failed",
                "status_description": error_msg
            })
            frappe.log_error(error_msg, "MCE File Processing")
            return
        batch_size = int(getattr(doc, "batch_size", 100) or 100)
        if batch_size <= 0:
            batch_size = 100
        elif batch_size > 5000:
            batch_size = 5000
        file_base_name = os.path.splitext(doc.file_name)[0]
        progress_dir = os.path.join(settings.in_progress_path, file_base_name)
        os.makedirs(progress_dir, exist_ok=True)

        temp_file_path = os.path.join(progress_dir, doc.file_name)
        shutil.move(source_file, temp_file_path)
        frappe.db.set_value("Active File Income", doc.name, {
            "file_path": progress_dir,
            "status_description": f"File moved to progress directory"
        })

        file_size_mb = os.path.getsize(temp_file_path) / (1024 * 1024)
        frappe.db.set_value("Active File Income", doc.name, {
            "status_description": f"Processing file ({file_size_mb:.1f} MB)..."
        })
        split_files = split_json_memory_efficient(
            input_file=temp_file_path,
            output_dir=progress_dir,
            invoices_per_file=batch_size,
            file_base=file_base_name,
            doc_name=doc.name
        )
        try:
            os.remove(temp_file_path)
        except Exception:
            pass
        create_split_file_records(doc.name, split_files, progress_dir)

        frappe.db.set_value("Active File Income", doc.name, {
            "status": "Completed",
            "status_description": f"File successfully split into {len(split_files)} batches",
            "end_time": frappe.utils.now()
        })
        process_split_files(doc.name)
    except Exception:
        error_msg = f"Failed to process file: {frappe.get_traceback()}"
        try:
            frappe.db.set_value("Active File Income", file_income, {
                "status": "Failed",
                "status_description": error_msg,
                "end_time": frappe.utils.now()
            })
        except Exception:
            pass
        frappe.log_error(frappe.get_traceback(), "MCE File Processing Error")


def split_json_memory_efficient(input_file, output_dir="output", invoices_per_file=2000, file_base=None, doc_name=None):

    start_time = time.time()
    if not file_base:
        file_base = os.path.splitext(os.path.basename(input_file))[0]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    frappe.logger().info(f"Starting memory-efficient split: {input_file}")
    frappe.logger().info(f"Output dir: {output_dir}  invoices_per_file: {invoices_per_file}")
    seen = OrderedDict() 
    total_items = 0
    try:
        with open(input_file, "rb") as f:
            for obj in ijson.items(f, "item"):
                total_items += 1
                pk = obj.get("invoice_pk")
                if pk and pk not in seen:
                    seen[pk] = 0
    except Exception:
        frappe.log_error(frappe.get_traceback(), "Splitter Pass 1 Error")
        raise
    invoice_keys = list(seen.keys())
    total_invoices = len(invoice_keys)
    if total_invoices == 0:
        frappe.logger().info("No invoice_pk found; nothing to split.")
        return []

    total_batches = (total_invoices + invoices_per_file - 1) // invoices_per_file
    frappe.logger().info(f"Found {total_invoices} invoices grouped in {total_batches} batches (approx {invoices_per_file} invoices per batch).")
    invoice_to_batch = {}
    for batch_index in range(total_batches):
        start = batch_index * invoices_per_file
        end = min((batch_index + 1) * invoices_per_file, total_invoices)
        for pk in invoice_keys[start:end]:
            invoice_to_batch[pk] = batch_index
    writers = []
    split_file_paths = []

    try:
        for batch_index in range(total_batches):
            out_name = f"{file_base}_{batch_index+1:04d}.json"
            out_path = os.path.join(output_dir, out_name)
            f_handle = open(out_path, "w", encoding="utf-8")
            f_handle.write("[\n")
            writers.append({
                "file": f_handle,
                "first": True,
                "count": 0,
                "path": out_path
            })
            split_file_paths.append(out_path)
        with open(input_file, "rb") as f:
            for obj in ijson.items(f, "item"):
                pk = obj.get("invoice_pk")
                if not pk:
                    continue
                batch_index = invoice_to_batch.get(pk)
                if batch_index is None:
                    continue

                writer = writers[batch_index]
                fh = writer["file"]

                if not writer["first"]:
                    fh.write(",\n")
                else:
                    writer["first"] = False

                fh.write(json.dumps(obj, ensure_ascii=False, cls=DecimalEncoder))

                writer["count"] += 1
    except Exception:
        for w in writers:
            try:
                w["file"].close()
            except Exception:
                pass
        frappe.log_error(frappe.get_traceback(), "Splitter Pass 2 Error")
        raise

    for writer in writers:
        try:
            writer["file"].write("\n]")
            writer["file"].close()
        except Exception:
            pass
    for i, w in enumerate(writers, start=1):
        frappe.logger().info(f"Batch {i:04d}: {os.path.basename(w['path'])} -> {w['count']:,} records")
    total_time = time.time() - start_time
    frappe.logger().info(f"Splitting finished in {total_time:.2f} seconds (total items scanned: {total_items:,})")

    return split_file_paths
import json
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
    
    
def validate_json_structure(file_path):
    try:
        if not os.path.exists(file_path):
            frappe.log_error(f"File does not exist: {file_path}", "JSON Validation")
            return False

        with open(file_path, "r", encoding="utf-8") as f:
            first_char = f.read(1)
            if first_char != "[":
                frappe.log_error("JSON file must be a list of objects [...]", "JSON Validation")
                return False
            f.seek(0)
            try:
                parser = ijson.items(open(file_path, "rb"), "item")
                first_obj = next(parser, None)
                if not first_obj:
                    frappe.log_error("Empty JSON list", "JSON Validation")
                    return False
            except Exception:
                frappe.log_error("Unable to parse first JSON object", "JSON Validation")
                return False

        required_fields = ["invoice_pk"]
        for field in required_fields:
            if field not in first_obj:
                frappe.log_error(f"Missing required field: {field}", "JSON Validation")
                return False

        return True

    except Exception:
        frappe.log_error(frappe.get_traceback(), "JSON Validation Error")
        return False

def count_json_records(file_path):
    try:
        cnt = 0
        with open(file_path, "rb") as f:
            for _ in ijson.items(f, "item"):
                cnt += 1
        return cnt
    except Exception:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return len(data) if isinstance(data, list) else 1
        except Exception:
            return 0
def create_split_file_records(parent_doc, split_files, progress_dir):
    try:
        parent = frappe.get_doc("Active File Income", parent_doc)
        for i, file_path in enumerate(split_files, 1):
            file_name = os.path.basename(file_path)
            record_count = count_json_records(file_path)

            split_doc = frappe.get_doc({
                "doctype": "Split File",
                "parent_active_file": parent_doc,
                "file_name": file_name,
                "file_path": progress_dir,
                "batch_number": i,
                "status": "Pending",
                "total_records": record_count
            })
            split_doc.insert(ignore_permissions=True)

            parent.append("split_files", {
                "split_file": split_doc.name,
                "file_name": file_name,
                "batch_number": i,
                "status": "Pending",
                "total_records": record_count,
                "file_path": file_path
            })

        parent.save(ignore_permissions=True)
        frappe.db.commit()
        frappe.logger().info(f"Created {len(split_files)} split file records for {parent_doc}")
    except Exception:
        frappe.log_error(frappe.get_traceback(), "Create Split File Records Error")
        raise
def process_split_files(active_file_name):
    """Start processing each split file"""
    try:
        split_files = frappe.get_all("Split File",
                                    filters={"parent_active_file": active_file_name, "status": "Pending"},
                                    fields=["name", "file_name", "file_path", "batch_number"])
        for split_file in split_files:
            frappe.logger().info(f"Processing split file: {split_file.file_name}")
            # Example: enqueue the per-split-file worker (uncomment & set path)
            # frappe.enqueue(
            #     "masar_mce_integration.tasks.process_single_split_file",
            #     split_file_name=split_file.name,
            #     queue='long',
            #     timeout=3600,
            #     is_async=True,
            #     job_name=f"process_split_{split_file.name}"
            # )
    except Exception:
        frappe.log_error(frappe.get_traceback(), "Process Split Files Error")
