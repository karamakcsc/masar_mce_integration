# Copyright (c) 2025, KCSC and contributors
# For license information, please see license.txt

import frappe
import os
from frappe.model.document import Document
from frappe import _

class ActiveFileIncome(Document):
    def on_submit(self):
        """Process file when document is submitted"""
        try:
            file_full_path = os.path.join(self.file_path, self.file_name)
            aleady_readed_file = frappe.db.sql("""
                SELECT 
                    name 
                FROM 
                    `tabActive File Income` 
                WHERE 
                    file_name = %s 
                    AND status IN ('Completed', 'Reading', 'Processing')
                    AND name != %s
                """, (self.file_name, self.name), as_dict=True)
            
            if aleady_readed_file:
                msg = f"File {self.file_name} is already being processed by document {aleady_readed_file[0].name}"
                frappe.db.set_value(self.doctype, self.name, "status", "Failed")
                frappe.db.set_value(self.doctype, self.name, "status_description", msg)
            frappe.enqueue(
                "masar_mce_integration.tasks.process_active_file_income_into_progress",
                file_income=self.name,
                queue='long',
                timeout=200000,
                is_async=True,
                job_name=f"process_file_{self.name}"
            )
        except Exception as e:
            frappe.log_error(frappe.get_traceback(), "ActiveFileIncome Submit Error")
            frappe.throw(_(f"Failed to start processing: {str(e)}"))