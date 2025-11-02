# Copyright (c) 2025, KCSC and contributors
# For license information, please see license.txt

import frappe , json , ast
from frappe.model.document import Document

class APIDataIncome(Document):
    def on_submit(self):
        json_str = self.get('json', "{'records':[]}")
        data_dict = ast.literal_eval(json_str) 
        records_list = data_dict.get("records", [])
        frappe.enqueue(
            "masar_mce_integration.utils.bulk_insert_pos_data",
            data=records_list,
            queue='long',
            timeout=100000
        )
