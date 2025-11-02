import frappe 
import json
from masar_mce_integration.utils import check_quality_incoming_data , master_data_check , create_sales_invoice_from_data_import
@frappe.whitelist()
def pos_data_integration():
    data_from_api = frappe.request.get_data()
    json_data = json.loads(data_from_api)
    if isinstance(json_data, list):
        json_data = {"records": json_data}
    doc = frappe.new_doc("API Data Income")
    doc.json = str(json_data)
    doc.insert(ignore_permissions=True)
    doc.submit()
    return {"status": "Data Received Successfully", "docname": doc.name}



@frappe.whitelist()
def pos_data_execution():
    frappe.enqueue(pos_data_execution_enq, queue='long', timeout=200000)
    return "POS Data Execution Enqueued Successfully"


def pos_data_execution_enq():
    check_quality_incoming_data()
    master_data_check()
    create_sales_invoice_from_data_import()