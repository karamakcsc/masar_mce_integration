import frappe , json , time
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
    frappe.enqueue(
        "masar_mce_integration.utils.pos_data_execution_wait_and_run",
        queue='default',
        timeout=600
    )
    return {"status": "POS Data Execution waiting for API data to complete. Job started in background."}


def pos_data_execution_wait_and_run():
    not_completed_count = frappe.db.count("API Data Income", {"status": ["!=", "COMPLETED"]})
    if not_completed_count == 0:
        frappe.enqueue(
            "masar_mce_integration.utils.pos_data_execution_enq",
            queue='long',
            timeout=200000
        )
        return
    frappe.enqueue(
        "masar_mce_integration.utils.pos_data_execution_wait_and_run",
        queue='default',
        timeout=600,
        job_name="POS Wait Loop",
        enqueue_after_commit=True,
        at_front=False,
    )



def pos_data_execution_enq():
    check_quality_incoming_data()
    master_data_check()
    create_sales_invoice_from_data_import()