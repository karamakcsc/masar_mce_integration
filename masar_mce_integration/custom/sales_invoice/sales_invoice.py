import frappe 

def on_submit (self , method) : 
    update_pos_data_import_status(self)
    
    
def update_pos_data_import_status(self): 
    frappe.db.set_value(
        'POS Data Import', 
        self.custom_pos_data_import, 
        {
            'status': 'SUCCESSFUL' if self.docstatus == 1 else 'CANCELLED',
            'rejected_reason' : None
        }
    )