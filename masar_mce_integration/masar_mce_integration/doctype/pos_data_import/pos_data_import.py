# Copyright (c) 2025, KCSC and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import flt

class POSDataImport(Document):
	def validate(self):
		self.check_existing_master_data()
		self.check_available_quantity()
	def on_submit(self):
		self.check_existing_master_data()
		self.check_available_quantity()
		self.create_sales_invoice()

	def check_existing_master_data(self):
		not_existing = []
		total_qty, total_amount = 0, 0
		if not frappe.db.exists("POS Profile", self.pos_profile):
			not_existing.append(f"POS Profile {self.pos_profile} does not exist.")
		if not frappe.db.exists("Mode of Payment", self.payment_method):
			not_existing.append(f"Mode of Payment {self.payment_method} does not exist.")
		for i in self.items:
			if not frappe.db.exists("Item", i.item_code):
				not_existing.append(f"Row {i.idx}: Item {i.item_code} does not exist.")
			total_qty += flt(i.quantity)
			total_amount += flt(i.sales_price) * flt(i.quantity)
		if total_qty != flt(self.total_quantity):
			not_existing.append(f"Total Quantity mismatch: Expected {self.total_quantity}, Found {total_qty}.")
		if total_amount != flt(self.invoice_total):
			not_existing.append(f"Total Amount mismatch: Expected {self.invoice_total}, Found {total_amount}.")
		if not_existing:
			self.status = "Rejected"
			self.rejected_reason = ", ".join(not_existing)
			self.db_set("status", self.status)
			self.db_set("rejected_reason", self.rejected_reason)
		else:
			self.status = "Master Data Checked"
			self.rejected_reason = ""
			self.db_set("status", self.status)
			self.db_set("rejected_reason", self.rejected_reason)
		if self.status == "Rejected" and self.docstatus == 1:
			frappe.throw(self.rejected_reason)
   
   
	def check_available_quantity(self):
		not_available = []
		warehouse = frappe.get_value("POS Profile", self.pos_profile, "warehouse")
		for i in self.items:
			actual_qty = frappe.get_value("Bin", {"item_code": i.item_code, "warehouse": warehouse}, "actual_qty")
			reserved_qty = frappe.get_value("Bin", {"item_code": i.item_code, "warehouse": warehouse}, "reserved_qty")
			get_bin = flt(actual_qty) - flt(reserved_qty)
			if flt(i.quantity) > flt(get_bin):
				not_available.append(f"Row {i.idx}: Item {i.item_code} has insufficient quantity. Available: {get_bin}, Required: {i.quantity}.")
		if not_available:
			self.status = "Rejected"
			self.rejected_reason = ", ".join(not_available)
		else:
			self.status = "Master Data Checked"
			self.rejected_reason = ""
		self.db_set("status", self.status)
		self.db_set("rejected_reason", self.rejected_reason)
		if self.status == "Rejected" and self.docstatus == 1:
			frappe.throw(self.rejected_reason)
   
	def create_sales_invoice(self):
		warehouse = frappe.get_value("POS Profile", self.pos_profile, "warehouse")
		si = frappe.new_doc("Sales Invoice")
		si.is_pos = 1 
		si.pos_profile = self.pos_profile
		si.set_posting_time = 1
		si.posting_date = self.posting_date
		si.posting_time = self.posting_time
		si.custom_pos_data_import = self.name
		si.update_stock = 1
		si.set_warehouse = warehouse
		for i in self.items:
			si.append("items", {
				"item_code": i.item_code,
				"description": i.item_description,
				"barcode" : i.barcode,
				"qty": i.quantity,
				"price_list_rate": flt(i.sales_price) + flt(i.discount_value),
				"rate": flt(i.sales_price),
				"discount_percentage": flt(i.discount_percent),
			})
		si.insert()
		si.append("payments", {
			"mode_of_payment": self.payment_method,
			"amount": si.grand_total
		})
		si.save()
		try:
			si.submit()
			self.status = "Submitted"
			self.rejected_reason = ""
			for i in self.items:
				frappe.db.set_value("POS Data Check", i.pos_check_name, "status", "Submitted")
		except Exception as e:
			self.status = "Rejected"
			self.rejected_reason = f"Failed to submit Sales Invoice: {str(e)}"
			if self.docstatus == 1:
				frappe.throw(self.rejected_reason)
		finally:
			self.db_set("status", self.status)
			self.db_set("rejected_reason", self.rejected_reason)
