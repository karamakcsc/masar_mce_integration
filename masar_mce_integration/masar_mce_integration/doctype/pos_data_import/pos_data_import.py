# Copyright (c) 2025, KCSC and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import flt, now
from frappe import _

class POSDataImport(Document):

    def validate(self):
        self.check_existing_master_data()
        self.check_available_quantity()

    def on_submit(self):
        self.check_duplicate_invoice()

        if self.status != "Master Data Checked":
            self.db_set("status", self.status)
            self.db_set("rejected_reason", self.rejected_reason or _("Not Master Data Checked"))
            frappe.db.commit()
            frappe.throw(_("Cannot submit: Parent status must be 'Master Data Checked'"))

        rejected_rows = [r.idx for r in (self.items or []) if (r.status or "").strip().lower() == "rejected"]
        if rejected_rows:
            msg = _("Cannot submit: some rows are rejected (idx: {0})").format(", ".join(map(str, rejected_rows)))
            self.db_set("status", "Rejected")
            self.db_set("rejected_reason", msg)
            frappe.db.commit()
            frappe.throw(msg)
        receipt_type = getattr(self, 'receipt_type', '')
        if self.is_return_receipt(receipt_type):
            self.process_pos_return()
        else:  
            self.create_sales_invoice()

    def is_return_receipt(self, receipt_type):
        """Check if the receipt type indicates a return"""
        if not receipt_type:
            return False       
        return_receipt_indicators = [
            "2", 
            2 ,
            "مرتجع", 
            "return",  
            "refund"   
        ]
        receipt_type_str = str(receipt_type).strip().lower()
        return any(indicator.lower() in receipt_type_str for indicator in return_receipt_indicators)

    def check_existing_master_data(self):
        errors = []
        total_qty = flt(0)
        total_amount = flt(0)

        if not self.pos_profile or not frappe.db.exists("POS Profile", self.pos_profile):
            errors.append(_("POS Profile {0} does not exist.").format(self.pos_profile or _("(empty)")))

        if self.payment_method and not frappe.db.exists("Mode of Payment", self.payment_method):
            errors.append(_("Mode of Payment {0} does not exist.").format(self.payment_method))

        for row in (self.items or []):
            if not row.item_code or not frappe.db.exists("Item", row.item_code):
                errors.append(_("Row {0}: Item {1} does not exist.").format(row.idx, row.item_code or _("(empty)")))
            qty = flt(row.quantity)
            amt = flt(row.amount)
            total_qty += qty
            total_amount += amt

        parent_total_qty = flt(self.total_quantity)
        parent_total_amount = flt(
            self.total if self.total is not None
            else self.net_value if self.net_value is not None
            else self.invoice_total if hasattr(self, "invoice_total")
            else 0
        )

        if parent_total_qty != total_qty:
            errors.append(
                _("Total Quantity mismatch: Expected {0}, Found {1}.").format(parent_total_qty, total_qty)
            )

        if abs(parent_total_amount - total_amount) > 0.01:
            errors.append(
                _("Total Amount mismatch: Expected {0}, Found {1}.").format(parent_total_amount, total_amount)
            )

        if errors:
            self.status = "Rejected"
            self.rejected_reason = ", ".join(errors)
            self.db_set("status", self.status)
            self.db_set("rejected_reason", self.rejected_reason)
        else:
            self.status = "Master Data Checked"
            self.rejected_reason = ""
            self.db_set("status", self.status)
            self.db_set("rejected_reason", self.rejected_reason)

        if self.status == "Rejected" and self.docstatus == 1:
            frappe.db.commit()
            frappe.throw(self.rejected_reason)
        else: 
            if self.rejected_reason:
                frappe.msgprint(self.rejected_reason)

    def check_available_quantity(self):
        if self.is_return_receipt(getattr(self, 'receipt_type', '')):
            return

        not_available = []
        warehouse = None

        if self.pos_profile:
            warehouse = frappe.get_value("POS Profile", self.pos_profile, "warehouse")

        if not warehouse:
            self.db_set("status", self.status or "")
            return

        for row in (self.items or []):
            item_code = row.item_code
            if not item_code:
                not_available.append(_("Row {0}: missing item_code").format(row.idx))
                continue

            actual_qty = flt(frappe.get_value("Bin", {"item_code": item_code, "warehouse": warehouse}, "actual_qty") or 0)
            reserved_qty = flt(frappe.get_value("Bin", {"item_code": item_code, "warehouse": warehouse}, "reserved_qty") or 0)
            available = flt(actual_qty) - flt(reserved_qty)
            required = flt(row.quantity)

            if required > available:
                not_available.append(
                    _("Row {0}: Item {1} has insufficient quantity. Available: {2}, Required: {3}.")
                    .format(row.idx, item_code, available, required)
                )

        if not_available:
            self.status = "Rejected"
            self.rejected_reason = ", ".join(not_available)
        else:
            if self.status != "Master Data Checked":
                if not getattr(self, "rejected_reason", None):
                    self.status = "Master Data Checked"
                    self.rejected_reason = ""
            else:
                self.rejected_reason = ""

        self.db_set("status", self.status)
        self.db_set("rejected_reason", self.rejected_reason)

        if self.status == "Rejected" and self.docstatus == 1:
            frappe.db.commit()
            frappe.throw(self.rejected_reason)
        else: 
            frappe.msgprint(self.rejected_reason)

    def check_duplicate_invoice(self):
        inv_pk = getattr(self, "invoice_pk", None)
        if not inv_pk:
            return
        others = frappe.get_all(
            "POS Data Import",
            filters=[
                ["invoice_pk", "=", inv_pk],
                ["name", "!=", self.name or ""]
            ],
            fields=["name", "docstatus", "status"],
            limit_page_length=50
        )

        if not others:
            return
        for e in others:
            if e.get("docstatus") == 1 and (e.get("status") or "").upper() == "SUCCESSFUL":
                self.db_set("status", "DUPLICATE")
                self.db_set("rejected_reason", _("DUPLICATE Invoice from {0}").format(e.get("name")))
                return

            if e.get("docstatus") == 0:
                try:
                    previous = frappe.get_doc("POS Data Import", e.get("name"))
                    previous.db_set("status", "DUPLICATE")
                    previous.db_set("rejected_reason",
                        _("New Import {0} has been submitted for this Invoice").format(self.name)
                    )
                except Exception:
                    frappe.db.commit()
                    frappe.throw(
                        f"Failed to mark previous POS Data Import {e.get('name')} as DUPLICATE"
                    )
    def process_pos_return(self):
        try:
            original_invoice = self.find_original_invoice()
            if not original_invoice:
                self.handle_return_failure(_("Original invoice not found"))
                return
            if original_invoice.docstatus != 1:
                try:
                    original_invoice.submit()
                except Exception as e:
                    self.handle_return_failure(_("Failed to submit original invoice: {0}").format(str(e)))
                    return
            self.create_return_invoice(original_invoice)

        except Exception as e:
            self.handle_return_failure(_("Error processing return: {0}").format(str(e)))

    def find_original_invoice(self):
        market_id = getattr(self, "market_id", None) or getattr(self, "custom_market_id", None)
        refund_receipt_pos_no = getattr(self, "refund_receipt_pos_no", None) or getattr(self, "custom_refund_receipt_pos_no", None)
        refund_receipt_no = getattr(self, "refund_receipt_no", None) or getattr(self, "custom_refund_receipt_no", None)
        filters = {
            "custom_market_id": market_id,
            "custom_pos_no": refund_receipt_pos_no,
            "custom_receipt_no": refund_receipt_no
        }
        if not filters:
            frappe.log_error(f"Missing required fields to find original invoice: market_id={market_id}, refund_receipt_pos_no={refund_receipt_pos_no}, refund_receipt_no={refund_receipt_no}")
            return None

        invoices = frappe.get_all(
            "Sales Invoice",
            filters=filters,
            fields=["name"],
            limit=1
        )

        if not invoices:
            frappe.log_error(f"No original invoice found with filters: {filters}")
            return None

        try:
            return frappe.get_doc("Sales Invoice", invoices[0].name)
        except Exception as e:
            frappe.log_error(f"Error loading original invoice {invoices[0].name}: {str(e)}")
            return None

    def create_return_invoice(self, original_invoice):
        warehouse = frappe.get_value("POS Profile", self.pos_profile, "warehouse")
        return_invoice = frappe.new_doc("Sales Invoice")
        return_invoice.is_pos = 1
        return_invoice.is_return = 1
        return_invoice.return_against = original_invoice.name
        return_invoice.pos_profile = self.pos_profile
        return_invoice.set_posting_time = 1
        if getattr(self, "posting_date", None):
            return_invoice.posting_date = self.posting_date
        if getattr(self, "posting_time", None):
            return_invoice.posting_time = self.posting_time
        self.set_custom_fields_for_return(return_invoice, original_invoice)

        return_invoice.customer = original_invoice.customer
        return_invoice.update_stock = 1
        if warehouse:
            return_invoice.set_warehouse = warehouse
        self.copy_items_for_return(return_invoice, original_invoice)
        self.handle_return_payments(return_invoice)
        return_invoice.insert()
        try:
            return_invoice.submit()
            self.handle_return_success(return_invoice)
        except Exception as e:
            self.handle_return_failure(_("Failed to submit return invoice: {0}").format(str(e)))
            frappe.log_error(f"Return invoice submission error: {str(e)}")
    def set_custom_fields_for_return(self, return_invoice, original_invoice):
        custom_fields_mapping = {
            "market_id": "custom_market_id",
            "receipt_no": "custom_receipt_no", 
            "invoice_pk": "custom_invoice_pk",
            "pay_value": "custom_pay_value",
            "pay_value_visa": "custom_pay_value_visa",
            "reminder_value": "custom_reminder_value",
            "pay_value_check_no": "custom_pay_value_check_no",
            "pay_visa_type": "custom_pay_visa_type",
            "pay_value_check": "custom_pay_value_check",
            "client_name": "custom_client_name",
            "program_id": "custom_program_id",
            "national_id": "custom_national_id",
            "tid": "custom_tid",
            "rrn": "custom_rrn",
            "auth": "custom_auth",
            "pos_no" : "custom_pos_no",
            "offers_id": "custom_offers_id",
            "refund_receipt_no": "custom_refund_receipt_no",
            "refund_receipt_pos_no": "custom_refund_receipt_pos_no",
            "cashier_no": "custom_cashier_no",
            "cashier_name": "custom_cashier_name",
            "customer_no": "custom_customer_no",
            "customer_ref": "custom_customer_ref",
            "customer_type": "custom_customer_type"
        }
        for source_field, target_field in custom_fields_mapping.items():
            source_value = getattr(self, source_field, None)
            if source_value is not None:
                setattr(return_invoice, target_field, source_value)
        return_invoice.custom_pos_data_import = self.name

    def copy_items_for_return(self, return_invoice, original_invoice):
        for original_item in original_invoice.items:
            return_item = return_invoice.append("items", {})
            return_item.item_code = original_item.item_code
            return_item.item_name = original_item.item_name
            return_item.description = original_item.description
            return_item.uom = original_item.uom
            return_item.conversion_factor = original_item.conversion_factor
            return_item.qty = -abs(flt(original_item.qty))
            return_item.rate = flt(original_item.rate)
            return_item.price_list_rate = flt(original_item.price_list_rate)
            return_item.discount_percentage = flt(original_item.discount_percentage)
            return_item.discount_amount = flt(original_item.discount_amount)
            return_item.income_account = original_item.income_account
            return_item.cost_center = original_item.cost_center
            return_item.expense_account = original_item.expense_account
            self.set_custom_fields_for_return_item(return_item, original_item)

    def set_custom_fields_for_return_item(self, return_item, original_item):
        item_custom_fields_mapping = {
            "custom_api_data_income": "custom_api_data_income",
            "custom_pos_data_import": "custom_pos_data_import",
            "custom_pos_data_check": "custom_pos_data_check", 
            "custom_pos_data_import_item": "custom_pos_data_import_item",
            "custom_invoice_pk": "custom_invoice_pk",
            "custom_row_pk": "custom_row_pk"
        }
        for target_field, source_field in item_custom_fields_mapping.items():
            source_value = getattr(original_item, source_field, None)
            if source_value is not None:
                setattr(return_item, target_field, source_value)
        return_item.custom_pos_data_import = self.name

    def handle_return_payments(self, return_invoice):
        payment_amount = flt(getattr(self, "net_value", None) or 0)
        if not payment_amount:
            total = 0
            for item in return_invoice.items:
                total += flt(item.qty) * flt(item.rate)
            payment_amount = abs(total) 
        if getattr(self, "payment_method", None):
            return_invoice.append("payments", {
                "mode_of_payment": self.payment_method,
                "amount": payment_amount
            })
    def handle_return_success(self, return_invoice):
        self.status = "SUCCESSFUL"
        self.rejected_reason = ""
        self.db_set("status", self.status)
        self.db_set("rejected_reason", self.rejected_reason)
        for row in (self.items or []):
            if getattr(row, "pos_data_ckeck", None):
                try:
                    frappe.db.set_value("POS Data Check", row.pos_data_ckeck, "status", "SUCCESSFUL")
                except Exception:
                    frappe.log_error(f"Failed to set POS Data Check {row.pos_data_ckeck} to SUCCESSFUL")

        frappe.msgprint(_("Return invoice {0} created successfully").format(return_invoice.name))

    def handle_return_failure(self, reason):
        self.status = "Failed"
        self.rejected_reason = reason
        self.db_set("status", self.status)
        self.db_set("rejected_reason", self.rejected_reason)
        frappe.log_error(f"POS Return Processing Failed: {reason}")
        if self.docstatus == 1:
            frappe.db.commit()
            frappe.throw(reason)

    def create_sales_invoice(self):
        if self.status != "Master Data Checked":
            self.db_set("status", self.status)
            self.db_set("rejected_reason", self.rejected_reason or _("Not Master Data Checked"))
            frappe.db.commit()
            frappe.throw(self.rejected_reason or _("Not Master Data Checked"))

        if any(((r.status or "").strip().lower() == "rejected") for r in (self.items or [])):
            msg = _("Cannot create Sales Invoice: one or more rows are rejected.")
            self.db_set("status", "Rejected")
            self.db_set("rejected_reason", msg)
            frappe.db.commit()
            frappe.throw(msg)
        warehouse = frappe.get_value("POS Profile", self.pos_profile, "warehouse")
        si = frappe.new_doc("Sales Invoice")
        si.is_pos = 1
        si.pos_profile = self.pos_profile
        si.set_posting_time = 1
        if getattr(self, "posting_date", None):
            si.posting_date = self.posting_date
        if getattr(self, "posting_time", None):
            si.posting_time = self.posting_time
        si.custom_pos_data_import = self.name
        si.customer = frappe.db.get_value("POS Profile", self.pos_profile, "customer")
        si.update_stock = 1
        if warehouse:
            si.set_warehouse = warehouse
        self.set_custom_fields_for_sales_invoice(si)
        for row in (self.items or []):
            qty = flt(row.quantity)
            amount = flt(row.amount)
            rate = flt(amount / qty) if qty else 0.0
            discount_pct = None
            if flt(row.discount_value) and qty:
                try:
                    discount_pct = (flt(row.discount_value) / (rate * qty)) * 100 if rate and qty else 0.0
                except Exception:
                    discount_pct = 0.0
            item_row = si.append("items", {
                "item_code": row.item_code,
                "description": row.item_description,
                "barcode": getattr(row, "barcode", None),
                "qty": qty,
                "price_list_rate": rate + (flt(row.discount_value) / qty if qty else 0.0),
                "rate": rate,
                "discount_percentage": flt(discount_pct) if discount_pct is not None else 0.0,
            })
            self.set_custom_fields_for_sales_invoice_item(item_row, row)
        payment_amount = flt(getattr(self, "net_value", None) or 0)
        si.insert()
        if not payment_amount:
            payment_amount = flt(si.grand_total)
        if getattr(self, "payment_method", None):
            si.append("payments", {
                "mode_of_payment": self.payment_method,
                "amount": payment_amount
            })
        si.save()
        try:
            si.submit()
            self.status = "SUCCESSFUL"
            self.rejected_reason = ""
            self.db_set("status", self.status)
            self.db_set("rejected_reason", self.rejected_reason)
            for row in (self.items or []):
                if getattr(row, "pos_data_ckeck", None):
                    try:
                        frappe.db.set_value("POS Data Check", row.pos_data_ckeck, "status", "SUCCESSFUL")
                    except Exception:
                        frappe.log_error(f"Failed to set POS Data Check {row.pos_data_ckeck} to SUCCESSFUL")
        except Exception as e:
            self.status = "Rejected"
            self.rejected_reason = _("Failed to submit Sales Invoice: {0}").format(str(e))
            self.db_set("status", self.status)
            self.db_set("rejected_reason", self.rejected_reason)
            if self.docstatus == 1:
                frappe.db.commit()
                frappe.throw(self.rejected_reason)
        finally:
            self.db_set("status", self.status)
            self.db_set("rejected_reason", self.rejected_reason)
            
    def set_custom_fields_for_sales_invoice(self, sales_invoice):
        custom_fields_mapping = {
            "market_id": "custom_market_id",
            "receipt_no": "custom_receipt_no",
            "invoice_pk": "custom_invoice_pk",
            "pay_value": "custom_pay_value",
            "pay_value_visa": "custom_pay_value_visa",
            "reminder_value": "custom_reminder_value",
            "pay_value_check_no": "custom_pay_value_check_no",
            "pay_visa_type": "custom_pay_visa_type",
            "pay_value_check": "custom_pay_value_check",
            "client_name": "custom_client_name",
            "program_id": "custom_program_id",
            "national_id": "custom_national_id",
            "tid": "custom_tid",
            "rrn": "custom_rrn",
            "auth": "custom_auth",
            "offers_id": "custom_offers_id",
            "refund_receipt_no": "custom_refund_receipt_no",
            "refund_receipt_pos_no": "custom_refund_receipt_pos_no",
            "cashier_no": "custom_cashier_no",
            "cashier_name": "custom_cashier_name",
            "customer_no": "custom_customer_no",
            "customer_ref": "custom_customer_ref",
            "customer_type": "custom_customer_type"
        }

        for source_field, target_field in custom_fields_mapping.items():
            source_value = getattr(self, source_field, None)
            if source_value is not None:
                setattr(sales_invoice, target_field, source_value)

    def set_custom_fields_for_sales_invoice_item(self, sales_invoice_item, pos_data_import_item):
        """Set custom fields for sales invoice items"""
        item_custom_fields_mapping = {
            "api_ref": "custom_api_data_income",
            "pos_data_check": "custom_pos_data_check",
            "invoice_pk": "custom_invoice_pk",
            "row_pk": "custom_row_pk"
        }
        for source_field, target_field in item_custom_fields_mapping.items():
            source_value = getattr(pos_data_import_item, source_field, None)
            if source_value is not None:
                setattr(sales_invoice_item, target_field, source_value)
        sales_invoice_item.custom_pos_data_import = self.name
        sales_invoice_item.custom_pos_data_import_item = pos_data_import_item.name