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

        self.create_sales_invoice()

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

    def check_available_quantity(self):
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
                    frappe.log_error(
                        f"Failed to mark previous POS Data Import {e.get('name')} as DUPLICATE"
                    )

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

            si.append("items", {
                "item_code": row.item_code,
                "description": row.item_description,
                "barcode": getattr(row, "barcode", None),
                "qty": qty,
                "price_list_rate": rate + (flt(row.discount_value) / qty if qty else 0.0),
                "rate": rate,
                "discount_percentage": flt(discount_pct) if discount_pct is not None else 0.0,
            })

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
