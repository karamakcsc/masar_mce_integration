# Copyright (c) 2025, KCSC and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import flt
from frappe import _
from frappe.exceptions import ValidationError
class POSDataImport(Document):
    def _reject_and_raise(self, message):
        """Persist rejection reason even if submit/validation fails."""
        self.status = "Rejected"
        self.rejected_reason = message

        frappe.db.set_value(
            self.doctype,
            self.name,
            {
                "status": self.status,
                "rejected_reason": self.rejected_reason,
            },
            update_modified=False,
        )
        frappe.db.commit()
        raise ValidationError(message)

    # -------------------------------------------------

    def validate(self):
        self.check_existing_master_data()
        self.check_available_quantity()

    def on_submit(self):
        self.check_duplicate_invoice()
        can_submit = True
        rejection_reasons = []
        if self.status != "Master Data Checked":
            can_submit = False
            rejection_reasons.append(_("Parent status must be 'Master Data Checked'"))
        rejected_rows = [
            r.idx for r in (self.items or [])
            if (r.status or "").strip().lower() == "rejected"
        ]
        if rejected_rows:
            can_submit = False
            rejection_reasons.append(
                _("Some rows are rejected (idx: {0})").format(
                    ", ".join(map(str, rejected_rows))
                )
            )

        if not can_submit:
            msg = ", ".join(rejection_reasons)
            self._reject_and_raise(_("Cannot submit: {0}").format(msg))

        self.create_sales_invoice()

    def check_existing_master_data(self):
        errors = []
        total_qty = flt(0)
        total_amount = flt(0)
        if not self.pos_profile or not frappe.db.exists("POS Profile", self.pos_profile):
            errors.append(
                _("POS Profile {0} does not exist.").format(
                    self.pos_profile or _("(empty)")
                )
            )
        if self.payment_method and not frappe.db.exists("Mode of Payment", self.payment_method):
            errors.append(
                _("Mode of Payment {0} does not exist.").format(self.payment_method)
            )
        for row in (self.items or []):
            if not row.item_code or not frappe.db.exists("Item", row.item_code):
                errors.append(
                    _("Row {0}: Item {1} does not exist.").format(
                        row.idx, row.item_code or _("(empty)")
                    )
                )
            qty = flt(row.quantity)
            amt = flt(row.amount)
            total_qty += qty
            total_amount += amt
        parent_total_qty = flt(self.total_quantity)
        parent_total_amount = flt(
            self.total if self.total is not None else
            self.net_value if self.net_value is not None else
            getattr(self, "invoice_total", 0)
        )
        if parent_total_qty != total_qty:
            errors.append(
                _("Total Quantity mismatch: Expected {0}, Found {1}.").format(
                    parent_total_qty, total_qty
                )
            )
        if abs(parent_total_amount - total_amount) > 0.01:
            errors.append(
                _("Total Amount mismatch: Expected {0}, Found {1}.").format(
                    parent_total_amount, total_amount
                )
            )
        if errors:
            self.status = "Rejected"
            self.rejected_reason = ", ".join(errors)
        else:
            if self.status != "Rejected":
                self.status = "Master Data Checked"
                self.rejected_reason = ""
        frappe.db.set_value(
            self.doctype,
            self.name,
            {
                "status": self.status,
                "rejected_reason": self.rejected_reason,
            },
            update_modified=False,
        )
    def check_available_quantity(self):
        not_available = []
        warehouse = None
        if self.pos_profile:
            warehouse = frappe.get_value("POS Profile", self.pos_profile, "warehouse")
        if not warehouse:
            frappe.db.set_value(
                self.doctype,
                self.name,
                {
                    "status": self.status or "",
                    "rejected_reason": self.rejected_reason or "",
                },
                update_modified=False,
            )
            return
        for row in (self.items or []):
            item_code = row.item_code
            if not item_code:
                not_available.append(_("Row {0}: missing item_code").format(row.idx))
                continue
            actual_qty = flt(
                frappe.get_value(
                    "Bin", {"item_code": item_code, "warehouse": warehouse},
                    "actual_qty"
                ) or 0
            )
            reserved_qty = flt(
                frappe.get_value(
                    "Bin", {"item_code": item_code, "warehouse": warehouse},
                    "reserved_qty"
                ) or 0
            )
            available = actual_qty - reserved_qty
            required = flt(row.quantity)
            if required > available:
                not_available.append(
                    _("Row {0}: Item {1} has insufficient quantity. "
                      "Available: {2}, Required: {3}.").format(
                          row.idx, item_code, available, required
                      )
                )

        if not_available:
            self.status = "Rejected"
            self.rejected_reason = ", ".join(not_available)
        else:
            if self.status == "Master Data Checked":
                self.rejected_reason = ""
        frappe.db.set_value(
            self.doctype,
            self.name,
            {
                "status": self.status,
                "rejected_reason": self.rejected_reason,
            },
            update_modified=False,
        )

    def create_sales_invoice(self):
        if self.status != "Master Data Checked":
            msg = self.rejected_reason or _("Not Master Data Checked")
            self._reject_and_raise(
                _("Cannot create Sales Invoice: {0}").format(msg)
            )

        rejected_rows = [
            r for r in (self.items or [])
            if (r.status or "").strip().lower() == "rejected"
        ]
        if rejected_rows:
            msg = _("Cannot create Sales Invoice: {0} rows are rejected.").format(
                len(rejected_rows)
            )
            self._reject_and_raise(msg)

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
            discount_pct = 0.0

            if flt(row.discount_value) and qty:
                try:
                    discount_pct = (
                        (flt(row.discount_value) / (rate * qty)) * 100
                        if rate and qty else 0.0
                    )
                except Exception:
                    discount_pct = 0.0
            si.append("items", {
                "item_code": row.item_code,
                "description": row.item_description,
                "barcode": getattr(row, "barcode", None),
                "qty": qty,
                "price_list_rate": rate + (flt(row.discount_value) / qty if qty else 0.0),
                "rate": rate,
                "discount_percentage": flt(discount_pct),
            })
        payment_amount = flt(
            getattr(self, "net_value", None)
            or 0
        )
        if not payment_amount:
            pass
        si.insert()
        if not payment_amount:
            payment_amount = flt(si.grand_total)
        if getattr(self, "payment_method", None):
            si.append("payments", {
                "mode_of_payment": self.payment_method,
                "amount": payment_amount,
            })

        try:
            si.submit()
            self.status = "SUCCESSFUL"
            self.rejected_reason = ""

            frappe.db.set_value(
                self.doctype,
                self.name,
                {
                    "status": self.status,
                    "rejected_reason": self.rejected_reason,
                },
                update_modified=False,
            )
            for row in (self.items or []):
                if getattr(row, "pos_data_ckeck", None):
                    try:
                        frappe.db.set_value(
                            "POS Data Check",
                            row.pos_data_ckeck,
                            "status",
                            "SUCCESSFUL",
                        )
                    except Exception:
                        frappe.log_error(
                            f"Failed to set POS Data Check {row.pos_data_ckeck} to SUCCESSFUL"
                        )
        except Exception as e:
            msg = _("Failed to submit Sales Invoice: {0}").format(str(e))
            self._reject_and_raise(msg)
            
    def check_duplicate_invoice(self):
        inv_pk = getattr(self, "invoice_pk", None)
        if not inv_pk:
            return
        others = frappe.get_all(
			"POS Data Import",
			filters=[
				["invoice_pk", "=", inv_pk],
				["name", "!=", self.name or ""],
			],
			fields=["name", "docstatus", "status"],
			limit_page_length=50,
		)
        if not others:
            return
        for e in others:
            docstatus = e.get("docstatus")
            other_status = (e.get("status") or "").upper()
            if docstatus == 1 and other_status == "SUCCESSFUL":
                msg = _("DUPLICATE Invoice from {0}").format(e.get("name"))
                self._reject_and_raise(msg)
            if docstatus == 0:
                try:
                    previous = frappe.get_doc("POS Data Import", e.get("name"))
                    previous.db_set("status", "DUPLICATE")
                    previous.db_set(
						"rejected_reason",
						_("New Import {0} has been submitted for this Invoice").format(self.name),
					)
                except Exception:
                    msg = _("DUPLICATE Invoice from {0}").format(e.get("name"))
                    self._reject_and_raise(msg)
