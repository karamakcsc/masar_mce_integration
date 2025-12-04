# Copyright (c) 2025, KCSC and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class MCEIntegrationSetting(Document):
	def validate(self):
		self.insert_job = 0
  
	@frappe.whitelist()
	def read_file(self):
		from masar_mce_integration.tasks import check_active_paths
		check_active_paths()