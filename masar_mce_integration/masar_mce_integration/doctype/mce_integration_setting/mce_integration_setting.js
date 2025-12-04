// Copyright (c) 2025, KCSC and contributors
// For license information, please see license.txt

frappe.ui.form.on("MCE Integration Setting", {
	read_file(frm) {
        frappe.call({
            doc:frm.doc , 
            method :"read_file"
        })
	},
});
