"""
ERPNext refuses to save a transaction against an order that is Closed or On
Hold: a Sales Invoice / Delivery Note against such a Sales Order, a Purchase
Invoice / Purchase Receipt against such a Purchase Order, a Purchase Order
against such a Material Request - and a Delivery Note (or a stock-updating
Sales Invoice) is refused once more at submit, by update_reserved_qty().

For a document synced in by event_streaming that check is wrong. The producer
already accepted the document while the order was still open; the consumer
only sees the order as Closed because it got there first - typically fetched
by sync_dependencies(), which copies a missing linked document in its
*current* state from the producer, i.e. after it was closed. The sync then
failed with "Sales Order ... is Closed" for a document that is valid where it
was made.

These patches skip only those checks, and only while event_streaming is
applying an update (frappe.flags.in_event_streaming_sync, set in
event_producer.sync()). Documents made on this site are checked exactly as
upstream. Imported - and so applied - by event_producer.sync() before it
applies any update; if ERPNext is not installed nothing is patched.

Pinned to the current ERPNext signatures of
SellingController.check_sales_order_on_hold_or_close(self, ref_fieldname),
SellingController.update_reserved_qty(self) and the
check_on_hold_or_closed_status(self) methods of Purchase Order, Purchase
Invoice and Purchase Receipt - an ERPNext upgrade that changes them needs
this file re-checked.
"""

import functools

import frappe
from frappe import _


def in_event_streaming_sync():
	return bool(frappe.flags.in_event_streaming_sync)


def _skip_during_sync(method):
	@functools.wraps(method)
	def wrapper(self, *args, **kwargs):
		if in_event_streaming_sync():
			return
		return method(self, *args, **kwargs)

	wrapper._skipped_during_event_streaming_sync = True
	return wrapper


def _update_reserved_qty(self):
	# Upstream SellingController.update_reserved_qty() - a Closed Sales Order
	# is not refused while syncing; a Cancelled one still is.
	so_map = {}
	for d in self.get("items"):
		if d.so_detail:
			if self.doctype == "Delivery Note" and d.against_sales_order:
				so_map.setdefault(d.against_sales_order, []).append(d.so_detail)
			elif self.doctype == "Sales Invoice" and d.sales_order and self.update_stock:
				so_map.setdefault(d.sales_order, []).append(d.so_detail)

	for so, so_item_rows in so_map.items():
		if so and so_item_rows:
			sales_order = frappe.get_doc("Sales Order", so)

			closed = sales_order.status == "Closed" and not self.is_return and not in_event_streaming_sync()
			if closed or sales_order.status in ["Cancelled"]:
				frappe.throw(
					_("{0} {1} is cancelled or closed").format(_("Sales Order"), so),
					frappe.InvalidStatusError,
				)

			sales_order.update_reserved_qty(so_item_rows)


def apply():
	try:
		from erpnext.accounts.doctype.purchase_invoice.purchase_invoice import PurchaseInvoice
		from erpnext.buying.doctype.purchase_order.purchase_order import PurchaseOrder
		from erpnext.controllers.selling_controller import SellingController
		from erpnext.stock.doctype.purchase_receipt.purchase_receipt import PurchaseReceipt
	except ImportError:
		return

	for cls, name in (
		(SellingController, "check_sales_order_on_hold_or_close"),
		(PurchaseOrder, "check_on_hold_or_closed_status"),
		(PurchaseInvoice, "check_on_hold_or_closed_status"),
		(PurchaseReceipt, "check_on_hold_or_closed_status"),
	):
		method = cls.__dict__[name]
		if not getattr(method, "_skipped_during_event_streaming_sync", False):
			setattr(cls, name, _skip_during_sync(method))

	SellingController.update_reserved_qty = _update_reserved_qty


apply()
