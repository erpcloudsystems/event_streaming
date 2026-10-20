frappe.listview_settings["Event Sync Log"] = {
	get_indicator: function (doc) {
		var colors = {
			Failed: "red",
			Synced: "green",
		};
		return [__(doc.status), colors[doc.status], "status,=," + doc.status];
	},

	onload: function (listview) {
		listview.page.add_actions_menu_item(__("Resync"), function () {
			const names = listview.get_checked_items(true);
			frappe.call({
				method: "event_streaming.event_streaming.doctype.event_producer.event_producer.bulk_resync",
				args: { names: names },
				freeze: true,
				callback: function (r) {
					if (r.message) {
						frappe.show_alert({
							message: __("Resyncing {0} failed logs in the background", [r.message]),
							indicator: "blue",
						});
					} else {
						frappe.msgprint(__("None of the selected logs has failed."));
					}
					listview.clear_checked_items();
				},
			});
		});

		frappe.realtime.off("event_streaming_bulk_resync_done");
		frappe.realtime.on("event_streaming_bulk_resync_done", function (counts) {
			frappe.msgprint({
				title: __("Bulk Resync finished"),
				message: __("{0} synced, {1} still failed (see the Error Log for why).", [
					counts.Synced,
					counts.Failed,
				]),
				indicator: counts.Failed ? "orange" : "green",
			});
			listview.refresh();
		});
	},
};
