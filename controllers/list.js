'use strict';

const	productLib	= require(__dirname + '/../../index.js'),
	async	= require('async');

exports.run = function(req, res, cb) {
	const	tasks	= [],
		data	= {'global': res.globalData};

	// Make sure the user have the correct rights
	// This is set in larvitadmingui controllerGlobal
	if ( ! res.adminRights) {
		cb(new Error('Invalid rights'), req, res, {});
		return;
	}

	data.global.menuControllerName = 'products';

	tasks.push(function(cb) {
		const	products	= new productLib.Orders();

		products.returnFields = ['name', 'status'];

		if (data.global.urlParsed.query.filterStatus) {
			products.matchAllAttributes = {'status': data.global.urlParsed.query.filterStatus};
		}

		products.get(function(err, result) {
			data.products	= result;
			cb(err);
		});
	});

	tasks.push(function(cb) {
		productLib.helpers.getAttributeValues('status', function(err, result) {
			data.statuses	= result;
			cb(err, result);
		});
	});

	async.series(tasks, function(err) {
		cb(err, req, res, data);
	});
};
