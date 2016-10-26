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

	data.global.menuControllerName	= 'products';
	data.pagination	= {};
	data.pagination.urlParsed	= data.global.urlParsed;
	data.pagination.elementsPerPage	= 100;

	tasks.push(function(cb) {
		const	products	= new productLib.Products();

		products.returnFields	= ['name', 'status'];
		products.limit	= data.pagination.elementsPerPage;
		products.offset	= parseInt(data.global.urlParsed.query.offset)	|| 0;

		if (isNaN(products.offset) || products.offset < 0) {
			products.offset = 0;
		}

		if (data.global.urlParsed.query.filterStatus) {
			products.matchAllFields = {'status': data.global.urlParsed.query.filterStatus};
		}

		products.get(function(err, result, totalElements) {
			data.products	= result;
			data.pagination.totalElements	= totalElements;
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
