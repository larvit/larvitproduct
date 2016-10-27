'use strict';

const	productLib	= require(__dirname + '/../../index.js'),
	async	= require('async'),
	log	= require('winston');

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
		data.product = new productLib.Product(data.global.urlParsed.query.uuid);

		data.product.loadFromDb(cb);
	});

	if (data.global.formFields.save !== undefined) {
		tasks.push(function(cb) {
			data.product.attributes	= {};

			// Handle product attributes
			for (let i = 0; data.global.formFields.attributeName[i] !== undefined; i ++) {
				const	attributeName	= data.global.formFields.attributeName[i],
					attributeValue	= data.global.formFields.attributeValue[i];

				if (attributeName && attributeValue !== undefined) {
					if (data.product.attributes[attributeName] === undefined) {
						data.product.attributes[attributeName] = [];
					}

					data.product.attributes[attributeName].push(attributeValue);
				}
			}

			data.product.save(function(err) {
				if (err) { cb(err); return; }

				if (data.product.uuid !== undefined && data.global.urlParsed.query.uuid === undefined) {
					log.verbose('larvitproduct: ./controllers/products/edit.js: run() - New product created, redirect to new uuid: "' + data.product.uuid + '"');
					req.session.data.nextCallData	= {'global': {'messages': ['New product created']}};
					res.statusCode	= 302;
					res.setHeader('Location', '/products/edit?uuid=' + data.product.uuid);
				} else {
					data.global.messages = ['Saved'];
				}

				cb();
			});
		});
	}

	if (data.global.formFields.rmProduct !== undefined) {
		tasks.push(function(cb) {
			log.verbose('larvitproduct: ./controllers/products/edit.js: run() - Removing product, uuid: "' + data.product.uuid + '"');
			data.product.rm(function(err) {
				if (err) { cb(err); return; }

				req.session.data.nextCallData	= {'global': {'messages': ['Product removed: ' + data.product.uuid]}};
				res.statusCode	= 302;
				res.setHeader('Location', '/products/list');
				cb();
			});
		});
	}

	async.series(tasks, function(err) {
		cb(err, req, res, data);
	});
};
