'use strict';

const async = require('async');

exports.run = function (req, res, cb) {
	const tasks = [];
	const data = {'global': res.globalData};
	const log = req.log;
	const productLib = req.productLib;

	if (! log) {
		const LUtils = require('larvitutils');
		const tmpLUtils = new LUtils();

		log = new tmpLUtils.Log();
	}

	if (! productLib) return cb(new Error('Required object "productLib" in req is missing'), req, res, {});

	// Make sure the user have the correct rights
	// This is set in larvitadmingui controllerGlobal
	if (! res.adminRights) {
		cb(new Error('Invalid rights'), req, res, {});

		return;
	}

	data.global.menuControllerName = 'products';

	if (data.global.formFields.import !== undefined && req.formFiles !== undefined && req.formFiles.file !== undefined && req.formFiles.file.size) {
		tasks.push(function (cb) {
			productLib.importer.fromFile(req.formFiles.file.path, {'replaceByField': req.formFields.replaceByField});

			log.verbose('larvitproduct: ./controllers/products/import.js - Import file uploaded with size: ' + req.formFiles.file.size);

			data.global.messages = ['File upload complete, importing.'];

			cb();
		});
	}

	async.series(tasks, function (err) {
		cb(err, req, res, data);
	});
};
