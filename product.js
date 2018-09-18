'use strict';

const topLogPrefix	= 'larvitproduct: product.js: ';
const uuidLib	= require('uuid');
const async	= require('async');

/**
 * 
 * @param {obj} options - {productLib, created, attributes}
 */
function Product(options) {
	const that = this;
	const logPrefix	= topLogPrefix + 'Product() - ';

	options = options || {};

	if (! options.productLib) throw new Error('Required option "productLib" is missing');
	that.productLib = options.productLib;

	if (! that.productLib.log) {
		const LUtils = require('larvitutils');
		const tmpLUtils = new LUtils();

		that.productLib.log = new tmpLUtils.Log();
	}
	that.log = that.productLib.log;

	if (options.uuid !== undefined) {
		that.uuid = options.uuid;
	} else {
		that.uuid = uuidLib.v1();
		that.log.verbose(logPrefix + 'New Product - Creating Product with uuid: ' + that.uuid);
	}

	that.dataWriter = that.productLib.dataWriter;
	that.intercom = that.productLib.dataWriter.intercom;
	that.es = that.productLib.dataWriter.elasticsearch;
	that.helpers = that.productLib.helpers;

	that.created = options.created;
	that.attributes	= options.attributes;

	if (that.attributes	=== undefined) { that.attributes = {}; }
	if (that.created	=== undefined) { that.created = new Date(); }
}

Product.prototype.loadFromDb = function (cb) {
	const that = this;
	const logPrefix = topLogPrefix + 'Product.prototype.loadFromDb() - uuid: ' + that.uuid + ' - ';
	const tasks = [];

	let	esResult;

	tasks.push(function (cb) {
		that.dataWriter.ready(cb);
	});

	// Get basic product info
	tasks.push(function (cb) {
		that.es.get({
			'index': that.dataWriter.esIndexName,
			'type':	 'product',
			'id':    that.uuid
		}, function (err, result) {
			if (err && err.status === 404) {
				that.log.debug(logPrefix + 'No product found in database');
				esResult = false;

				return cb();
			} else if (err) {
				that.log.error(logPrefix + 'that.es.get() - err: ' + err.message);

				return cb(err);
			}

			esResult = result;
			cb();
		});
	});

	tasks.push(function (cb) {
		that.helpers.formatEsResult(esResult, function (err, result) {
			if (err) return cb(err);

			if (result && result.uuid) {
				that.uuid	= result.uuid;
			}

			if (result && result.created) {
				that.created	= result.created;
			}

			if (result && result.attributes) {
				that.attributes	= result.attributes;
			}

			if (result && result.images) {
				that.images	= result.images;
			}

			if (result && result.files) {
				that.files = result.files;
			}

			cb();
		});
	});

	async.series(tasks, cb);
};

Product.prototype.rm = function (cb) {
	const that = this;
	const options = {'exchange': that.dataWriter.exchangeName};
	const message = {};

	message.action	= 'rmProducts';
	message.params	= {};

	message.params.uuids	= [that.uuid];

	that.intercom.send(message, options, function (err, msgUuid) {
		if (err) return cb(err);

		that.dataWriter.emitter.once(msgUuid, cb);
	});
};

// Saving the product object to the database.
Product.prototype.save = function (cb) {
	const tasks = [];
	const that = this;

	// Await database readiness
	tasks.push(function (cb) {
		that.dataWriter.ready(cb);
	});

	tasks.push(function (cb) {
		const options = {'exchange': that.dataWriter.exchangeName};
		const message = {};

		message.action	= 'writeProduct';
		message.params	= {};

		message.params.uuid	= that.uuid;
		message.params.created	= that.created;
		message.params.attributes	= that.attributes;

		that.intercom.send(message, options, function (err, msgUuid) {
			if (err) return cb(err);

			that.dataWriter.emitter.once(msgUuid, cb);
		});
	});

	tasks.push(function (cb) {
		that.loadFromDb(cb);
	});

	async.series(tasks, cb);
};

exports = module.exports = Product;
