'use strict';

const Product = require('./product.js');
const DataWriter = require('./dataWriter.js');
const Helpers = require('./helpers.js');
const Importer = require('./importer.js');
const LUtils = require('larvitutils');
const async = require('async');

/**
 * 
 * @param   {obj}  options - {log, mode, intercom, esIndexName, elasticsearch, amsync}
 * @param   {func} cb      - callback 
 */
function ProductLib(options, cb) {
	const that = this;
	const tasks = [];

	that.options = options || {};

	if (! cb) cb = function () {};

	for (const key of Object.keys(options)) {
		that[key] = options[key];
	}

	if (! that.log) {
		const tmpLUtils = new LUtils();

		that.log = new tmpLUtils.Log();
	}

	tasks.push(function (cb) {
		that.dataWriter = new DataWriter({
			'log': that.log,
			'mode': that.mode,
			'intercom': that.intercom,
			'esIndexName': that.esIndexName,
			'elasticsearch': that.elasticsearch,
			'amsync': that.amsync
		}, cb);
	});

	tasks.push(function (cb) {
		that.helpers = new Helpers({'log': that.log, 'productLib': that}, cb);
	});

	tasks.push(function (cb) {
		that.importer = new Importer({'log': that.log, 'productLib': that}, cb);
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);

		cb();
	});
}

ProductLib.prototype.ready = function ready(cb) {
	const that = this;

	that.dataWriter.ready(cb);
};

ProductLib.prototype.createProduct = function createProduct(options) {
	const that = this;

	if (typeof options === 'string') {
		options = {'uuid': options};
	}

	options = options || {};
	options.productLib = that;

	return new Product(options);
};

exports.ProductLib = ProductLib;
exports.Product	= Product;
