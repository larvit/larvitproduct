'use strict';

const	dataWriter	= require('./models/dataWriter.js'),
	importer	= require('./models/importer.js'),
	Products	= require('./models/products.js'),
	helpers	= require('./models/helpers.js'),
	Product	= require('./models/product.js'),
	log	= require('winston');

function ProductLib(options, cb) {
	const	that	= this;

	if (typeof options === 'function') {
		cb	= options;
		options	= {};
	}

	if (options === undefined) {
		options = {};
	}

	if (cb === undefined) {
		cb = function() {};
	}

	if (options.mode === undefined) {
		options.mode = 'dataSource';
	}

	if (options.mode !== 'dataSource' && options.mode !== 'dataSlave') {
		const	err	= new Error('Invalid options.mode provided. Must be "dataSource" or "dataSlave", but received: "' + options.mode + '"');
		log.error('larvitproduct: index.js - ' + err.message);
		cb(err);
		return;
	}

	if (options.intercom === undefined) {
		const err = new Error('options.intercom is required, but missing');
		log.error('larvitproduct: index.js - ' + err.message);
		cb(err);
		return;
	}

	that
}

exports = module.exports = ProductLib;

const	intercom	= require('larvitutils').instances.intercom,
	log	= require('winston');

// We are strictly in need of the intercom!
if ( ! (intercom instanceof require('larvitamintercom'))) {
	const	err	= new Error('larvitutils.instances.intercom is not an instance of Intercom!');
	log.error('larvitproduct: index.js - ' + err.message);
	throw err;
}

exports.dataWriter	= require('./dataWriter.js');
exports.helpers	= require('./helpers.js');
exports.importer	= require('./importer.js');
exports.Product	= require('./product.js');
exports.Products	= require('./products.js');
