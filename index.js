'use strict';

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
