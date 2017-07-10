'use strict';

exports.dataWriter	= require('./dataWriter.js');
exports.helpers	= require('./helpers.js');
exports.importer	= require('./importer.js');
exports.Product	= require('./product.js');

exports.ready	= exports.dataWriter.ready;
exports.dataWriter.esIndexName	= 'larvitproduct';
