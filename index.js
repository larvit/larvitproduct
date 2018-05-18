'use strict';

const dataWriter = require('./dataWriter.js');

exports.dataWriter	= dataWriter;
exports.helpers	= require('./helpers.js');
exports.importer	= require('./importer.js');
exports.Product	= require('./product.js');
exports.ready	= exports.dataWriter.ready;
exports.dataWriter.esIndexName	= 'larvitproduct';
