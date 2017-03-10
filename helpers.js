'use strict';

const	topLogPrefix	= 'larvitproduct: helpers.js - ',
	dataWriter	= require(__dirname + '/dataWriter.js'),
	lUtils	= require('larvitutils'),
	log	= require('winston'),
	_	= require('lodash');

let	es;

function formatEsResult(esResult, cb) {
	const	logPrefix	= topLogPrefix + 'formatEsResult() - ',
		product	= {};

	if (esResult === false) {
		return cb(null, product);
	}

	if ( ! esResult._id) {
		const	err	= new Error('Missing esResult._id, full esResult: ' + JSON.stringify(esResult));
		log.warn(logPrefix + err.message);
		return cb(err);
	}

	product.uuid	= esResult._id;

	if (esResult._source) {
		product.attributes = _.cloneDeep(esResult._source);

		if (product.attributes.created) {
			product.created	= new Date(esResult._source.created);
			delete product.attributes.created;
		}
	}

	cb(null, product);
}

function getAttributeValues(attributeName, cb) {
	const	logPrefix	= topLogPrefix + 'getAttributeValues() - ';

	ready(function (err) {
		const	values	= [];

		if (err) return cb(err);

		es.search({
			'index':	'larvitproduct',
			'type':	'product',
			'body': {
				'aggs': {
					'thingie': {
						'terms': {
							'field': attributeName + '.keyword'
						}
					}
				}
			}
		}, function (err, result) {
			if (err) {
				log.error(logPrefix + err.message);
				return cb(err);
			}

			for (let i = 0; result.aggregations.thingie.buckets[i] !== undefined; i ++) {
				values.push(result.aggregations.thingie.buckets[i].key);
			}

			cb(null, values);
		});
	});
}

function ready(cb) {
	dataWriter.ready(function (err) {
		es	= lUtils.instances.elasticsearch;
		cb(err);
	});
}

exports.attributes	= [];
exports.formatEsResult	= formatEsResult;
exports.getAttributeValues	= getAttributeValues;
