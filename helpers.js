'use strict';

const	topLogPrefix	= 'larvitproduct: helpers.js - ',
	dataWriter	= require(__dirname + '/dataWriter.js'),
	request	= require('request'),
	lUtils	= require('larvitutils'),
	log	= require('winston'),
	_	= require('lodash');

let	intercom,
	esUrl,
	es;

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
	const	searchBody	= {'size':0, 'aggs':{'thingie':{'terms':{'field':attributeName}}}, 'query':{'bool':{'must':[]}}},
		logPrefix	= topLogPrefix + 'getAttributeValues() - url: ' + esUrl + '/larvitproduct/product/_search',
		values	= [],
		url	= esUrl + '/larvitproduct/product/_search';

	searchBody.aggs.thingie.terms.size = 2147483647; // http://stackoverflow.com/questions/22927098/show-all-elasticsearch-aggregation-results-buckets-and-not-just-10

	ready(function (err) {
		if (err) return cb(err);

		require({'url': url, 'body': searchBody, 'json': true}, function (err, response, body) {
			if (err) {
				log.error(logPrefix + err.message);
				return cb(err);
			}

			for (let i = 0; body.aggregations.thingie.buckets[i] !== undefined; i ++) {
				values.push(body.aggregations.thingie.buckets[i].key);
			}

			cb(null, values, body.aggregations.thingie.buckets);
		});
	});
}

function getKeywords(cb) {

	ready(function (err) {
		const	logPrefix	= topLogPrefix + 'getKeywords() - url: "' + esUrl + '/larvitproduct/_mapping/product"',
			url	= esUrl + '/larvitproduct/_mapping/product';

		if (err) { return cb(err); }

		request({'url': url, 'json': true}, function (err, response, body) {
			const	keywords	= [];

			if (err) {
				log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);
				return cb(err);
			}

			if (response.statusCode !== 200) {
				const	err	= new Error('non-200 statusCode: ' + response.statusCode);
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			for (const fieldName of Object.keys(body.larvitproduct.mappings.product.properties)) {
				const	fieldProps	= body.larvitproduct.mappings.product.properties[fieldName];

				if (fieldProps.type === 'keyword') {
					keywords.push(fieldName);
				} else if (fieldProps.fields && fieldProps.fields.keyword && fieldProps.fields.keyword.type === 'keyword') {
					keywords.push(fieldName + '.keyword');
				}
			}

			cb(null, keywords);
		});
	});
}

function ready(cb) {
	dataWriter.ready(function (err) {
		intercom	= lUtils.instances.intercom;
		es	= lUtils.instances.elasticsearch;
		esUrl	= 'http://' + es.transport._config.host;
		cb(err);
	});
}

function updateByQuery(updateBody, cb) {
	const	options	= {'exchange': dataWriter.exchangeName},
		message	= {};

	message.action	= 'updateByQuery';
	message.params	= {};

	message.params.updateBody	= updateBody;

	intercom.send(message, options, function (err, msgUuid) {
		if (err) return cb(err);

		dataWriter.emitter.once(msgUuid, cb);
	});
}

exports.attributes	= [];
exports.formatEsResult	= formatEsResult;
exports.getAttributeValues	= getAttributeValues;
exports.getKeywords	= getKeywords;
exports.updateByQuery	= updateByQuery;
