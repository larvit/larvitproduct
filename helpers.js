'use strict';

const topLogPrefix = 'larvitproduct: helpers.js - ';
const Product = require(__dirname + '/product.js');
const request = require('requestretry');
const async	= require('async');
const LUtils = require('larvitutils');
const _	= require('lodash');

/**
 * 
 * @param   {obj}  options - {log, productLib}
 * @param   {func} cb      - callback
 * @returns {*}            - on error, return cb(err)
 */
function Helpers(options, cb) {
	const that = this;

	for (const key of Object.keys(options)) {
		that[key] = options[key];
	}

	if (! that.productLib) {
		return cb(new Error('Required option "productLib" is missing'));
	}

	that.dataWriter = that.productLib.dataWriter;
	that.intercom = that.productLib.dataWriter.intercom;
	that.es	= that.productLib.dataWriter.elasticsearch;
	that.esUrl = 'http://' + that.es.transport._config.host;
	that.attributes	= [];

	if (! that.log) {
		const tmpLUtils = new LUtils();

		that.log = new tmpLUtils.Log();
	}

	cb();
}

Helpers.prototype.deleteByQuery = function deleteByQuery(queryBody, cb) {
	const that = this;
	const logPrefix	= topLogPrefix + 'deleteByQuery() - ';
	const uuids = [];
	const tasks = [];

	tasks.push(function (cb) {
		that.ready(cb);
	});

	// Get products to delete
	tasks.push(function (cb) {
		const reqOptions = {};

		reqOptions.url = that.esUrl + '/' + that.dataWriter.esIndexName + '/product/_search';
		reqOptions.json	= true;
		reqOptions.body	= queryBody;
		reqOptions.body.size = 10000;

		request(reqOptions, function (err, response, body) {
			if (err) {
				that.log.warn(logPrefix + 'Could not get products to delete, err: ' + err.message);

				return cb(err);
			}

			if (response.statusCode !== 200) {
				const err	= new Error('non-200 response code: ' + response.statusCode + ', query: ' + JSON.stringify(reqOptions.body) + ', response body: ' + JSON.stringify(body));

				that.log.warn(logPrefix + err.message);

				return cb(err);
			}

			for (let i = 0; body.hits.hits[i] !== undefined; i ++) {
				uuids.push(body.hits.hits[i]._id);
			}

			cb();
		});
	});

	// Remove products
	tasks.push(function (cb) {
		const message = {};

		message.action = 'rmProducts';
		message.params = {'uuids': uuids};

		that.intercom.send(message, {'exchange': 'larvitproduct'}, function (err) {
			if (err) {
				that.log.error(logPrefix + 'Could not send to queue, err: ' + err.message);
			}

			cb(err);
		});
	});

	// Refreesh ES index
	tasks.push(function (cb) {
		request.post(that.esUrl + '/' + that.dataWriter.esIndexName + '/_refresh', cb);
	});

	// Run this function again if any uuids was encountered
	tasks.push(function (cb) {
		if (uuids.length) {
			that.deleteByQuery(queryBody, cb);
		} else {
			cb();
		}
	});

	async.series(tasks, cb);
};

Helpers.prototype.formatEsResult = function formatEsResult(esResult, cb) {
	const that = this;
	const logPrefix	= topLogPrefix + 'formatEsResult() - ';
	const product = {};

	if (esResult === false) {
		return cb(null, product);
	}

	if (! esResult._id) {
		const err = new Error('Missing esResult._id, full esResult: ' + JSON.stringify(esResult));

		that.log.warn(logPrefix + err.message);

		return cb(err);
	}

	product.uuid = esResult._id;

	if (esResult._source) {
		product.attributes = _.cloneDeep(esResult._source);

		if (product.attributes.created) {
			product.created	= new Date(esResult._source.created);
			delete product.attributes.created;
		}
	}

	cb(null, product);
};

Helpers.prototype.getAttributeValues = function getAttributeValues(attributeName, options, cb) {
	const that = this;
	const values = [];
	const tasks = [];

	let	valueList;
	let buckets;	// Regarding size, see: http://stackoverflow.com/questions/22927098/show-all-elasticsearch-aggregation-results-buckets-and-not-just-10
	let searchBody = {
		'size': 0,
		'aggs': {
			'thingie': {
				'terms': {
					'field': attributeName,
					'size':  2147483647
				}
			}
		},
		'query': {
			'bool': {
				'must': []
			}
		}
	};

	if (typeof options === 'function') {
		cb	= options;
		options	= {};
	}

	if (options.query) {
		searchBody.query = options.query;
	}

	if (options.searchBody) {
		searchBody	= options.searchBody;
	}

	tasks.push(function (cb) {
		that.ready(cb);
	});

	tasks.push(function (cb) {
		const logPrefix	= topLogPrefix + 'getAttributeValues() - url: ' + that.esUrl + '/' + that.dataWriter.esIndexName + '/product/_search';
		const url = that.esUrl + '/' + that.dataWriter.esIndexName + '/product/_search';

		request({'url': url, 'body': searchBody, 'json': true}, function (err, response, body) {
			if (err) {
				that.log.error(logPrefix + err.message);

				return cb(err);
			}

			if (response.statusCode === 400 && body.error && body.error.root_cause && body.error.root_cause.reason === 'Fielddata is disabled on text fields by default. Set fielddata=true on [trams] in order to load fielddata in memory by uninverting the inverted index. Note that this can however use significant memory.') {
				const err = new Error('Can not get attribute values on non-key fields. Have you tried appending .keyword to your field name?');

				that.log.warn(logPrefix + err.message);

				return cb(err);
			} else if (response.statusCode !== 200 || ! body || ! body.aggregations || ! body.aggregations.thingie || ! body.aggregations.thingie.buckets) {
				const err = new Error('Invalid response from Elasticsearch. statusCode: ' + response.statusCode + ' response body: "' + JSON.stringify(body) + '" search body: "' + JSON.stringify(searchBody) + '"');

				that.log.warn(logPrefix + err.message);

				return cb(err);
			}

			for (let i = 0; body.aggregations.thingie.buckets[i] !== undefined; i ++) {
				values.push(body.aggregations.thingie.buckets[i].key);
			}

			valueList = values;
			buckets	= body.aggregations.thingie.buckets;
			cb();
		});
	});

	async.series(tasks, function (err) {
		cb(err, valueList, buckets);
	});
};

Helpers.prototype.getBooleans = function getBooleans(cb) {
	const that = this;
	const booleans = [];
	const tasks = [];

	tasks.push(function (cb) {
		that.ready(cb);
	});

	tasks.push(function (cb) {
		const logPrefix	= topLogPrefix + 'getBooleans() - url: "' + that.esUrl + '/' + that.dataWriter.esIndexName + '/_mapping/product"';
		const url = that.esUrl + '/' + that.dataWriter.esIndexName + '/_mapping/product';

		request({'url': url, 'json': true}, function (err, response, body) {
			if (err) {
				that.log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);

				return cb(err);
			}

			if (response.statusCode !== 200) {
				const err = new Error('non-200 statusCode: ' + response.statusCode);

				that.log.warn(logPrefix + err.message);

				return cb(err);
			}

			for (const fieldName of Object.keys(body[that.dataWriter.esIndexName].mappings.product.properties)) {
				const fieldProps = body[that.dataWriter.esIndexName].mappings.product.properties[fieldName];

				if (fieldProps.type === 'boolean') {
					booleans.push(fieldName);
				}
			}

			cb();
		});
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);
		cb(null, booleans);
	});
};

Helpers.prototype.getDates = function getDates(cb) {
	const that = this;
	const dates	= [];
	const tasks = [];

	tasks.push(function (cb) {
		that.ready(cb);
	});

	tasks.push(function (cb) {
		const logPrefix	= topLogPrefix + 'getDates() - url: "' + that.esUrl + '/' + that.dataWriter.esIndexName + '/_mapping/product"';
		const url = that.esUrl + '/' + that.dataWriter.esIndexName + '/_mapping/product';

		request({'url': url, 'json': true}, function (err, response, body) {
			if (err) {
				that.log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);

				return cb(err);
			}

			if (response.statusCode !== 200) {
				const err = new Error('non-200 statusCode: ' + response.statusCode);

				that.log.warn(logPrefix + err.message);

				return cb(err);
			}

			for (const fieldName of Object.keys(body[that.dataWriter.esIndexName].mappings.product.properties)) {
				const fieldProps = body[that.dataWriter.esIndexName].mappings.product.properties[fieldName];

				if (fieldProps.type === 'date') {
					dates.push(fieldName);
				}
			}

			cb();
		});
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);
		cb(null, dates);
	});
};

Helpers.prototype.getKeywords = function getKeywords(cb) {
	const that = this;
	const keywords = [];
	const tasks	= [];

	tasks.push(function (cb) {
		that.ready(cb);
	});

	tasks.push(function (cb) {
		const logPrefix	= topLogPrefix + 'getKeywords() - url: "' + that.esUrl + '/' + that.dataWriter.esIndexName + '/_mapping/product"';
		const url = that.esUrl + '/' + that.dataWriter.esIndexName + '/_mapping/product';

		request({'url': url, 'json': true}, function (err, response, body) {
			if (err) {
				that.log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);

				return cb(err);
			}

			if (response.statusCode !== 200) {
				const err	= new Error('non-200 statusCode: ' + response.statusCode);

				that.log.warn(logPrefix + err.message);

				return cb(err);
			}

			if (body[that.dataWriter.esIndexName] === undefined) {
				const err = new Error('Could not get mappings, since index did not exist in body. Full body: ' + JSON.stringify(body));

				that.log.warn(logPrefix + err.message);

				return cb(err);
			}

			for (const fieldName of Object.keys(body[that.dataWriter.esIndexName].mappings.product.properties)) {
				const fieldProps = body[that.dataWriter.esIndexName].mappings.product.properties[fieldName];

				if (fieldProps.type === 'keyword') {
					keywords.push(fieldName);
				} else if (fieldProps.fields && fieldProps.fields.keyword && fieldProps.fields.keyword.type === 'keyword') {
					keywords.push(fieldName + '.keyword');
				}
			}

			cb();
		});
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);
		cb(null, keywords);
	});
};

Helpers.prototype.getMappedFieldNames = function getMappedFieldNames(cb) {
	const that = this;
	const tasks = [];

	let names;

	tasks.push(function (cb) {
		that.ready(cb);
	});

	tasks.push(function (cb) {
		const logPrefix = topLogPrefix + 'getMappedFieldNames() - url: "' + that.esUrl + '/' + that.dataWriter.esIndexName + '/_mapping/product"';
		const url = that.esUrl + '/' + that.dataWriter.esIndexName + '/_mapping/product';

		request({'url': url, 'json': true}, function (err, response, body) {
			if (err) {
				that.log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);

				return cb(err);
			}

			if (response.statusCode !== 200) {
				const err = new Error('non-200 statusCode: ' + response.statusCode);

				that.log.warn(logPrefix + err.message);

				return cb(err);
			}

			if (body[that.dataWriter.esIndexName] === undefined) {
				const err = new Error('Could not get mappings, since index did not exist in body. Full body: ' + JSON.stringify(body));

				that.log.warn(logPrefix + err.message);

				return cb(err);
			}

			names = Object.keys(body[that.dataWriter.esIndexName].mappings.product.properties);
			cb();
		});
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);
		cb(null, names);
	});
};

Helpers.prototype.ready = function ready(cb) {
	const that = this;

	that.dataWriter.ready(function (err) {
		cb(err);
	});
};

Helpers.prototype.updateByQuery = function updateByQuery(queryBody, updates, cb) {
	const that = this;
	const logPrefix = topLogPrefix + 'updateByQuery() - ';
	const tasks = [];

	tasks.push(function (cb) {
		that.ready(cb);
	});

	tasks.push(function (cb) {
		let scrollId = null;
		let done = false;

		async.whilst(function () { return ! done; }, function (cb) {
			const tasks = [];
			const uuids	= [];

			// Get scroll ID
			tasks.push(function (cb) {
				const reqOptions = {};

				reqOptions.url	= that.esUrl + '/' + that.dataWriter.esIndexName + '/product/_search?scroll=60m';
				reqOptions.json	= true;
				reqOptions.body	= queryBody;
				reqOptions.body.size	= 1000;

				if (scrollId !== null) {
					reqOptions.url = that.esUrl + '/_search/scroll';
					reqOptions.body = {
						'scroll':    '60m',
						'scroll_id': scrollId
					};
				}

				request.post(reqOptions, function (err, response, body) {
					if (err) {
						that.log.warn(logPrefix + 'Could not get products to update, err: ' + err.message);

						return cb(err);
					}

					if (response.statusCode !== 200) {
						that.log.warn(logPrefix + 'Non 200 response from ElasticSeardch');

						return cb(err);
					}

					if (body.hits.hits.length === 0) {
						done = true;

						return cb();
					}

					for (let i = 0; body.hits.hits[i] !== undefined; i ++) {
						const hit = body.hits.hits[i];

						uuids.push(hit._id);
					}

					scrollId = body._scroll_id;
					cb();
				});
			});

			// Run updates
			tasks.push(function (cb) {
				const tasks	= [];

				for (let i = 0; uuids[i] !== undefined; i ++) {
					const uuid = uuids[i];

					tasks.push(function (cb) {
						const product = new Product({
							'uuid':       uuid,
							'log':        that.log,
							'productLib': that.productLib
						});

						product.loadFromDb(function (err) {
							if (err) return cb(err);

							if (typeof updates === 'function') {
								updates(product.attributes, function (err) {
									if (err) return cb(err);
									product.save(cb);
								});
							} else {
								for (const attributeName of Object.keys(updates)) {
									product.attributes[attributeName] = updates[attributeName];
								}
								product.save(cb);
							}
						});
					});
				}

				async.parallelLimit(tasks, 100, cb);
			});

			async.series(tasks, cb);
		}, function (err) {
			if (err) return cb(err);

			if (scrollId === null) return cb();

			request.delete(that.esUrl + '/_search/scroll/' + scrollId, {}, function (err, response, body) {
				if (err) {
					that.log.warn(logPrefix + 'Could not get products to update, err: ' + err.message);

					return cb(err);
				}

				if (response.statusCode !== 200) {
					const err = new Error('non-200 response code: ' + response.statusCode + ', response body: ' + JSON.stringify(body));

					that.log.warn(logPrefix + err.message);

					return cb(err);
				}

				cb();
			});
		});
	});

	async.series(tasks, cb);
};

exports = module.exports = Helpers;
