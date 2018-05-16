'use strict';

const	topLogPrefix	= 'larvitproduct: helpers.js - ',
	dataWriter	= require(__dirname + '/dataWriter.js'),
	Product	= require(__dirname + '/product.js'),
	request	= require('request'),
	leftPad	= require('left-pad'),
	fileLib	= require('larvitfiles'),
	imgLib	= require('larvitimages'),
	async	= require('async'),
	log	= require('winston'),
	_	= require('lodash');

let	intercom,
	esUrl,
	es;

function deleteByQuery(queryBody, cb) {
	const	logPrefix	= topLogPrefix + 'deleteByQuery() - ',
		uuids	= [],
		tasks	= [];

	tasks.push(ready);

	// Get products to delete
	tasks.push(function (cb) {
		const	reqOptions	= {};

		reqOptions.url	= esUrl + '/' + dataWriter.esIndexName + '/product/_search';
		reqOptions.json	= true;
		reqOptions.body	= queryBody;
		reqOptions.body.size	= 10000;

		request(reqOptions, function (err, response, body) {
			if (err) {
				log.warn(logPrefix + 'Could not get products to delete, err: ' + err.message);
				return cb(err);
			}

			if (response.statusCode !== 200) {
				const	err	= new Error('non-200 response code: ' + response.statusCode + ', query: ' + JSON.stringify(reqOptions.body) + ', response body: ' + JSON.stringify(body));
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			for (let i = 0; body.hits.hits[i] !== undefined; i ++) {
				const	hit	= body.hits.hits[i];

				uuids.push(hit._id);
			}

			cb();
		});
	});

	// Remove products
	tasks.push(function (cb) {
		const	message	= {};

		message.action	= 'rmProducts';
		message.params	= {'uuids': uuids};

		intercom.send(message, {'exchange': 'larvitproduct'}, function (err) {
			if (err) {
				log.error(logPrefix + 'Could not send to queue, err: ' + err.message);
			}

			cb(err);
		});
	});

	// Refreesh ES index
	tasks.push(function (cb) {
		request.post(esUrl + '/' + dataWriter.esIndexName + '/_refresh', cb);
	});

	// Run this function again if any uuids was encountered
	tasks.push(function (cb) {
		if (uuids.length) {
			deleteByQuery(queryBody, cb);
		} else {
			cb();
		}
	});

	async.series(tasks, cb);
}

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

	getImagesForProducts([product], function (err) {
		if (err) log.warn(logPrefix + err.message);
		getFilesForProducts([product], function (err) {
			cb(err, product);
		});
	});
}

function getAttributeValues(attributeName, options, cb) {
	const	values	= [],
		tasks	= [];

	let	valueList,
		buckets,	// Regarding size, see: http://stackoverflow.com/questions/22927098/show-all-elasticsearch-aggregation-results-buckets-and-not-just-10
		searchBody	= {'size':0, 'aggs':{'thingie':{'terms':{'field':attributeName, 'size': 2147483647}}}, 'query':{'bool':{'must':[]}}};

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

	tasks.push(ready);

	tasks.push(function (cb) {
		const	logPrefix	= topLogPrefix + 'getAttributeValues() - url: ' + esUrl + '/' + dataWriter.esIndexName + '/product/_search',
			url	= esUrl + '/' + dataWriter.esIndexName + '/product/_search';

		request({'url': url, 'body': searchBody, 'json': true}, function (err, response, body) {
			if (err) {
				log.error(logPrefix + err.message);
				return cb(err);
			}

			if (response.statusCode === 400 && body.error && body.error.root_cause && body.error.root_cause.reason === 'Fielddata is disabled on text fields by default. Set fielddata=true on [trams] in order to load fielddata in memory by uninverting the inverted index. Note that this can however use significant memory.') {
				const	err	= new Error('Can not get attribute values on non-key fields. Have you tried appending .keyword to your field name?');
				log.warn(logPrefix + err.message);
				return cb(err);
			} else if (response.statusCode !== 200 || ! body || ! body.aggregations || ! body.aggregations.thingie || ! body.aggregations.thingie.buckets) {
				const	err	= new Error('Invalid response from Elasticsearch. statusCode: ' + response.statusCode + ' response body: "' + JSON.stringify(body) + '" search body: "' + JSON.stringify(searchBody) + '"');
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			for (let i = 0; body.aggregations.thingie.buckets[i] !== undefined; i ++) {
				values.push(body.aggregations.thingie.buckets[i].key);
			}

			valueList	= values;
			buckets	= body.aggregations.thingie.buckets;
			cb();
		});
	});

	async.series(tasks, function (err) {
		cb(err, valueList, buckets);
	});
}

function getBooleans(cb) {
	const	booleans	= [],
		tasks	= [];

	tasks.push(ready);

	tasks.push(function (cb) {
		const	logPrefix	= topLogPrefix + 'getBooleans() - url: "' + esUrl + '/' + dataWriter.esIndexName + '/_mapping/product"',
			url	= esUrl + '/' + dataWriter.esIndexName + '/_mapping/product';

		request({'url': url, 'json': true}, function (err, response, body) {
			if (err) {
				log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);
				return cb(err);
			}

			if (response.statusCode !== 200) {
				const	err	= new Error('non-200 statusCode: ' + response.statusCode);
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			for (const fieldName of Object.keys(body[dataWriter.esIndexName].mappings.product.properties)) {
				const	fieldProps	= body[dataWriter.esIndexName].mappings.product.properties[fieldName];

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
}

function getDates(cb) {
	const	dates	= [],
		tasks	= [];

	tasks.push(ready);

	tasks.push(function (cb) {
		const	logPrefix	= topLogPrefix + 'getDates() - url: "' + esUrl + '/' + dataWriter.esIndexName + '/_mapping/product"',
			url	= esUrl + '/' + dataWriter.esIndexName + '/_mapping/product';

		request({'url': url, 'json': true}, function (err, response, body) {
			if (err) {
				log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);
				return cb(err);
			}

			if (response.statusCode !== 200) {
				const	err	= new Error('non-200 statusCode: ' + response.statusCode);
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			for (const fieldName of Object.keys(body[dataWriter.esIndexName].mappings.product.properties)) {
				const	fieldProps	= body[dataWriter.esIndexName].mappings.product.properties[fieldName];

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
}

function getImagesForProducts(products, cb) {
	const	logPrefix	= topLogPrefix + 'getImagesForProducts() - ',
		slugs	= [];

	if ( ! Array.isArray(products)) {
		return cb(new Error('Inavlid input, is not an array'));
	}

	for (let i = 0; products[i] !== undefined; i ++) {
		const	product	= products[i];

		if ( ! product.uuid) {
			const	err	= new Error('Invalid input, product have no uuid');
			log.warn(logPrefix + err.message);
			return cb(err);
		}

		for (let i = 1; i !== 25; i ++) {
			slugs.push('product_' + product.uuid + '_' + leftPad(i, 2, '0') + '.jpg');
			slugs.push('product_' + product.uuid + '_' + leftPad(i, 2, '0') + '.png');
			slugs.push('product_' + product.uuid + '_' + leftPad(i, 2, '0') + '.gif');
		}
	}

	imgLib.getImages({'slugs': slugs, 'limit': 10000}, function (err, result) {
		if (err) return cb(err);

		for (let i = 0; products[i] !== undefined; i ++) {
			const	product	= products[i];

			product.images	= [];

			for (const imgUuid of Object.keys(result)) {
				if (product.uuid === result[imgUuid].slug.substring(8, 44)) {
					product.images.push(result[imgUuid]);
					delete result[imgUuid];
				}
			}
		}

		cb(null, products);
	});
}

function getFilesForProducts(products, cb) {
	const	files	= new fileLib.Files();

	if ( ! Array.isArray(products)) {
		return cb(new Error('Inavlid input, is not an array'));
	}

	if (products.length === 0) return cb(null, products);

	files.filter.metadata	= { 'productUuid': [] };
	files.filter.operator	= 'or';

	for (const product of products) {
		files.filter.metadata.productUuid.push(product.uuid);
	}

	files.get(function (err, fileList) {
		if (err) return cb(err);

		if (fileList.length === 0) return cb();

		for (const product of products) {
			product.files = _.filter(fileList, function (f) { return f.metadata.productUuid.indexOf(product.uuid) !== - 1; });
		}

		cb(null, products);
	});
}

function getKeywords(cb) {
	const	keywords	= [],
		tasks	= [];

	tasks.push(ready);

	tasks.push(function (cb) {
		const	logPrefix	= topLogPrefix + 'getKeywords() - url: "' + esUrl + '/' + dataWriter.esIndexName + '/_mapping/product"',
			url	= esUrl + '/' + dataWriter.esIndexName + '/_mapping/product';

		request({'url': url, 'json': true}, function (err, response, body) {
			if (err) {
				log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);
				return cb(err);
			}

			if (response.statusCode !== 200) {
				const	err	= new Error('non-200 statusCode: ' + response.statusCode);
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			if (body[dataWriter.esIndexName] === undefined) {
				const	err	= new Error('Could not get mappings, since index did not exist in body. Full body: ' + JSON.stringify(body));
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			for (const fieldName of Object.keys(body[dataWriter.esIndexName].mappings.product.properties)) {
				const	fieldProps	= body[dataWriter.esIndexName].mappings.product.properties[fieldName];

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
}

function getMappedFieldNames(cb) {
	const	tasks	= [];

	let names;

	tasks.push(ready);

	tasks.push(function (cb) {
		const	logPrefix	= topLogPrefix + 'getMappedFieldNames() - url: "' + esUrl + '/' + dataWriter.esIndexName + '/_mapping/product"',
			url	= esUrl + '/' + dataWriter.esIndexName + '/_mapping/product';

		request({'url': url, 'json': true}, function (err, response, body) {
			if (err) {
				log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);
				return cb(err);
			}

			if (response.statusCode !== 200) {
				const	err	= new Error('non-200 statusCode: ' + response.statusCode);
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			if (body[dataWriter.esIndexName] === undefined) {
				const	err	= new Error('Could not get mappings, since index did not exist in body. Full body: ' + JSON.stringify(body));
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			names = Object.keys(body[dataWriter.esIndexName].mappings.product.properties);
			cb();
		});
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);
		cb(null, names);
	});	
}

function ready(cb) {
	dataWriter.ready(function (err) {
		intercom	= dataWriter.intercom;
		es	= dataWriter.elasticsearch;
		esUrl	= 'http://' + es.transport._config.host;
		cb(err);
	});
}

function updateByQuery(queryBody, updates, cb) {
	const	logPrefix	= topLogPrefix + 'updateByQuery() - ',
		tasks	= [];

	tasks.push(ready);

	tasks.push(function (cb) {
		let scrollId	= null,
			done	= false;

		async.whilst(function () { return ! done; }, function (cb) {
			const	tasks	= [],
				uuids	= [];

			// Get scroll ID
			tasks.push(function (cb) {
				const	reqOptions	= {};

				reqOptions.url	= esUrl + '/' + dataWriter.esIndexName + '/product/_search?scroll=60m';
				reqOptions.json	= true;
				reqOptions.body	= queryBody;
				reqOptions.body.size	= 1000;
				//reqOptions.body.stored_fields	= []; // Only get ID

				if (scrollId !== null) {
					reqOptions.url = esUrl + '/_search/scroll';
					reqOptions.body = {
						'scroll':	'60m',
						'scroll_id':	scrollId
					};
				}

				request.post(reqOptions, function (err, response, body) {
					if (err) {
						log.warn(logPrefix + 'Could not get products to update, err: ' + err.message);
						return cb(err);
					}

					if (response.statusCode !== 200) {
						log.warn(logPrefix + 'Non 200 response from ElasticSeardch');
						return cb(err);
					}

					if (body.hits.hits.length === 0) {
						done = true;
						return cb();
					}

					for (let i = 0; body.hits.hits[i] !== undefined; i ++) {
						const	hit	= body.hits.hits[i];
						uuids.push(hit._id);
					}

					scrollId	= body._scroll_id;
					cb();
				});
			});

			// Run updates
			tasks.push(function (cb) {
				const	tasks	= [];

				for (let i = 0; uuids[i] !== undefined; i ++) {
					const	uuid	= uuids[i];

					tasks.push(function (cb) {
						const	product	= new Product(uuid);

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

			request.delete(esUrl + '/_search/scroll/' + scrollId, {}, function (err, response, body) {
				if (err) {
					log.warn(logPrefix + 'Could not get products to update, err: ' + err.message);
					return cb(err);
				}

				if (response.statusCode !== 200) {
					const	err	= new Error('non-200 response code: ' + response.statusCode + ', response body: ' + JSON.stringify(body));
					log.warn(logPrefix + err.message);
					return cb(err);
				}

				cb();
			});
		});
	});

	async.series(tasks, cb);
}

exports.attributes	= [];
exports.deleteByQuery	= deleteByQuery;
exports.formatEsResult	= formatEsResult;
exports.getAttributeValues	= getAttributeValues;
exports.getBooleans	= getBooleans;
exports.getDates	= getDates;
exports.getImagesForProducts	= getImagesForProducts;
exports.getFilesForProducts	= getFilesForProducts;
exports.getKeywords	= getKeywords;
exports.getMappedFieldNames	= getMappedFieldNames;
exports.updateByQuery	= updateByQuery;
