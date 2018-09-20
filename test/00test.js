/* eslint-disable require-jsdoc */
'use strict';

const elasticsearch	= require('elasticsearch');
const uuidValidate = require('uuid-validate');
const Intercom	= require('larvitamintercom');
const LUtils = require('larvitutils');
const log	= new (new LUtils()).Log();
const {ProductLib, Product} = require(__dirname + '/../index.js');
const request	= require('request');
const assert	= require('assert');
const async	= require('async');
const fs	= require('fs');
const os	= require('os');

const testIndexName = 'something';

let esUrl;
let prodLib;
let es;

before(function (done) {
	const tasks	= [];

	this.timeout(10000);

	// Check for empty ES
	tasks.push(function (cb) {
		let	confFile;

		if (fs.existsSync(__dirname + '/../config/es_test.json')) {
			confFile	= __dirname + '/../config/es_test.json';
		} else if (process.env.ESCONFFILE) {
			confFile	= process.env.ESCONFFILE;
		} else {
			throw new Error('No es config file found');
		}

		log.verbose('ES config file: "' + confFile + '"');

		const esConf = require(confFile);

		log.verbose('ES config: ' + JSON.stringify(esConf));
		esUrl = 'http://' + esConf.clientOptions.host;
		es = new elasticsearch.Client(esConf.clientOptions);

		request({'url': esUrl + '/_cat/indices?v', 'json': true}, function (err, response, body) {
			if (err) throw err;

			for (let i = 0; body[i] !== undefined; i ++) {
				const index = body[i];

				if (index.index === testIndexName || index.index === testIndexName + '_db_version') {
					throw new Error('Elasticsearch "' + prodLib.dataWriter.esIndexName + '" index already exists!');
				}
			}

			cb(err);
		});
	});

	// Create ProductLib
	tasks.push(function (cb) {
		const libOptions = {};

		libOptions.log = log;
		libOptions.esIndexName	= testIndexName;
		libOptions.mode = 'noSync';
		libOptions.intercom = new Intercom('loopback interface');
		libOptions.amsync = {};
		libOptions.amsync.host	= null;
		libOptions.amsync.minPort = null;
		libOptions.amsync.maxPort = null;
		libOptions.elasticsearch = es;

		prodLib = new ProductLib(libOptions, cb);
	});

	// Wait for dataWriter to be ready
	tasks.push(function (cb) {
		prodLib.dataWriter.ready(cb);
	});

	// Put mappings to ES to match our tests
	tasks.push(function (cb) {
		prodLib.dataWriter.elasticsearch.indices.putMapping({
			'index': prodLib.dataWriter.esIndexName,
			'type': 'product',
			'body': {
				'product': {
					'properties': {
						'trams': { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword' } } },
						'foo': { 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword' } } },
						'artNo': { 'type': 'keyword'},
						'supplier': { 'type': 'keyword'},
						'boolTest': { 'type': 'boolean'},
						'ragg': { 'type': 'boolean'}
					}
				}
			}
		}, cb);
	});

	async.series(tasks, done);
});

describe('Lib', function () {
	it('should create a log instance if no one is provided', function (done) {
		const LUtils = require('larvitutils');
		const lUtils = new LUtils();
		const libOptions = {};

		libOptions.esIndexName	= testIndexName;
		libOptions.mode = 'noSync';
		libOptions.intercom = new Intercom('loopback interface');
		libOptions.elasticsearch = es;

		const lib = new ProductLib(libOptions);

		assert(lib.log instanceof lUtils.Log);

		done();
	});
});

describe('Product', function () {
	let	productUuid;

	it('should not instantiate a new plain product object if productLib is missing from options', function (done) {
		try {
			new Product({});
		} catch (error) {
			assert.equal(error.message, 'Required option "productLib" is missing');
			done();
		}
	});

	it('should instantiate a new plain product object', function (done) {
		const product = new Product({'productLib': prodLib});

		assert.deepStrictEqual(toString.call(product),	'[object Object]');
		assert.deepStrictEqual(toString.call(product.attributes),	'[object Object]');
		assert.deepStrictEqual(uuidValidate(product.uuid, 1),	true);
		assert.deepStrictEqual(toString.call(product.created),	'[object Date]');

		done();
	});

	it('should instantiate a new plain product object with productLib factory function', function (done) {
		const product = prodLib.createProduct();

		assert.deepStrictEqual(toString.call(product),	'[object Object]');
		assert.deepStrictEqual(toString.call(product.attributes),	'[object Object]');
		assert.deepStrictEqual(uuidValidate(product.uuid, 1),	true);
		assert.deepStrictEqual(toString.call(product.created),	'[object Date]');
		assert.strictEqual(product.productLib, prodLib);

		done();
	});

	it('should instantiate a new plain product object, with empty object as option', function (done) {
		const product = prodLib.createProduct({});

		assert.deepStrictEqual(toString.call(product),	'[object Object]');
		assert.deepStrictEqual(toString.call(product.attributes),	'[object Object]');
		assert.deepStrictEqual(uuidValidate(product.uuid, 1),	true);
		assert.deepStrictEqual(toString.call(product.created),	'[object Date]');
		assert.strictEqual(product.productLib, prodLib);

		done();
	});

	it('should instantiate a new plain product object, with custom uuid', function (done) {
		const product = prodLib.createProduct('6a7c9adc-9b73-11e6-9f33-a24fc0d9649c');

		product.loadFromDb(function (err) {
			if (err) throw err;

			assert.deepStrictEqual(toString.call(product),	'[object Object]');
			assert.deepStrictEqual(toString.call(product.attributes),	'[object Object]');
			assert.deepStrictEqual(uuidValidate(product.uuid, 1),	true);
			assert.deepStrictEqual(product.uuid,	'6a7c9adc-9b73-11e6-9f33-a24fc0d9649c');
			assert.deepStrictEqual(toString.call(product.created),	'[object Date]');

			done();
		});
	});

	it('should instantiate a new plain product object, with custom uuid as explicit option', function (done) {
		const	product	= prodLib.createProduct({'uuid': '6a7c9adc-9b73-11e6-9f33-a24fc0d9649c'});

		product.loadFromDb(function (err) {
			if (err) throw err;

			assert.deepStrictEqual(toString.call(product),	'[object Object]');
			assert.deepStrictEqual(toString.call(product.attributes),	'[object Object]');
			assert.deepStrictEqual(uuidValidate(product.uuid, 1),	true);
			assert.deepStrictEqual(product.uuid,	'6a7c9adc-9b73-11e6-9f33-a24fc0d9649c');
			assert.deepStrictEqual(toString.call(product.created),	'[object Date]');

			done();
		});
	});

	it('should instantiate a new plain product object, with custom created', function (done) {
		const manCreated = new Date();
		const product = prodLib.createProduct({'created': manCreated});

		product.loadFromDb(function (err) {
			if (err) throw err;

			assert.deepStrictEqual(toString.call(product),	'[object Object]');
			assert.deepStrictEqual(toString.call(product.attributes),	'[object Object]');
			assert.deepStrictEqual(uuidValidate(product.uuid, 1),	true);
			assert.deepStrictEqual(product.created,	manCreated);

			done();
		});
	});

	it('should save a product', function (done) {
		function createProduct(cb) {
			const product = prodLib.createProduct();

			productUuid = product.uuid;

			product.attributes = {
				'name': 'Test product #69',
				'price': 99,
				'weight': 14,
				'color': ['blue', 'green']
			};

			product.save(cb);
		}

		function checkProduct(cb) {
			prodLib.dataWriter.elasticsearch.get({
				'index': prodLib.dataWriter.esIndexName,
				'type': 'product',
				'id': productUuid
			}, function (err, result) {
				if (err) throw err;

				assert.strictEqual(result._id,	productUuid);
				assert.strictEqual(result.found,	true);
				assert.strictEqual(result._source.name[0],	'Test product #69');
				assert.strictEqual(result._source.price[0],	99);
				assert.strictEqual(result._source.weight[0],	14);
				assert.strictEqual(result._source.color[0],	'blue');
				assert.strictEqual(result._source.color[1],	'green');

				cb();
			});
		}

		async.series([createProduct, checkProduct], function (err) {
			if (err) throw err;
			done();
		});
	});

	it('should load saved product from db', function (done) {
		const	product	= prodLib.createProduct(productUuid);

		product.loadFromDb(function (err) {
			if (err) throw err;

			assert.deepStrictEqual(product.uuid,	productUuid);
			assert.deepStrictEqual(product.attributes.name[0],	'Test product #69');
			assert.deepStrictEqual(product.attributes.price[0],	99);
			assert.deepStrictEqual(product.attributes.weight[0],	14);
			product.attributes.color.sort();
			assert.deepStrictEqual(product.attributes.color[0],	'blue');
			assert.deepStrictEqual(product.attributes.color[1],	'green');

			done();
		});
	});

	it('should alter an product already saved to db', function (done) {
		const	tasks	= [];

		tasks.push(function (cb) {
			const	product	= prodLib.createProduct(productUuid);

			product.loadFromDb(function (err) {
				if (err) throw err;

				product.attributes.boll = ['foo'];
				delete product.attributes.weight;

				product.save(function (err) {
					if (err) throw err;

					assert.deepStrictEqual(product.uuid,	productUuid);
					assert.deepStrictEqual(product.attributes.name,	['Test product #69']);
					assert.deepStrictEqual(product.attributes.price,	[99]);
					assert.deepStrictEqual(product.attributes.weight,	undefined);
					assert.deepStrictEqual(product.attributes.boll,	['foo']);
					product.attributes.color.sort();
					assert.deepStrictEqual(product.attributes.color,	['blue', 'green']);

					cb();
				});
			});
		});

		tasks.push(function (cb) {
			const	product	= prodLib.createProduct(productUuid);

			product.loadFromDb(function (err) {
				if (err) throw err;

				assert.deepStrictEqual(product.uuid,	productUuid);
				assert.deepStrictEqual(product.attributes.name,	['Test product #69']);
				assert.deepStrictEqual(product.attributes.price,	[99]);
				assert.deepStrictEqual(product.attributes.weight,	undefined);
				assert.deepStrictEqual(product.attributes.boll,	['foo']);
				product.attributes.color.sort();
				assert.deepStrictEqual(product.attributes.color,	['blue', 'green']);

				cb();
			});
		});

		async.series(tasks, done);
	});

	it('should remove a product', function (done) {
		const	tasks	= [];

		// Add some more products
		tasks.push(function (cb) {
			const	product	= prodLib.createProduct();

			product.attributes.foo	= 'bar';
			product.attributes.nisse	= 'mm';
			product.attributes.active	= 'true';
			product.attributes.bacon	= 'yes';
			product.save(cb);
		});
		tasks.push(function (cb) {
			const	product	= prodLib.createProduct();

			product.attributes.foo	= 'baz';
			product.attributes.nisse	= 'nej';
			product.attributes.active	= 'true';
			product.attributes.bacon	= 'no';
			product.save(cb);
		});
		tasks.push(function (cb) {
			const	product	= prodLib.createProduct();

			product.attributes.foo	= 'bar';
			product.attributes.active	= 'true';
			product.attributes.bacon	= 'narwhal';
			product.save(cb);
		});

		// Get all products before
		tasks.push(function (cb) {
			prodLib.dataWriter.elasticsearch.search({
				'index': prodLib.dataWriter.esIndexName,
				'type': 'product'
			}, function (err, result) {
				if (err) throw err;

				assert.strictEqual(result.hits.total,	4);

				cb();
			});
		});

		// Remove a product
		tasks.push(function (cb) {
			const	product	= prodLib.createProduct(productUuid);

			product.rm(cb);
		});

		// Refresh index
		tasks.push(function (cb) {
			request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', cb);
		});

		// Get all products after
		tasks.push(function (cb) {
			prodLib.dataWriter.elasticsearch.search({
				'index': prodLib.dataWriter.esIndexName,
				'type': 'product'
			}, function (err, result) {
				if (err) throw err;

				assert.strictEqual(result.hits.total,	3);

				for (let i = 0; result.hits.hits[i] !== undefined; i ++) {
					assert.notStrictEqual(result.hits.hits[i]._id,	productUuid);
				}

				cb();
			});
		});

		async.series(tasks, function (err) {
			if (err) throw err;
			done();
		});
	});
});

describe('Helpers', function () {
	it('should save some more products to play with', function (done) {
		const tasks = [];

		tasks.push(function (cb) {
			const	product	= prodLib.createProduct();

			product.attributes.enabled2	= 'true';
			product.attributes.enabled	= 'true';
			product.attributes.country	= 'all';
			product.attributes.country2	= 'all';
			product.save(cb);
		});

		tasks.push(function (cb) {
			const	product	= prodLib.createProduct();

			product.attributes.enabled2	= ['true', 'maybe'];
			product.attributes.enabled	= ['true', 'maybe'];
			product.attributes.country	= 'se';
			product.attributes.country2	= 'se';
			product.save(cb);
		});

		tasks.push(function (cb) {
			const	product	= prodLib.createProduct();

			product.attributes.enabled2	= 'false';
			product.attributes.enabled	= 'false';
			product.attributes.country	= 'se';
			product.attributes.country2	= 'se';
			product.save(cb);
		});

		tasks.push(function (cb) {
			const	product	= prodLib.createProduct();

			product.attributes.enabled2	= ['maybe', 'true'];
			product.attributes.enabled	= ['true', 'maybe'];
			product.attributes.country	= 'dk';
			product.attributes.country2	= 'dk';
			product.save(cb);
		});

		tasks.push(function (cb) {
			const	product	= prodLib.createProduct();

			product.attributes.enabled2	= ['maybe', 'true'];
			product.attributes.enabled	= ['true', 'maybe'];
			product.attributes.country	= 'all';
			product.attributes.country2	= 'se';
			product.save(cb);
		});

		async.parallel(tasks, function (err) {
			if (err) throw err;

			// Refresh index
			request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', function (err) {
				if (err) throw err;
				done();
			});
		});
	});

	it('should get attribute values', function (done) {
		prodLib.helpers.getAttributeValues('foo.keyword', function (err, result) {
			if (err) throw err;

			assert.deepStrictEqual(result,	['bar', 'baz']);
			done();
		});
	});

	it('should get empty array on non existing attribute name', function (done) {
		prodLib.helpers.getAttributeValues('trams.keyword', function (err, result) {
			if (err) throw err;

			assert.deepStrictEqual(result,	[]);
			done();
		});
	});

	it('should ignore BOMs in strings', function (done) {
		const	product	= prodLib.createProduct();

		product.attributes[Buffer.from('efbbbf70', 'hex').toString()]	= 'bulle';
		product.save(function (err) {
			if (err) throw err;

			prodLib.dataWriter.elasticsearch.get({
				'index': prodLib.dataWriter.esIndexName,
				'type': 'product',
				'id': product.uuid
			}, function (err, result) {
				if (err) throw err;

				assert.deepStrictEqual(Object.keys(result._source), ['created', 'p']);

				done();
			});
		});
	});

	it('should get all keywords', function (done) {
		const	expectedKeywords	= [];

		expectedKeywords.push('active.keyword');
		expectedKeywords.push('artNo');
		expectedKeywords.push('bacon.keyword');
		expectedKeywords.push('boll.keyword');
		expectedKeywords.push('color.keyword');
		expectedKeywords.push('country.keyword');
		expectedKeywords.push('country2.keyword');
		expectedKeywords.push('enabled.keyword');
		expectedKeywords.push('enabled2.keyword');
		expectedKeywords.push('foo.keyword');
		expectedKeywords.push('name.keyword');
		expectedKeywords.push('nisse.keyword');
		expectedKeywords.push('p.keyword');
		expectedKeywords.push('supplier');
		expectedKeywords.push('trams.keyword');

		prodLib.helpers.getKeywords(function (err, keywords) {
			if (err) throw err;

			expectedKeywords.sort();
			keywords.sort();

			assert.deepStrictEqual(expectedKeywords,	keywords);

			done();
		});
	});

	it('should get all booleans', function (done) {
		const	expectedBools	= ['ragg', 'boolTest'];

		prodLib.helpers.getBooleans(function (err, booleans) {
			expectedBools.sort();
			booleans.sort();

			assert.deepEqual(booleans, expectedBools);

			done();
		});
	});

	it('update by query', function (done) {
		const	tasks	= [];

		tasks.push(function (cb) {
			const queryBody	= {};
			const updates = {};

			queryBody.query	= {'bool': {'filter': {'term': {'active': 'true'}}}};
			updates.enabled	= ['true'];

			prodLib.helpers.updateByQuery(queryBody, updates, cb);
		});

		// Refresh index
		tasks.push(function (cb) {
			request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', cb);
		});

		tasks.push(function (cb) {
			request({'url': esUrl + '/' + prodLib.dataWriter.esIndexName + '/product/_search', 'json': true}, function (err, response, body) {
				if (err) throw err;

				for (let i = 0; body.hits.hits[i] !== undefined; i ++) {
					const	source	= body.hits.hits[i]._source;

					if (Array.isArray(source.active) && source.active[0] === 'true') {
						assert.strictEqual(source.enabled[0],	'true');
					}
				}

				cb();
			});
		});

		async.series(tasks, function (err) {
			if (err) throw err;
			done();
		});
	});

	it('delete by query', function (done) {
		const	tasks	= [];

		let	prodBeforeDelete;

		// Refresh index
		tasks.push(function (cb) {
			request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', function (err) {
				if (err) throw err;
				setTimeout(cb, 200);
			});
		});

		// Pre-calc products
		tasks.push(function (cb) {
			const	reqOptions	= {};

			reqOptions.url	= esUrl + '/' + prodLib.dataWriter.esIndexName + '/product/_search';
			reqOptions.body	= {'size': 1000, 'query': {'match_all': {}}};
			reqOptions.json	= true;

			request(reqOptions, function (err, response, body) {
				if (err) throw err;

				prodBeforeDelete	= body.hits.hits.length;

				cb();
			});
		});

		tasks.push(function (cb) {
			const	queryBody	= {};

			queryBody.query	= {'bool': {'filter': {'term': {'foo': 'bar'}}}};

			prodLib.helpers.deleteByQuery(queryBody, cb);
		});

		// Refresh index
		tasks.push(function (cb) {
			request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', function (err) {
				if (err) throw err;
				setTimeout(cb, 200);
			});
		});

		tasks.push(function (cb) {
			const	reqOptions	= {};

			reqOptions.url	= esUrl + '/' + prodLib.dataWriter.esIndexName + '/product/_search';
			reqOptions.body	= {'size': 1000, 'query': {'match_all': {}}};
			reqOptions.json	= true;

			request(reqOptions, function (err, response, body) {
				if (err) throw err;

				assert.strictEqual(body.hits.hits.length,	prodBeforeDelete - 2);

				cb();
			});
		});

		async.series(tasks, function (err) {
			if (err) throw err;
			done();
		});
	});

	it('should get all mapped field names', function (done) {
		prodLib.helpers.getMappedFieldNames(function (err, names) {
			if (err) throw err;
			assert.strictEqual(names.length,	20);
			assert.notStrictEqual(names.indexOf('price'),	- 1);
			assert.notStrictEqual(names.indexOf('enabled'),	- 1);
			done();
		});
	});
});

describe('Import', function () {
	// Make sure the index is refreshed between each test
	beforeEach(function (done) {
		request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', function (err) {
			if (err) throw err;
			done();
		});
	});

	function importFromStr(str, options, cb) {
		const tmpFile = os.tmpdir() + '/tmp_products.csv';
		const tasks = [];

		let	uuids	= [];

		// First create our test file
		tasks.push(function (cb) {
			fs.writeFile(tmpFile, str, cb);
		});

		// Import file
		tasks.push(function (cb) {
			prodLib.importer.fromFile(tmpFile, options, function (err, result) {
				uuids	= result;

				if (err) throw err;

				cb();
			});
		});

		// Remove tmp file
		tasks.push(function (cb) {
			fs.unlink(tmpFile, cb);
		});

		// Refresh index
		tasks.push(function (cb) {
			request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', cb);
		});

		async.series(tasks, function (err) {
			cb(err, uuids);
		});
	}

	function getProductData(uuids, cb) {
		const	options	= {};

		options.method	= 'GET';
		options.json	= true;
		options.url	= esUrl + '/' + prodLib.dataWriter.esIndexName + '/product/_search';
		options.body	= {'query': {'ids': {'values': uuids}}};

		request(options, function (err, response, result) {
			if (err) throw err;

			return cb(null, result.hits.hits);
		});
	}

	function countProducts(cb) {
		request({'url': esUrl + '/' + prodLib.dataWriter.esIndexName + '/product/_count', 'json': true}, function (err, response, body) {
			if (err) throw err;

			cb(err, body.count);
		});
	}

	function uniqueConcat(array) {
		for (let i = 0; i < array.length; ++ i) {
			for (let j = i + 1; j < array.length; ++ j) {
				if (array[i] === array[j]) array.splice(j --, 1);
			}
		}

		return array;
	};

	function refreshIndex(cb) {
		request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', cb);
	}

	function deleteAllProducts(cb) {
		const	options	= {};

		options.method	= 'POST';
		options.json	= true;
		options.url	= esUrl + '/' + prodLib.dataWriter.esIndexName + '/product/_delete_by_query?refresh';
		options.body	= {'query': {'match_all': {}}};

		request(options, cb);
	}

	it('very simple test case', function (done) {
		const productStr = 'name,price,description\nball,100,it is round\ntv,55,"About 32"" in size"';
		const tasks = [];

		let	uuids;

		// Remove all previous products
		tasks.push(function (cb) {
			deleteAllProducts(cb);
		});

		// Run importer
		tasks.push(function (cb) {
			importFromStr(productStr, {}, function (err, result) {
				if (err) throw err;

				uuids	= result;

				assert.strictEqual(uuids.length,	2);
				cb();
			});
		});

		// Get product data and check it
		tasks.push(function (cb) {
			getProductData(uuids, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.length,	2);

				for (let i = 0; testProducts[i] !== undefined; i ++) {
					const	product	= testProducts[i];

					assert.strictEqual(Object.keys(product._source).length,	4);

					if (product._source.name[0] === 'ball') {
						assert.strictEqual(product._source.price[0],	'100');
						assert.strictEqual(product._source.description[0],	'it is round');
					} else if (product._source.name[0] === 'tv') {
						assert.strictEqual(product._source.price[0],	'55');
						assert.strictEqual(product._source.description[0],	'About 32" in size');
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}

				cb();
			});
		});

		// Count total number of products in database
		tasks.push(function (cb) {
			countProducts(function (err, count) {
				assert.strictEqual(count,	2);
				cb(err);
			});
		});

		async.series(tasks, done);
	});

	it('Override static column data', function (done) {
		const productStr = 'name,artNo,size,enabled\nball,abc01,3,true\ntv,abc02,14,false\nspoon,abc03,2,true';
		const options = {'staticCols': { 'foul': 'nope', 'enabled': 'false'} };
		const tasks = [];

		let	uuids;

		// Remove all previous products
		tasks.push(function (cb) {
			deleteAllProducts(cb);
		});

		// Import
		tasks.push(function (cb) {
			importFromStr(productStr, options, function (err, result) {
				if (err) throw err;

				uuids	= result;

				assert.strictEqual(uuids.length,	3);
				cb();
			});
		});

		// Get and check product data
		tasks.push(function (cb) {
			getProductData(uuids, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.length,	3);

				for (let i = 0; testProducts[i] !== undefined; i ++) {
					const	product	= testProducts[i];

					assert.strictEqual(Object.keys(product._source).length,	6);

					if (product._source.name[0] === 'ball') {
						assert.strictEqual(product._source.artNo[0],	'abc01');
						assert.strictEqual(product._source.size[0],	'3');
						assert.strictEqual(product._source.enabled[0],	'true');
						assert.strictEqual(product._source.foul[0],	'nope');
					} else if (product._source.name[0] === 'tv') {
						assert.strictEqual(product._source.artNo[0],	'abc02');
						assert.strictEqual(product._source.size[0],	'14');
						assert.strictEqual(product._source.enabled[0],	'false');
						assert.strictEqual(product._source.foul[0],	'nope');
					} else if (product._source.name[0] === 'spoon') {
						assert.strictEqual(product._source.artNo[0],	'abc03');
						assert.strictEqual(product._source.size[0],	'2');
						assert.strictEqual(product._source.enabled[0],	'true');
						assert.strictEqual(product._source.foul[0],	'nope');
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}

				cb();
			});
		});

		// Count products
		tasks.push(function (cb) {
			countProducts(function (err, count) {
				assert.strictEqual(count,	3);
				cb(err);
			});
		});

		async.series(tasks, done);
	});

	it('Replace by one column', function (done) {
		const initProductStr = 'name,artNo,size,description\n' +
				'house,abc01,20,huge\n' +
				'napkin,food3k,9,small\n' +
				'car,abc13,7,vehicle\n' +
				'plutt,ieidl3,10,no';
		const replProductStr = 'name,artNo,size\n' +
				'ball,abc01,15\n' +
				'tv,abc02,14\n' +
				'car," abc13",2'; // Deliberate space
		const tasks = [];

		let	uuids;

		// Remove all previous products
		tasks.push(function (cb) {
			deleteAllProducts(cb);
		});

		// Run initial report
		tasks.push(function (cb) {
			importFromStr(initProductStr, {}, function (err, result) {
				if (err) throw err;
				uuids	= result;
				cb();
			});
		});

		// Refresh index
		tasks.push(refreshIndex);

		// Run replacement import
		tasks.push(function (cb) {
			importFromStr(replProductStr, {'replaceByCols': 'artNo'}, function (err, result) {
				if (err) throw err;
				uuids	= uuids.concat(result);
				cb();
			});
		});

		// Refresh index
		tasks.push(refreshIndex);

		// Count hits
		tasks.push(function (cb) {
			countProducts(function (err, count) {
				if (err) throw err;
				assert.strictEqual(count, 5);
				cb();
			});
		});

		// Check product data
		tasks.push(function (cb) {
			getProductData(uuids, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.length,	5);

				for (let i = 0; testProducts[i] !== undefined; i ++) {
					const	product	= testProducts[i];

					if (product._source.name[0] === 'ball') {
						assert.strictEqual(product._source.artNo[0],	'abc01');
						assert.strictEqual(product._source.size[0],	'15');
						assert.strictEqual(Object.keys(product._source).length,	4);
					} else if (product._source.name[0] === 'tv') {
						assert.strictEqual(product._source.artNo[0],	'abc02');
						assert.strictEqual(product._source.size[0],	'14');
						assert.strictEqual(Object.keys(product._source).length,	4);
					} else if (product._source.name[0] === 'car') {
						assert.strictEqual(product._source.artNo[0],	'abc13');
						assert.strictEqual(product._source.size[0],	'2');
						assert.strictEqual(Object.keys(product._source).length,	4);
					} else if (product._source.name[0] === 'napkin') {
						assert.strictEqual(product._source.artNo[0],	'food3k');
						assert.strictEqual(product._source.size[0],	'9');
						assert.strictEqual(product._source.description[0],	'small');
						assert.strictEqual(Object.keys(product._source).length,	5);
					} else if (product._source.name[0] === 'plutt') {
						assert.strictEqual(product._source.artNo[0],	'ieidl3');
						assert.strictEqual(product._source.size[0],	'10');
						assert.strictEqual(product._source.description[0],	'no');
						assert.strictEqual(Object.keys(product._source).length,	5);
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}
				cb();
			});
		});

		async.series(tasks, done);
	});

	it('Replace by two columns', function (done) {
		const productStr1 = 'supplier,artNo,name\nurkus ab,bb1,foo\nurkus ab,bb2,bar\nbleff ab,bb1,elk';
		const productStr2 = 'supplier,artNo,name\nurkus ab,bb1,MUU\nblimp 18,bb2,tefflon\nbleff ab,bb1,bolk';
		const options = {'replaceByCols': ['artNo', 'supplier']};
		const tasks = [];

		let	preNoProducts;
		let uuids1;
		let uuids2;

		// Remove all previous products
		tasks.push(function (cb) {
			deleteAllProducts(cb);
		});

		// Run the import of productStr1
		tasks.push(function (cb) {
			importFromStr(productStr1, options, function (err, result) {
				if (err) throw err;
				uuids1 = result;
				assert.strictEqual(uuids1.length,	3);
				cb();
			});
		});

		// Refresh index
		tasks.push(function (cb) {
			request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', cb);
		});

		// Pre-count products
		tasks.push(function (cb) {
			countProducts(function (err, count) {
				preNoProducts	= count;
				cb(err);
			});
		});

		// Run the import of productStr2
		tasks.push(function (cb) {
			importFromStr(productStr2, options, function (err, result) {
				if (err) throw err;
				uuids2 = result;
				assert.strictEqual(uuids2.length,	3);
				cb();
			});
		});

		// Refresh index
		tasks.push(function (cb) {
			request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', cb);
		});

		tasks.push(function (cb) {
			setTimeout(cb, 1100);
		});

		// Count hits after index
		tasks.push(function (cb) {
			countProducts(function (err, count) {
				if (err) throw err;
				assert.strictEqual(preNoProducts, (count - 1));
				cb();
			});
		});

		// Check product data
		tasks.push(function (cb) {
			getProductData(uuids2, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.length,	3);

				for (let i = 0; testProducts[i] !== undefined; i ++) {
					const	product	= testProducts[i];

					assert.strictEqual(Object.keys(product._source).length,	4);

					if (product._source.supplier[0] === 'urkus ab' && product._source.artNo[0] === 'bb1') {
						assert.strictEqual(product._source.name[0],	'MUU');
					} else if (product._source.supplier[0] === 'blimp 18' && product._source.artNo[0] === 'bb2') {
						assert.strictEqual(product._source.name[0],	'tefflon');
					} else if (product._source.supplier[0] === 'bleff ab' && product._source.artNo[0] === 'bb1') {
						assert.strictEqual(product._source.name[0],	'bolk');
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}
				cb();
			});
		});

		async.series(tasks, done);
	});

	it('Update by two columns', function (done) {
		const productStr1 = 'supplier,artNo,name,size\nslam ab,rd1,foo,100\nslam ab,rd2,bar,200\nbang ab,hhv4,elk,300';
		const productStr2 = 'supplier,artNo,name\nslam ab,rd1,MUU\npaow,bb2,tefflon\nbang ab,hhv4,bolk';
		const options = {'updateByCols': ['artNo', 'supplier']};
		const tasks = [];

		let	preNoProducts;
		let uuids1;
		let uuids2;

		// Run the import of productStr1
		tasks.push(function (cb) {
			importFromStr(productStr1, options, function (err, result) {
				if (err) throw err;
				uuids1 = result;
				assert.strictEqual(uuids1.length,	3);
				cb();
			});
		});

		// Refresh index
		tasks.push(function (cb) {
			request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', cb);
		});

		// Pre-count products
		tasks.push(function (cb) {
			countProducts(function (err, count) {
				preNoProducts	= count;
				cb(err);
			});
		});

		// Run the import of productStr1
		tasks.push(function (cb) {
			importFromStr(productStr2, options, function (err, result) {
				if (err) throw err;
				uuids2 = result;
				assert.strictEqual(uuids2.length,	3);
				cb();
			});
		});

		// Refresh index
		tasks.push(function (cb) {
			request.post(esUrl + '/' + prodLib.dataWriter.esIndexName + '/_refresh', cb);
		});

		// Count hits after index
		tasks.push(function (cb) {
			countProducts(function (err, count) {
				if (err) throw err;
				assert.strictEqual(preNoProducts, (count - 1));
				cb();
			});
		});

		// Check product data
		tasks.push(function (cb) {
			const	uuids	= uniqueConcat(uuids1.concat(uuids2));

			getProductData(uuids, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.length,	4);

				for (let i = 0; testProducts[i] !== undefined; i ++) {
					const	product	= testProducts[i];

					if (product._source.supplier[0] === 'slam ab' && product._source.artNo[0] === 'rd1') {
						assert.strictEqual(product._source.name[0],	'MUU');
						assert.strictEqual(parseInt(product._source.size[0]),	100);
					} else if (product._source.supplier[0] === 'paow' && product._source.artNo[0] === 'bb2') {
						assert.strictEqual(product._source.name[0],	'tefflon');
						assert.strictEqual(product._source.size,	undefined);
					} else if (product._source.supplier[0] === 'bang ab' && product._source.artNo[0] === 'hhv4') {
						assert.strictEqual(product._source.name[0],	'bolk');
						assert.strictEqual(parseInt(product._source.size[0]),	300);
					} else if (product._source.supplier[0] === 'slam ab' && product._source.artNo[0] === 'rd2') {
						assert.strictEqual(product._source.name[0],	'bar');
						assert.strictEqual(parseInt(product._source.size[0]),	200);
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}
				cb();
			});
		});

		async.series(tasks, done);
	});

	it('Ignore column values', function (done) {
		const productStr = 'name,price,description,foo\nball,100,it is round,N/A\ntv,55,Large sized,bar\nsoffa,1200,n/a,N/A\nbord,20,,n/a';
		const tasks	= [];

		let	uuids;

		// Remove all previous products
		tasks.push(function (cb) {
			deleteAllProducts(cb);
		});

		// Run importer
		tasks.push(function (cb) {
			importFromStr(productStr, {'removeColValsContaining': ['N/A', '']}, function (err, result) {
				if (err) throw err;

				uuids	= result;

				assert.strictEqual(uuids.length,	4);
				cb();
			});
		});

		// Get product data and check it
		tasks.push(function (cb) {
			getProductData(uuids, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.length,	4);

				for (let i = 0; testProducts[i] !== undefined; i ++) {
					const	product	= testProducts[i];

					if (product._source.name[0] === 'ball') {
						assert.strictEqual(product._source.price[0],	'100');
						assert.strictEqual(product._source.description[0],	'it is round');
						assert.strictEqual(product._source.foo,	undefined);
					} else if (product._source.name[0] === 'tv') {
						assert.strictEqual(product._source.price[0],	'55');
						assert.strictEqual(product._source.description[0],	'Large sized');
						assert.strictEqual(product._source.foo[0],	'bar');
					} else if (product._source.name[0] === 'soffa') {
						assert.strictEqual(product._source.price[0],	'1200');
						assert.strictEqual(product._source.description[0],	'n/a');
						assert.strictEqual(product._source.foo,	undefined);
					} else if (product._source.name[0] === 'bord') {
						assert.strictEqual(product._source.price[0],	'20');
						assert.strictEqual(product._source.description,	undefined);
						assert.strictEqual(product._source.foo[0],	'n/a');
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}

				cb();
			});
		});

		// Count total number of products in database
		tasks.push(function (cb) {
			countProducts(function (err, count) {
				assert.strictEqual(count,	4);
				cb(err);
			});
		});

		async.series(tasks, done);
	});

	it('Remove values where empty', function (done) {
		const productStr = 'name,price,description,foo\n' +
							  'ball,100,it is round,N/A\n' +
							  'tv,55,Large sized,bar\n' +
							  'soffa,1200,n/a,N/A\n' +
							  'bord,20,untz,n/a';
		const tasks = [];

		let	uuids;

		// Remove all previous products
		tasks.push(function (cb) {
			deleteAllProducts(cb);
		});

		// Run importer
		tasks.push(function (cb) {
			importFromStr(productStr, {}, function (err, result) {
				if (err) throw err;

				uuids	= result;

				assert.strictEqual(uuids.length,	4);
				cb();
			});
		});

		// Get product data and check it
		tasks.push(function (cb) {
			getProductData(uuids, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.length,	4);

				for (let i = 0; testProducts[i] !== undefined; i ++) {
					const	product	= testProducts[i];

					if (product._source.name[0] === 'ball') {
						assert.strictEqual(product._source.price[0],	'100');
						assert.strictEqual(product._source.description[0],	'it is round');
						assert.strictEqual(product._source.foo[0],	'N/A');
					} else if (product._source.name[0] === 'tv') {
						assert.strictEqual(product._source.price[0],	'55');
						assert.strictEqual(product._source.description[0],	'Large sized');
						assert.strictEqual(product._source.foo[0],	'bar');
					} else if (product._source.name[0] === 'soffa') {
						assert.strictEqual(product._source.price[0],	'1200');
						assert.strictEqual(product._source.description[0],	'n/a');
						assert.strictEqual(product._source.foo[0],	'N/A');
					} else if (product._source.name[0] === 'bord') {
						assert.strictEqual(product._source.price[0],	'20');
						assert.strictEqual(product._source.description[0], 'untz');
						assert.strictEqual(product._source.foo[0],	'n/a');
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}

				cb();
			});
		});

		// Count total number of products in database
		tasks.push(function (cb) {
			countProducts(function (err, count) {
				assert.strictEqual(count,	4);
				cb(err);
			});
		});

		// Run importer
		tasks.push(function (cb) {
			const prodStr2 = 'name,price,description,foo\n' +
							 'ball,100,it is round,\n' +
							 'tv,55,Large sized,bar\n' +
							 'soffa,1200,n/a,\n' +
							 'bord,20,,n/a';

			importFromStr(prodStr2, {'removeValWhereEmpty': true, 'updateByCols': ['name'], 'removeColValsContaining': ['N/A', 'n/a']}, function (err, result) {
				if (err) throw err;

				uuids	= result;

				assert.strictEqual(uuids.length,	4);
				cb();
			});
		});

		// Get product data and check it
		tasks.push(function (cb) {
			getProductData(uuids, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.length,	4);

				for (let i = 0; testProducts[i] !== undefined; i ++) {
					const	product	= testProducts[i];

					if (product._source.name[0] === 'ball') {
						assert.strictEqual(product._source.price[0],	'100');
						assert.strictEqual(product._source.description[0],	'it is round');
						assert.strictEqual(product._source.foo,	undefined);
					} else if (product._source.name[0] === 'tv') {
						assert.strictEqual(product._source.price[0],	'55');
						assert.strictEqual(product._source.description[0],	'Large sized');
						assert.strictEqual(product._source.foo[0],	'bar');
					} else if (product._source.name[0] === 'soffa') {
						assert.strictEqual(product._source.price[0],	'1200');
						assert.strictEqual(product._source.description[0],	'n/a');
						assert.strictEqual(product._source.foo,	undefined);
					} else if (product._source.name[0] === 'bord') {
						assert.strictEqual(product._source.price[0],	'20');
						assert.strictEqual(product._source.description,	undefined);
						assert.strictEqual(product._source.foo[0],	'n/a');
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}

				cb();
			});
		});

		async.series(tasks, done);
	});

	it('Hook: afterEachCsvRow', function (done) {
		const productStr = 'name,price,description,foo\nball,100,it is round,N/A\ntv,55,Large sized,bar\nsoffa,1200,n/a,N/A\nbord,20,,n/a';
		const prodNames	= [];
		const tasks = [];

		let	uuids;

		// Remove all previous products
		tasks.push(function (cb) {
			deleteAllProducts(cb);
		});

		// Run importer
		tasks.push(function (cb) {
			const	options	= {};

			options.hooks = {
				'afterEachCsvRow': function (stuff, cb) {
					prodNames.push(stuff.product.attributes.name[0]);
					cb();
				}
			};

			importFromStr(productStr, options, function (err, result) {
				if (err) throw err;

				uuids	= result;

				assert.strictEqual(uuids.length,	4);
				cb();
			});
		});

		// Check prodNames
		tasks.push(function (cb) {
			assert.strictEqual(prodNames.length,	4);
			assert.notStrictEqual(prodNames.indexOf('tv'),	- 1);
			assert.notStrictEqual(prodNames.indexOf('bord'),	- 1);
			assert.notStrictEqual(prodNames.indexOf('soffa'),	- 1);
			assert.notStrictEqual(prodNames.indexOf('ball'),	- 1);
			cb();
		});

		// Count total number of products in database
		tasks.push(function (cb) {
			countProducts(function (err, count) {
				assert.strictEqual(count,	4);
				cb(err);
			});
		});

		async.series(tasks, done);
	});
});

after(function (done) {
	const	tasks	= [];

	// Remove all data from elasticsearch
	tasks.push(function (cb) {
		if (! esUrl) return cb();
		request.delete(esUrl + '/' + prodLib.dataWriter.esIndexName, cb);
	});
	tasks.push(function (cb) {
		if (! esUrl) return cb();
		request.delete(esUrl + '/' + prodLib.dataWriter.esIndexName + '_db_version', cb);
	});

	async.parallel(tasks, done);
});
