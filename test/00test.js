'use strict';

const	elasticsearch	= require('elasticsearch'),
	uuidValidate	= require('uuid-validate'),
	productLib	= require(__dirname + '/../index.js'),
	Intercom	= require('larvitamintercom'),
	assert	= require('assert'),
	lUtils	= require('larvitutils'),
	async	= require('async'),
	log	= require('winston'),
	fs	= require('fs'),
	os	= require('os');

let	esConf,
	es;

// Set up winston
log.remove(log.transports.Console);
/**/log.add(log.transports.Console, {
	'level':	'warn',
	'colorize':	true,
	'timestamp':	true,
	'json':	false,
	'handleException':	true,
	'humanReadableUnhandledException':	true
}); /**/

before(function (done) {
	this.timeout(10000);
	const	tasks	= [];

	// Run ES Setup
	tasks.push(function (cb) {
		let	confFile;

		if (process.env.ESCONFFILE === undefined) {
			confFile = __dirname + '/../config/es_test.json';
		} else {
			confFile = process.env.ESCONFFILE;
		}

		log.verbose('ES config file: "' + confFile + '"');

		// First look for absolute path
		fs.stat(confFile, function (err) {
			if (err) {

				// Then look for this string in the config folder
				confFile = __dirname + '/../config/' + confFile;
				fs.stat(confFile, function (err) {
					if (err) throw err;
					esConf	= require(confFile);
					log.verbose('ES config: ' + JSON.stringify(esConf));

					es = lUtils.instances.elasticsearch = new elasticsearch.Client(esConf.clientOptions);
					es.ping(cb);
				});

				return;
			}

			esConf	= require(confFile);
			log.verbose('DB config: ' + JSON.stringify(esConf));
			es = lUtils.instances.elasticsearch = new elasticsearch.Client(esConf.clientOptions);
			es.ping(cb);
		});
	});

	// Check for empty db
	tasks.push(function (cb) {
		es.cat.indices({'v': true}, function (err, result) {
			if (err) throw err;

			// Source: https://www.elastic.co/guide/en/elasticsearch/reference/1.4/_list_all_indexes.html
			if (result !== 'health status index uuid pri rep docs.count docs.deleted store.size pri.store.size\n') {
				throw new Error('Database is not empty. To make a test, you must supply an empty database!');
			}

			cb(err);
		});
	});

	// Setup intercom
	tasks.push(function (cb) {
		let confFile;

		if (process.env.INTCONFFILE === undefined) {
			confFile = __dirname + '/../config/amqp_test.json';
		} else {
			confFile = process.env.INTCONFFILE;
		}

		log.verbose('Intercom config file: "' + confFile + '"');

		// First look for absolute path
		fs.stat(confFile, function (err) {
			if (err) {

				// Then look for this string in the config folder
				confFile = __dirname + '/../config/' + confFile;
				fs.stat(confFile, function (err) {
					if (err) throw err;
					log.verbose('Intercom config: ' + JSON.stringify(require(confFile)));
					lUtils.instances.intercom = new Intercom(require(confFile).default);
					lUtils.instances.intercom.on('ready', cb);
				});

				return;
			}

			log.verbose('Intercom config: ' + JSON.stringify(require(confFile)));
			lUtils.instances.intercom = new Intercom(require(confFile).default);
			lUtils.instances.intercom.on('ready', cb);
		});
	});

	// Wait for dataWriter to be ready
	tasks.push(productLib.dataWriter.ready);

	// Put mappings to ES to match our tests
	tasks.push(function (cb) {
		es.indices.putMapping({
			'index':	'larvitproduct',
			'type':	'product',
			'body': {
				'product': {
					'properties': {
						'trams':	{ 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword' } } },
						'foo':	{ 'type': 'text', 'fields': { 'keyword': { 'type': 'keyword' } } }
					}
				}
			}
		}, cb);
	});

	// Preload caches etc
	// We do this so the timing of the rest of the tests gets more correct

	async.series(tasks, done);
});

describe('Product', function () {
	let	productUuid;

	it('should instantiate a new plain product object', function (done) {
		const product = new productLib.Product();

		assert.deepStrictEqual(toString.call(product),	'[object Object]');
		assert.deepStrictEqual(toString.call(product.attributes),	'[object Object]');
		assert.deepStrictEqual(uuidValidate(product.uuid, 1),	true);
		assert.deepStrictEqual(toString.call(product.created),	'[object Date]');

		done();
	});

	it('should instantiate a new plain product object, with empty object as option', function (done) {
		const product = new productLib.Product({});

		assert.deepStrictEqual(toString.call(product),	'[object Object]');
		assert.deepStrictEqual(toString.call(product.attributes),	'[object Object]');
		assert.deepStrictEqual(uuidValidate(product.uuid, 1),	true);
		assert.deepStrictEqual(toString.call(product.created),	'[object Date]');

		done();
	});

	it('should instantiate a new plain product object, with custom uuid', function (done) {
		const product = new productLib.Product('6a7c9adc-9b73-11e6-9f33-a24fc0d9649c');

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
		const product = new productLib.Product({'uuid': '6a7c9adc-9b73-11e6-9f33-a24fc0d9649c'});

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
		const	manCreated	= new Date(),
			product	= new productLib.Product({'created': manCreated});

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
			const product = new productLib.Product();

			productUuid = product.uuid;

			product.attributes = {
				'name':	'Test product #69',
				'price':	99,
				'weight':	14,
				'color':	['blue', 'green']
			};

			product.save(cb);
		}

		function checkProduct(cb) {
			es.get({
				'index':	'larvitproduct',
				'type':	'product',
				'id':	productUuid
			}, function (err, result) {
				if (err) throw err;

				assert.strictEqual(result._id,	productUuid);
				assert.strictEqual(result.found,	true);
				assert.strictEqual(result._source.name[0],	'Test product #69');
				assert.strictEqual(result._source.price[0],	'99');
				assert.strictEqual(result._source.weight[0],	'14');
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
		const product = new productLib.Product(productUuid);

		product.loadFromDb(function (err) {
			if (err) throw err;

			assert.deepStrictEqual(product.uuid,	productUuid);
			assert.deepStrictEqual(product.attributes.name[0],	'Test product #69');
			assert.deepStrictEqual(product.attributes.price[0],	'99');
			assert.deepStrictEqual(product.attributes.weight[0],	'14');
			product.attributes.color.sort();
			assert.deepStrictEqual(product.attributes.color[0],	'blue');
			assert.deepStrictEqual(product.attributes.color[1],	'green');

			done();
		});
	});

	it('should alter an product already saved to db', function (done) {
		const	tasks	= [];

		tasks.push(function (cb) {
			const	product	= new productLib.Product(productUuid);

			product.loadFromDb(function (err) {
				if (err) throw err;

				product.attributes.boll = ['foo'];
				delete product.attributes.weight;

				product.save(function (err) {
					if (err) throw err;

					assert.deepStrictEqual(product.uuid,	productUuid);
					assert.deepStrictEqual(product.attributes.name,	['Test product #69']);
					assert.deepStrictEqual(product.attributes.price,	['99']);
					assert.deepStrictEqual(product.attributes.weight,	undefined);
					assert.deepStrictEqual(product.attributes.boll,	['foo']);
					product.attributes.color.sort();
					assert.deepStrictEqual(product.attributes.color,	['blue', 'green']);

					cb();
				});
			});
		});

		tasks.push(function (cb) {
			const	product	= new productLib.Product(productUuid);

			product.loadFromDb(function (err) {
				if (err) throw err;

				assert.deepStrictEqual(product.uuid,	productUuid);
				assert.deepStrictEqual(product.attributes.name,	['Test product #69']);
				assert.deepStrictEqual(product.attributes.price,	['99']);
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
			const	product	= new productLib.Product();

			product.attributes.foo	= 'bar';
			product.attributes.nisse	= 'mm';
			product.attributes.active	= 'true';
			product.attributes.bacon	= 'yes';
			product.save(cb);
		});
		tasks.push(function (cb) {
			const	product	= new productLib.Product();

			product.attributes.foo	= 'baz';
			product.attributes.nisse	= 'nej';
			product.attributes.active	= 'true';
			product.attributes.bacon	= 'no';
			product.save(cb);
		});
		tasks.push(function (cb) {
			const	product	= new productLib.Product();

			product.attributes.foo	= 'bar';
			product.attributes.active	= 'true';
			product.attributes.bacon	= 'narwhal';
			product.save(cb);
		});

		// Get all products before
		tasks.push(function (cb) {
			es.search({
				'index':	'larvitproduct',
				'type':	'product'
			}, function (err, result) {
				if (err) throw err;

				assert.strictEqual(result.hits.total,	4);

				cb();
			});
		});

		// Remove a product
		tasks.push(function (cb) {
			const	product	= new productLib.Product(productUuid);

			product.rm(cb);
		});

		// Refresh the index
		tasks.push(function (cb) {
			es.indices.refresh({'index': 'larvitproduct'}, cb);
		});

		// Get all products after
		tasks.push(function (cb) {
			es.search({
				'index':	'larvitproduct',
				'type':	'product'
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
			const	product	= new productLib.Product();

			product.attributes.enabled2	= 'true';
			product.attributes.enabled	= 'true';
			product.attributes.country	= 'all';
			product.attributes.country2	= 'all';
			product.save(cb);
		});

		tasks.push(function (cb) {
			const	product	= new productLib.Product();

			product.attributes.enabled2	= ['true', 'maybe'];
			product.attributes.enabled	= ['true', 'maybe'];
			product.attributes.country	= 'se';
			product.attributes.country2	= 'se';
			product.save(cb);
		});

		tasks.push(function (cb) {
			const	product	= new productLib.Product();

			product.attributes.enabled2	= 'false';
			product.attributes.enabled	= 'false';
			product.attributes.country	= 'se';
			product.attributes.country2	= 'se';
			product.save(cb);
		});

		tasks.push(function (cb) {
			const	product	= new productLib.Product();

			product.attributes.enabled2	= ['maybe', 'true'];
			product.attributes.enabled	= ['true', 'maybe'];
			product.attributes.country	= 'dk';
			product.attributes.country2	= 'dk';
			product.save(cb);
		});

		tasks.push(function (cb) {
			const	product	= new productLib.Product();

			product.attributes.enabled2	= ['maybe', 'true'];
			product.attributes.enabled	= ['true', 'maybe'];
			product.attributes.country	= 'all';
			product.attributes.country2	= 'se';
			product.save(cb);
		});

		async.parallel(tasks, function (err) {
			if (err) throw err;

			// Refresh the index
			es.indices.refresh({'index': 'larvitproduct'}, function (err) {
				if (err) throw err;
				done();
			});
		});
	});

	it('should get attribute values', function (done) {
		productLib.helpers.getAttributeValues('foo', function (err, result) {
			if (err) throw err;

			assert.deepStrictEqual(result,	['bar', 'baz']);
			done();
		});
	});

	it('should get empty array on non existing attribute name', function (done) {
		productLib.helpers.getAttributeValues('trams', function (err, result) {
			if (err) throw err;

			assert.deepStrictEqual(result,	[]);
			done();
		});
	});

	it('should ignore BOMs in strings', function (done) {
		const	product	= new productLib.Product();

		product.attributes[new Buffer('efbbbf70', 'hex').toString()]	= 'bulle';
		product.save(function (err) {
			if (err) throw err;

			es.get({
				'index':	'larvitproduct',
				'type':	'product',
				'id':	product.uuid
			}, function (err, result) {
				if (err) throw err;

				assert.deepStrictEqual(Object.keys(result._source), ['created', 'p']);

				done();
			});
		});
	});
});

describe('Import', function () {

	// Make sure the index is refreshed between each test
	beforeEach(function (done) {
		es.indices.refresh({'index': 'larvitproduct'}, done);
	});

	function importFromStr(str, options, cb) {
		const	tmpFile	= os.tmpdir() + '/tmp_products.csv',
			tasks	= [];

		let	uuids	= [];

		// First create our test file
		tasks.push(function (cb) {
			fs.writeFile(tmpFile, str, cb);
		});

		// Import file
		tasks.push(function (cb) {
			productLib.importer.fromFile(tmpFile, options, function (err, result) {
				uuids	= result;

				if (err) throw err;

				cb();
			});
		});

		// Remove tmp file
		tasks.push(function (cb) {
			fs.unlink(tmpFile, cb);
		});

		async.series(tasks, function (err) {
			cb(err, uuids);
		});
	}

	function getProductData(uuids, cb) {
		const	body	= {'body':{'docs':[]}};

		for (const uuid of uuids) {
			body.body.docs.push({'_index': 'larvitproduct', '_type': 'product', '_id': uuid});
		}

		es.mget(body, cb);
	}

	it('very simple test case', function (done) {
		const	productStr	= 'name,price,description\nball,100,it is round\ntv,55,"About 32"" in size"';

		importFromStr(productStr, {}, function (err, uuids) {
			if (err) throw err;

			assert.deepStrictEqual(uuids.length,	2);

			getProductData(uuids, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.docs.length,	2);

				for (let i = 0; testProducts[i] !== undefined; i ++) {
					const	product	= testProducts[i];

					assert.deepStrictEqual(Object.keys(product._source).length,	4);

					if (product._source.name[0] === 'ball') {
						assert.deepStrictEqual(product._source.price[0],	'100');
						assert.deepStrictEqual(product._source.description[0],	'it is round');
					} else if (product._source.name[0] === 'tv') {
						assert.deepStrictEqual(product._source.price[0],	'55');
						assert.deepStrictEqual(product._source.description[0],	'About 32" in size');
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}

				done();
			});
		});
	});

	it('Override static column data', function (done) {
		const	productStr	= 'name,artNo,size,enabled\nball,abc01,3,true\ntv,abc02,14,false\nspoon,abc03,2,true',
			options	= {'staticCols': { 'foul': 'nope', 'enabled': 'false'} };

		importFromStr(productStr, options, function (err, uuids) {
			if (err) throw err;

			assert.deepStrictEqual(uuids.length,	3);

			getProductData(uuids, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.docs.length,	3);

				for (let i = 0; testProducts.docs[i] !== undefined; i ++) {
					const	product	= testProducts.docs[i];

					assert.deepStrictEqual(Object.keys(product._source).length,	6);

					if (product._source.name[0] === 'ball') {
						assert.deepStrictEqual(product._source.artNo[0],	'abc01');
						assert.deepStrictEqual(product._source.size[0],	'3');
						assert.deepStrictEqual(product._source.enabled[0],	'true');
						assert.deepStrictEqual(product._source.foul[0],	'nope');
					} else if (product._source.name[0] === 'tv') {
						assert.deepStrictEqual(product._source.artNo[0],	'abc02');
						assert.deepStrictEqual(product._source.size[0],	'14');
						assert.deepStrictEqual(product._source.enabled[0],	'false');
						assert.deepStrictEqual(product._source.foul[0],	'nope');
					} else if (product._source.name[0] === 'spoon') {
						assert.deepStrictEqual(product._source.artNo[0],	'abc03');
						assert.deepStrictEqual(product._source.size[0],	'2');
						assert.deepStrictEqual(product._source.enabled[0],	'true');
						assert.deepStrictEqual(product._source.foul[0],	'nope');
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}
				done();
			});
		});
	});

	it('Replace by columns', function (done) {
		const	productStr	= 'name,artNo,size\nball,abc01,15\ntv,abc02,14\ncar,abc13,2',
			options	= {'replaceByCols': 'artNo'},
			tasks	= [];

		let	preNoProducts,
			uuids;

		// Count hits in index before
		tasks.push(function (cb) {
			es.count({'index': 'larvitproduct', 'type': 'product'}, function (err, result) {
				if (err) throw err;
				preNoProducts = result.count;
				cb();
			});
		});

		// Run the import
		tasks.push(function (cb) {
			importFromStr(productStr, options, function (err, result) {
				if (err) throw err;
				uuids = result;
				assert.deepStrictEqual(uuids.length,	3);
				cb();
			});
		});

		// Refresh index
		tasks.push(function (cb) {
			es.indices.refresh({'index': 'larvitproduct'}, cb);
		});

		// Count hits after index
		tasks.push(function (cb) {
			es.count({'index': 'larvitproduct', 'type': 'product'}, function (err, result) {
				if (err) throw err;
				assert.strictEqual(preNoProducts, (result.count - 1));
				cb();
			});
		});

		// Check product data
		tasks.push(function (cb) {
			getProductData(uuids, function (err, testProducts) {
				if (err) throw err;

				assert.strictEqual(testProducts.docs.length,	3);

				for (let i = 0; testProducts.docs[i] !== undefined; i ++) {
					const	product	= testProducts.docs[i];

					assert.deepStrictEqual(Object.keys(product._source).length,	4);

					if (product._source.name[0] === 'ball') {
						assert.deepStrictEqual(product._source.artNo[0],	'abc01');
						assert.deepStrictEqual(product._source.size[0],	'15');
					} else if (product._source.name[0] === 'tv') {
						assert.deepStrictEqual(product._source.artNo[0],	'abc02');
						assert.deepStrictEqual(product._source.size[0],	'14');
					} else if (product._source.name[0] === 'car') {
						assert.deepStrictEqual(product._source.artNo[0],	'abc13');
						assert.deepStrictEqual(product._source.size[0],	'2');
					} else {
						throw new Error('Unexpected product: ' + JSON.stringify(product));
					}
				}
				cb();
			});
		});

		async.series(tasks, done);
	});
});

after(function (done) {
	// Remove all data from elasticsearch
	es.indices.delete({'index': '*'}, done);
});
