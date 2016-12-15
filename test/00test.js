'use strict';

const	uuidValidate	= require('uuid-validate'),
	Intercom	= require('larvitamintercom'),
	uuidLib	= require('uuid'),
	assert	= require('assert'),
	lUtils	= require('larvitutils'),
	async	= require('async'),
	log	= require('winston'),
	db	= require('larvitdb'),
	fs	= require('fs');

let	productLib;

// Set up winston
log.remove(log.transports.Console);
/**/log.add(log.transports.Console, {
	'level':	'warn',
	'colorize':	true,
	'timestamp':	true,
	'json':	false
});/**/

before(function(done) {
	this.timeout(10000);
	const	tasks	= [];

	// Run DB Setup
	tasks.push(function(cb) {
		let confFile;

		if (process.env.DBCONFFILE === undefined) {
			confFile = __dirname + '/../config/db_test.json';
		} else {
			confFile = process.env.DBCONFFILE;
		}

		log.verbose('DB config file: "' + confFile + '"');

		// First look for absolute path
		fs.stat(confFile, function(err) {
			if (err) {

				// Then look for this string in the config folder
				confFile = __dirname + '/../config/' + confFile;
				fs.stat(confFile, function(err) {
					if (err) throw err;
					log.verbose('DB config: ' + JSON.stringify(require(confFile)));
					db.setup(require(confFile), cb);
				});

				return;
			}

			log.verbose('DB config: ' + JSON.stringify(require(confFile)));
			db.setup(require(confFile), cb);
		});
	});

	// Check for empty db
	tasks.push(function(cb) {
		db.query('SHOW TABLES', function(err, rows) {
			if (err) throw err;

			if (rows.length) {
				throw new Error('Database is not empty. To make a test, you must supply an empty database!');
			}

			cb();
		});
	});

	// Setup intercom
	tasks.push(function(cb) {
		let confFile;

		if (process.env.INTCONFFILE === undefined) {
			confFile = __dirname + '/../config/amqp_test.json';
		} else {
			confFile = process.env.INTCONFFILE;
		}

		log.verbose('Intercom config file: "' + confFile + '"');

		// First look for absolute path
		fs.stat(confFile, function(err) {
			if (err) {

				// Then look for this string in the config folder
				confFile = __dirname + '/../config/' + confFile;
				fs.stat(confFile, function(err) {
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

	// Preload caches etc
	// We do this so the timing of the rest of the tests gets more correct
	tasks.push(function(cb) {
		const	tasks	= [];

		productLib	= require(__dirname + '/../index.js');

		tasks.push(function(cb) {
			const	product	= new productLib.Product();
			product.ready(cb);
		});

		async.parallel(tasks, cb);
	});

	async.series(tasks, done);
});

describe('Product', function() {
	let	productUuid;

	it('should instantiate a new plain product object', function(done) {
		const product = new productLib.Product();

		assert.deepEqual(toString.call(product),	'[object Object]');
		assert.deepEqual(toString.call(product.attributes),	'[object Object]');
		assert.deepEqual(uuidValidate(product.uuid, 1),	true);
		assert.deepEqual(toString.call(product.created),	'[object Date]');

		done();
	});

	it('should instantiate a new plain product object, with empty object as option', function(done) {
		const product = new productLib.Product({});

		assert.deepEqual(toString.call(product),	'[object Object]');
		assert.deepEqual(toString.call(product.attributes),	'[object Object]');
		assert.deepEqual(uuidValidate(product.uuid, 1),	true);
		assert.deepEqual(toString.call(product.created),	'[object Date]');

		done();
	});

	it('should instantiate a new plain product object, with custom uuid', function(done) {
		const product = new productLib.Product('6a7c9adc-9b73-11e6-9f33-a24fc0d9649c');

		product.loadFromDb(function(err) {
			if (err) throw err;

			assert.deepEqual(toString.call(product),	'[object Object]');
			assert.deepEqual(toString.call(product.attributes),	'[object Object]');
			assert.deepEqual(uuidValidate(product.uuid, 1),	true);
			assert.deepEqual(product.uuid,	'6a7c9adc-9b73-11e6-9f33-a24fc0d9649c');
			assert.deepEqual(toString.call(product.created),	'[object Date]');

			done();
		});
	});

	it('should instantiate a new plain product object, with custom uuid as explicit option', function(done) {
		const product = new productLib.Product({'uuid': '6a7c9adc-9b73-11e6-9f33-a24fc0d9649c'});

		product.loadFromDb(function(err) {
			if (err) throw err;

			assert.deepEqual(toString.call(product),	'[object Object]');
			assert.deepEqual(toString.call(product.attributes),	'[object Object]');
			assert.deepEqual(uuidValidate(product.uuid, 1),	true);
			assert.deepEqual(product.uuid,	'6a7c9adc-9b73-11e6-9f33-a24fc0d9649c');
			assert.deepEqual(toString.call(product.created),	'[object Date]');

			done();
		});
	});

	it('should instantiate a new plain product object, with custom created', function(done) {
		const	manCreated	= new Date(),
			product	= new productLib.Product({'created': manCreated});

		product.loadFromDb(function(err) {
			if (err) throw err;

			assert.deepEqual(toString.call(product),	'[object Object]');
			assert.deepEqual(toString.call(product.attributes),	'[object Object]');
			assert.deepEqual(uuidValidate(product.uuid, 1),	true);
			assert.deepEqual(product.created,	manCreated);

			done();
		});
	});

	it('should save a product', function(done) {
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
			const	dbFields	=	[lUtils.uuidToBuffer(productUuid)],
				sql	=	'SELECT *\n' +
						'FROM product_product_attributes\n' +
						'	JOIN product_attributes ON uuid = attributeUuid\n' +
						'WHERE productUuid = ?';

			db.query(sql, dbFields, function(err, rows) {
				if (err) throw err;

				assert.deepEqual(rows.length,	5);

				for (let i = 0; rows[i] !== undefined; i ++) {
					const	row = rows[i];

					if (
							(row.name === 'name'	&& row.data === 'Test product #69')
						||	(row.name === 'price'	&& row.data === '99')
						||	(row.name === 'weight'	&& row.data === '14')
						||	(row.name === 'color'	&& (row.data === 'blue' || row.data === 'green'))
					) {
						// Pass!
					} else {
						throw new Error('Invalid row: ' + JSON.stringify(row));
					}
				}

				cb(err);
			});
		}

		async.series([createProduct, checkProduct], function(err) {
			if (err) throw err;
			done();
		});
	});

	it('should load saved product from db', function(done) {
		const product = new productLib.Product(productUuid);

		product.loadFromDb(function(err) {
			if (err) throw err;

			assert.deepEqual(product.uuid,	productUuid);
			assert.deepEqual(product.attributes.name[0],	'Test product #69');
			assert.deepEqual(product.attributes.price[0],	'99');
			assert.deepEqual(product.attributes.weight[0],	'14');
			product.attributes.color.sort();
			assert.deepEqual(product.attributes.color[0],	'blue');
			assert.deepEqual(product.attributes.color[1],	'green');

			done();
		});
	});

	it('should alter an product already saved to db', function(done) {
		const	tasks	= [];

		tasks.push(function(cb) {
			const	product	= new productLib.Product(productUuid);

			product.loadFromDb(function(err) {
				if (err) throw err;

				product.attributes.boll = ['foo'];
				delete product.attributes.weight;

				product.save(function(err) {
					if (err) throw err;

					assert.deepEqual(product.uuid,	productUuid);
					assert.deepEqual(product.attributes.name,	['Test product #69']);
					assert.deepEqual(product.attributes.price,	['99']);
					assert.deepEqual(product.attributes.weight,	undefined);
					assert.deepEqual(product.attributes.boll,	['foo']);
					product.attributes.color.sort();
					assert.deepEqual(product.attributes.color,	['blue', 'green']);

					cb();
				});
			});
		});

		tasks.push(function(cb) {
			const	product	= new productLib.Product(productUuid);

			product.loadFromDb(function(err) {
				if (err) throw err;

				assert.deepEqual(product.uuid,	productUuid);
				assert.deepEqual(product.attributes.name,	['Test product #69']);
				assert.deepEqual(product.attributes.price,	['99']);
				assert.deepEqual(product.attributes.weight,	undefined);
				assert.deepEqual(product.attributes.boll,	['foo']);
				product.attributes.color.sort();
				assert.deepEqual(product.attributes.color,	['blue', 'green']);

				cb();
			});
		});

		async.series(tasks, done);
	});

	it('should remove a product', function(done) {
		const	tasks	= [];

		let	prevCount;

		// Add some more products
		tasks.push(function(cb) {
			const	product	= new productLib.Product();

			product.attributes.foo	= 'bar';
			product.attributes.nisse	= 'mm';
			product.attributes.active	= 'true';
			product.save(cb);
		});
		tasks.push(function(cb) {
			const	product	= new productLib.Product();

			product.attributes.foo	= 'baz';
			product.attributes.nisse	= 'nej';
			product.attributes.active	= 'true';
			product.save(cb);
		});
		tasks.push(function(cb) {
			const	product	= new productLib.Product();

			product.attributes.foo	= 'bar';
			product.attributes.active	= 'true';
			product.save(cb);
		});

		// Get all products before
		tasks.push(function(cb) {
			const	products	= new productLib.Products();

			products.get(function(err, result) {
				prevCount	= Object.keys(result).length;
				assert.notDeepEqual(Object.keys(result).indexOf(productUuid), - 1);
				cb();
			});
		});

		// Remove a product
		tasks.push(function(cb) {
			const	product	= new productLib.Product(productUuid);

			product.rm(cb);
		});

		// Get all products after
		tasks.push(function(cb) {
			const	products	= new productLib.Products();

			products.get(function(err, result) {
				assert.deepEqual(Object.keys(result).length, (prevCount - 1));
				assert.deepEqual(Object.keys(result).indexOf(productUuid), - 1);
				cb();
			});
		});

		async.series(tasks, function(err) {
			if (err) throw err;
			done();
		});
	});
});

describe('Products', function() {
	let	dbUuids	= [];

	// Since we've created products above, they should turn up here
	it('should get a list of products', function(done) {
		const products = new productLib.Products();

		products.get(function(err, productList, productsCount) {
			if (err) throw err;
			assert.deepEqual(typeof productList,	'object');
			assert.deepEqual(Object.keys(productList).length,	3);
			assert.deepEqual(productsCount,	3);

			for (const uuid of Object.keys(productList)) {
				assert.deepEqual(uuidValidate(productList[uuid].uuid, 1),	true);
				assert.deepEqual(toString.call(productList[uuid].created),	'[object Date]');
			}

			done();
		});
	});

	it('should get products by uuids', function(done) {
		const tasks = [];

		// Get all uuids in db
		tasks.push(function(cb) {
			const products = new productLib.Products();

			products.get(function(err, productList, productsCount) {
				if (err) throw err;

				dbUuids = Object.keys(productList);
				assert.deepEqual(dbUuids.length,	productsCount);

				cb();
			});
		});

		// Get by first uuid
		tasks.push(function(cb) {
			const products = new productLib.Products();

			products.uuids = dbUuids[0];

			products.get(function(err, productList, productsCount) {
				if (err) throw err;
				assert.deepEqual(typeof productList,	'object');
				assert.deepEqual(Object.keys(productList).length,	1);
				assert.deepEqual(productsCount,	1);
				assert.deepEqual(uuidValidate(productList[dbUuids[0]].uuid, 1),	true);
				assert.deepEqual(productList[dbUuids[0]].uuid,	dbUuids[0]);
				assert.deepEqual(toString.call(productList[dbUuids[0]].created),	'[object Date]');

				cb();
			});
		});

		// Get 0 results for wrong uuids
		tasks.push(function(cb) {
			const products = new productLib.Products();

			products.uuids = uuidLib.v1();

			products.get(function(err, productList, productsCount) {
				if (err) throw err;
				assert.deepEqual(typeof productList,	'object');
				assert.deepEqual(Object.keys(productList).length,	0);
				assert.deepEqual(productsCount,	0);

				cb();
			});
		});

		// Get 0 results for no uuids (empty array)
		tasks.push(function(cb) {
			const products = new productLib.Products();

			products.uuids = [];

			products.get(function(err, productList, productsCount) {
				if (err) throw err;
				assert.deepEqual(typeof productList,	'object');
				assert.deepEqual(Object.keys(productList).length,	0);
				assert.deepEqual(productsCount,	0);

				cb();
			});
		});

		// get 2 results for two uuids
		tasks.push(function(cb) {
			const products = new productLib.Products();

			products.uuids = [dbUuids[0], dbUuids[2]];

			products.get(function(err, productList, productsCount) {
				if (err) throw err;
				assert.deepEqual(typeof productList,	'object');
				assert.deepEqual(Object.keys(productList).length,	2);
				assert.deepEqual(productsCount,	2);

				assert.deepEqual(uuidValidate(productList[dbUuids[0]].uuid, 1),	true);
				assert.deepEqual(productList[dbUuids[0]].uuid,	dbUuids[0]);
				assert.deepEqual(toString.call(productList[dbUuids[0]].created),	'[object Date]');

				assert.deepEqual(uuidValidate(productList[dbUuids[2]].uuid, 1),	true);
				assert.deepEqual(productList[dbUuids[2]].uuid,	dbUuids[2]);
				assert.deepEqual(toString.call(productList[dbUuids[2]].created),	'[object Date]');

				cb();
			});
		});

		async.series(tasks, done);
	});

	it('should get products with limits', function(done) {
		const products = new productLib.Products();

		products.limit = 2;

		products.get(function(err, productList, productsCount) {
			if (err) throw err;
			assert.deepEqual(typeof productList,	'object');
			assert.deepEqual(Object.keys(productList).length,	2);
			assert.deepEqual(productsCount,	3);

			done();
		});
	});

	it('should get products with limit and offset', function(done) {
		const products = new productLib.Products();

		products.limit	= 2;
		products.offset	= 2;

		products.get(function(err, productList, productsCount) {

			if (err) throw err;
			assert.deepEqual(typeof productList,	'object');

			// Since there are only 3 rows in the database, a single row should be returned
			assert.deepEqual(Object.keys(productList).length,	1);
			assert.deepEqual(productsCount,	3);

			done();
		});
	});

	describe('should get products based on attributes', function() {
		it('multiple attributes with values', function(done) {
			const	products	= new productLib.Products();

			products.matchAllAttributes = {
				'active':	'true',
				'nisse':	'nej'
			};

			products.get(function(err, productList, productsCount) {
				if (err) throw err;

				assert.deepEqual(Object.keys(productList).length,	1);
				assert.deepEqual(productsCount,	1);

				done();
			});
		});

		it('multiple attributes, one with value the other without', function(done) {
			const	products	= new productLib.Products();

			products.matchAllAttributes = {
				'active':	'true',
				'nisse':	undefined
			};

			products.get(function(err, productList, productsCount) {
				if (err) throw err;

				assert.deepEqual(Object.keys(productList).length,	2);
				assert.deepEqual(productsCount,	2);

				done();
			});
		});
	});

	describe('should get products and their attributes', function() {

		// Get all products and all attributes
		it('should get a list of products and all their attributes', function(done) {
			const products = new productLib.Products();

			products.returnAllAttributes = true;

			products.get(function(err, productList, productsCount) {
				if (err) throw err;

				assert.deepEqual(typeof productList,	'object');
				assert.deepEqual(Object.keys(productList).length,	3);
				assert.deepEqual(productsCount,	3);

				for (const uuid of Object.keys(productList)) {
					const	product	= productList[uuid];

					assert.deepEqual(uuidValidate(product.uuid, 1),	true);
					assert.deepEqual(toString.call(product.created),	'[object Date]');
					assert.deepEqual(typeof product.attributes,	'object');
					assert.deepEqual(product.attributes.active,	['true']);

					if (JSON.stringify(product.attributes.nisse) === JSON.stringify(['mm'])) {
						assert.deepEqual(product.attributes.foo,	['bar']);
						assert.deepEqual(Object.keys(product.attributes).length,	3);
					} else if (JSON.stringify(product.attributes.nisse) === JSON.stringify(['nej'])) {
						assert.deepEqual(product.attributes.foo,	['baz']);
						assert.deepEqual(Object.keys(product.attributes).length,	3);
					} else {
						assert.deepEqual(product.attributes.foo,	['bar']);
						assert.deepEqual(Object.keys(product.attributes).length,	2);
					}

				}

				done();
			});
		});

		// Get all products and foo and active attributes
		it('should get a list of products and the foo and active attributes', function(done) {
			const products = new productLib.Products();

			products.returnAttributes = ['foo', 'active'];

			products.get(function(err, productList, productsCount) {
				let	bar	= 0,
					baz	= 0;

				if (err) throw err;

				assert.deepEqual(typeof productList,	'object');
				assert.deepEqual(Object.keys(productList).length,	3);
				assert.deepEqual(productsCount,	3);

				for (const uuid of Object.keys(productList)) {
					const	product	= productList[uuid];

					assert.deepEqual(uuidValidate(product.uuid, 1),	true);
					assert.deepEqual(toString.call(product.created),	'[object Date]');
					assert.deepEqual(typeof product.attributes,	'object');
					assert.deepEqual(product.attributes.active,	['true']);
					assert.deepEqual(Object.keys(product.attributes).length,	2);

					if (JSON.stringify(product.attributes.foo) === JSON.stringify(['baz'])) {
						baz ++;
					} else if (JSON.stringify(product.attributes.foo) === JSON.stringify(['bar'])) {
						bar ++;
					}
				}

				assert.deepEqual(baz,	1);
				assert.deepEqual(bar, 2);

				done();
			});
		});

	});

	describe('should be able to search', function() {
		it('should be able to search for a product', function(done) {
			const attributes = {
				'name':	'Searchable product #1',
				'price':	959,
				'weight':	111214
			};

			// Create a product to search for.
			function createProduct(cb) {
				const product = new productLib.Product();
				product.attributes = attributes;
				product.save(cb);
			}

			// Search for product.
			function searchProduct(cb) {
				const products = new productLib.Products();

				products.searchString = 'searchable';
				products.returnAttributes = ['name', 'price', 'weight'];

				products.get(function(err, result) {
					for (const uuid of Object.keys(result)) {
						assert.deepEqual(attributes.name, result[uuid].attributes.name[0]);
						assert.deepEqual(attributes.price, result[uuid].attributes.price[0]);
						assert.deepEqual(attributes.weight, result[uuid].attributes.weight[0]);
					};
					cb();
				});
			}

			async.series([createProduct, searchProduct], function(err) {
				if (err) throw err;
				done();
			});
		});
	});
});

describe('Helpers', function() {
	it('should get attribute values', function(done) {
		productLib.helpers.getAttributeValues('foo', function(err, result) {
			if (err) throw err;

			assert.deepEqual(result,	['bar', 'baz']);
			done();
		});
	});

	it('should get empty array on non existing attribute name', function(done) {
		productLib.helpers.getAttributeValues('trams', function(err, result) {
			if (err) throw err;

			assert.deepEqual(result,	[]);
			done();
		});
	});

	it('should get an attribute uuid', function(done) {
		productLib.helpers.getAttributeUuidBuffer('foo', function(err, uuid) {
			if (err) throw err;

			assert.deepEqual(uuidValidate(lUtils.formatUuid(uuid), 1), true);

			done();
		});
	});

	it('should not create duplicate attribute names', function(done) {
		productLib.helpers.getAttributeUuidBuffers(['lurt', 'flams', 'lurt', 'annat', 'lurt'], function(err, uuids) {
			if (err) throw err;

			for (const attributeName of Object.keys(uuids)) {
				assert.deepEqual(uuidValidate(lUtils.formatUuid(uuids[attributeName]), 1), true);
			}

			done();
		});
	});
});

after(function(done) {
	db.removeAllTables(done);
});
