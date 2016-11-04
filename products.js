'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	dbmigration	= require('larvitdbmigration')({'tableName': 'product_db_version', 'migrationScriptsPath': __dirname + '/dbmigration'}),
	lUtils	= require('larvitutils'),
	async	= require('async'),
	log	= require('winston'),
	db	= require('larvitdb');

let	readyInProgress	= false,
	isReady	= false;

function ready(cb) {
	const	tasks	= [];

	if (isReady === true) { cb(); return; }

	if (readyInProgress === true) {
		eventEmitter.on('ready', cb);
		return;
	}

	readyInProgress = true;

	// Migrate database
	tasks.push(function(cb) {
		dbmigration(function(err) {
			if (err) {
				log.error('larvitproduct: products.js: Database error: ' + err.message);
				return;
			}

			cb();
		});
	});

	async.series(tasks, function() {
		isReady	= true;
		eventEmitter.emit('ready');
		cb();
	});
}

function Products() {
	this.ready	= ready;
}

Products.prototype.get = function(cb) {
	const	tasks	= [],
		that	= this;

	let	productsCount	= 0,
		products	= {};

	// Make sure database is ready
	tasks.push(ready);

	// Get basic products
	tasks.push(function(cb) {
		const dbFields = [];

		let	countSql	= 'SELECT COUNT(*) AS products',
			sql	= ' FROM product_products products WHERE 1';

		if (that.uuids !== undefined) {
			if ( ! (that.uuids instanceof Array)) {
				that.uuids = [that.uuids];
			}

			if (that.uuids.length === 0) {
				sql += '	AND 0';
			} else {
				sql += '	AND products.uuid IN (';

				for (let i = 0; that.uuids[i] !== undefined; i ++) {
					sql += '?,';
					dbFields.push(lUtils.uuidToBuffer(that.uuids[i]));
				}

				sql = sql.substring(0, sql.length - 1) + ')';
			}
		}

		if (that.matchAllAttributes !== undefined) {
			for (const attributeName of Object.keys(that.matchAllAttributes)) {
				const	attributeValue	= that.matchAllAttributes[attributeName];

				sql += '	AND products.uuid IN (\n';
				sql += '		SELECT DISTINCT productUuid\n';
				sql += '		FROM product_product_attributes\n';

				dbFields.push(attributeName);
				if (attributeValue === undefined) {
					sql += '		WHERE attributeUuid = (SELECT uuid FROM product_attributes WHERE name = ?)\n';
				} else {
					sql += '		WHERE attributeUuid = (SELECT uuid FROM product_attributes WHERE name = ?) AND `data` = ?\n';
					dbFields.push(attributeValue);
				}

				sql += ')';
			}
		}

		countSql	+= sql;
		sql	= 'SELECT *' + sql;

		sql += '	ORDER BY created DESC';

		if (that.limit) {
			sql += ' LIMIT ' + parseInt(that.limit);
			if (that.offset) {
				sql += ' OFFSET ' + parseInt(that.offset);
			}
		}

		ready(function() {
			const	tasks	= [];

			tasks.push(function(cb) {
				db.query(sql, dbFields, function(err, rows) {
					if (err) { cb(err); return; }

					for (let i = 0; rows[i] !== undefined; i ++) {
						const	row	= rows[i],
							productUuid	= lUtils.formatUuid(row.uuid);

						products[productUuid]	= {};
						products[productUuid].uuid	= productUuid;
						products[productUuid].created	= row.created;
					}

					cb();
				});
			});

			tasks.push(function(cb) {
				db.query(countSql, dbFields, function(err, rows) {
					if (err) { cb(err); return; }

					productsCount = rows[0].products;
					cb();
				});
			});

			async.parallel(tasks, cb);
		});
	});

	// Get fields
	tasks.push(function(cb) {
		const dbFields = [];

		let sql;

		if ((that.returnAllAttributes !== true && ! that.returnAttributes) || Object.keys(products).length === 0) {
			cb();
			return;
		}

		sql =  'SELECT productUuid, name AS attributeName, `data`\n';
		sql += 'FROM product_product_attributes JOIN product_attributes ON attributeUuid = uuid\n';
		sql += 'WHERE\n';
		sql += '	productUuid IN (';

		for (const productUuid of Object.keys(products)) {
			sql += '?,';
			dbFields.push(lUtils.uuidToBuffer(productUuid));
		}

		sql = sql.substring(0, sql.length - 1) + ')\n';

		if (that.returnAllAttributes !== true) {
			sql += '	AND name IN (';

			for (let i = 0; that.returnAttributes[i] !== undefined; i ++) {
				sql += '?,';
				dbFields.push(that.returnAttributes[i]);
			}

			sql = sql.substring(0, sql.length - 1) + ')\n';
		}

		db.query(sql, dbFields, function(err, rows) {
			if (err) { cb(err); return; }

			for (let i = 0; rows[i] !== undefined; i ++) {
				const row = rows[i];

				row.productUuid = lUtils.formatUuid(row.productUuid);

				if (products[row.productUuid].attributes === undefined) {
					products[row.productUuid].attributes = {};
				}

				if (products[row.productUuid].attributes[row.attributeName] === undefined) {
					products[row.productUuid].attributes[row.attributeName] = [];
				}

				products[row.productUuid].attributes[row.attributeName].push(row.data);
			}

			cb();
		});
	});

	async.series(tasks, function(err) {
		if (err) { cb(err); return; }

		cb(null, products, productsCount);
	});

};

exports = module.exports = Products;
