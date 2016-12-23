'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	dataWriter	= require(__dirname + '/dataWriter.js'),
	lUtils	= require('larvitutils'),
	async	= require('async'),
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

	// dataWriter handes database migrations etc, make sure its run first
	tasks.push(function(cb) {
		dataWriter.ready(cb);
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
		let	countSql	= 'SELECT COUNT(*) AS products FROM product_products products WHERE 1',
			sql	= 'SELECT * FROM product_products products WHERE 1';

		that.generateWhere(function(where, dbFields) {
			where	+= ' ORDER BY created DESC';
			countSql	+= where;
			sql 	+= where;

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
		sql += ' productUuid IN (';

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

Products.prototype.getUniqeAttributes = function(filters, cb) {
	const	attributes	= {},
		tasks	= [],
		that	= this;

	tasks.push(ready);

	tasks.push(function(cb) {
		let	sql;
		sql	=	'SELECT DISTINCT product_attributes.name, product_product_attributes.data\n';
		sql	+=	'FROM product_products as products\n';
		sql	+=	'JOIN product_product_attributes ON products.uuid = product_product_attributes.productUuid\n';
		sql	+=	'JOIN product_attributes ON product_product_attributes.attributeUuid = product_attributes.uuid WHERE 1\n';

		that.generateWhere(function(where, dbFields) {
			sql 	+= where;

			if (filters.length > 0) {
				sql += ' AND (';
				for (let i = 0; filters[i] !== undefined; i ++) {
					sql += ' product_attributes.name = ? OR';
					dbFields.push(filters[i]);
				}
				sql = sql.substring(0, sql.length - 3);
				sql += ')';
			}

			ready(function() {
				db.query(sql, dbFields, function(err, rows) {
					for (let i = 0; rows[i] !== undefined; i ++) {
						if (attributes[rows[i].name] == undefined) {
							attributes[rows[i].name] = [rows[i].data];
						} else {
							attributes[rows[i].name].push(rows[i].data);
						}
					}
					cb();
				});
			});
		});
	});

	async.series(tasks, function(err) {
		if (err) { cb(err); return; }
		cb(null, attributes);
	});
};

Products.prototype.generateWhere = function(cb) {
	const	dbFields	= [],
		that	= this;

	let sql = '';

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

			if (Array.isArray(attributeValue)) {
				for (let i = 0; attributeValue[i] !== undefined; i ++) {
					if (i === 0) {
						sql += '	AND ( products.uuid IN (\n';
					} else {
						sql += '	OR products.uuid IN (\n';
					}

					sql += '		SELECT DISTINCT productUuid\n';
					sql += '		FROM product_product_attributes\n';

					dbFields.push(attributeName);
					if (attributeValue[i] === undefined) {
						sql += '		WHERE attributeUuid = (SELECT uuid FROM product_attributes WHERE name = ?)\n';
					} else {
						sql += '		WHERE attributeUuid = (SELECT uuid FROM product_attributes WHERE name = ?) AND `data` = ?\n';
						dbFields.push(attributeValue[i]);
					}
					sql += ')';
				}
				sql += ')';
			} else {
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
	}

	if (that.searchString !== undefined && that.searchString !== '') {
		sql += '	AND products.uuid IN (\n';
		sql += '		SELECT DISTINCT productUuid\n';
		sql += '		FROM product_product_attributes\n';
		sql += ' WHERE data LIKE ?)\n';

		dbFields.push('%' + that.searchString.trim() + '%');
	}

	cb(sql, dbFields);
};

exports = module.exports = Products;
