'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	dataWriter	= require(__dirname + '/dataWriter.js'),
	lUtils	= require('larvitutils'),
	async	= require('async'),
	db	= require('larvitdb');

let	readyInProgress	= false,
	isReady	= false,
	intercom;

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

	// Set intercom after dataWriter is ready
	tasks.push(function(cb) {
		intercom	= require('larvitutils').instances.intercom;
		cb();
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

Products.prototype.generateWhere = function(cb) {
	const	dbFields	= [],
		that	= this;

	let sql = '';

	if (that.uuids !== undefined) {
		if ( ! (that.uuids instanceof Array)) {
			that.uuids = [that.uuids];
		}

		for (let i = 0; that.uuids[i] !== undefined; i ++) {
			if ( ! lUtils.uuidToBuffer(that.uuids[i])) {
				that.uuids = that.uuids.slice(i, 0);
			}
		}

		if (that.uuids.length === 0) {
			sql += '	AND 0';
		} else {
			sql += '	AND p.uuid IN (';

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
						sql += '	AND (p.uuid IN (\n';
					} else {
						sql += '	OR p.uuid IN (\n';
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
				sql += '	AND p.uuid IN (\n';
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

	if (that.matchAnyAttribute !== undefined) {

		if (that.matchAllAttributes === undefined) {
			sql += ' AND (\n';
		} else {
			sql += ' OR (\n';
		}

		for (const attributeName of Object.keys(that.matchAnyAttribute)) {
			const	attributeValue	= that.matchAnyAttribute[attributeName];

			sql += '	OR (p.uuid IN (\n';
			sql += '		SELECT DISTINCT productUuid\n';
			sql += '		FROM product_product_attributes\n';
			sql += '		WHERE attributeUuid = (SELECT uuid FROM product_attributes WHERE name = ?)\n';

			dbFields.push(attributeName);

			if (Array.isArray(attributeValue) && attributeValue[0] !== undefined) {

				sql += '	AND (';

				for (let i = 0; attributeValue[i] !== undefined; i ++) {

					if (i > 0) {
						sql += ' OR ';
					}

					sql += '		`data` = ?\n';
					dbFields.push(attributeValue[i]);
				}	
				
				sql += '	)';
				
			} else if (attributeValue !== undefined) {
				sql += '	AND `data` = ?\n';
				dbFields.push(attributeValue);
			}

			sql += '))';
		}

		sql += ')';
	}

	if (that.searchString !== undefined && that.searchString !== '') {
		sql += '	AND p.uuid IN (\n';
		sql += '		SELECT DISTINCT productUuid\n';
		sql += '		FROM product_product_attributes\n';
		sql += ' WHERE data LIKE ?)\n';

		dbFields.push('%' + that.searchString.trim() + '%');
	}

	cb(null, sql, dbFields);
};

Products.prototype.get = function(cb) {
	const	tasks	= [],
		that	= this;

	let	productsCount	= 0,
		products	= {};

	// Make sure database is ready
	tasks.push(ready);

	// Get basic products
	tasks.push(function(cb) {
		let	countSql	= 'SELECT COUNT(*) AS products FROM product_products p WHERE 1',
			sql	= 'SELECT * FROM product_products p WHERE 1';

		that.generateWhere(function(err, where, dbFields) {
			const	tasks	= [];

			if (err) { cb(err); return; }

			where	+= ' ORDER BY created DESC';
			countSql	+= where;
			sql 	+= where;

			if (that.limit) {
				sql += ' LIMIT ' + parseInt(that.limit);
				if (that.offset) {
					sql += ' OFFSET ' + parseInt(that.offset);
				}
			}

			tasks.push(function(cb) {

				if (dbFields.indexOf('no') > -1) {
					console.log(sql);
				}

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

	if (typeof filters === 'function') {
		cb	= filters;
		filters	= undefined;
	}

	if (Array.isArray(filters) && filters.length === 0) {
		cb(null, {});
		return;
	}

	tasks.push(ready);

	tasks.push(function(cb) {
		let	sql;

		sql	=	'SELECT DISTINCT pa.name, ppa.data\n';
		sql	+=	'FROM product_products AS p\n';
		sql	+=	'	JOIN product_product_attributes	AS ppa	ON p.uuid	= ppa.productUuid\n';
		sql	+=	'	JOIN product_attributes	AS pa	ON ppa.attributeUuid	= pa.uuid\n';
		sql	+=	'WHERE 1\n';

		that.generateWhere(function(err, where, dbFields) {
			if (err) { cb(err); return; }

			sql 	+= where;

			if (filters !== undefined) {
				sql += ' AND (';
				for (let i = 0; filters[i] !== undefined; i ++) {
					sql += ' pa.uuid = ? OR';
					dbFields.push(lUtils.uuidToBuffer(filters[i]));
				}
				sql = sql.substring(0, sql.length - 3);
				sql += ')\n';
			}

			sql += 'ORDER BY pa.name, ppa.data;';

			db.query(sql, dbFields, function(err, rows) {
				if (err) { cb(err); return; }

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

	async.series(tasks, function(err) {
		if (err) { cb(err); return; }
		cb(null, attributes);
	});
};

Products.prototype.getUuids = function(cb) {
	const	tasks	= [],
		uuids	= [],
		that	= this;

	// Make sure database is ready
	tasks.push(ready);

	// Get uuids
	tasks.push(function(cb) {
		let	sql	= 'SELECT uuid FROM product_products p WHERE 1';

		that.generateWhere(function(err, where, dbFields) {
			if (err) { cb(err); return; }

			sql 	+= where;

			db.query(sql, dbFields, function(err, rows) {
				if (err) { cb(err); return; }

				for (let i = 0; rows[i] !== undefined; i ++) {
					uuids.push(lUtils.formatUuid(rows[i].uuid));
				}

				cb();
			});
		});
	});

	async.series(tasks, function(err) {
		if (err) { cb(err); return; }

		cb(err, uuids);
	});
};

Products.prototype.rm = function(cb) {
	const	tasks	= [],
		that	= this;

	let	uuids;

	// Get uuids
	tasks.push(function(cb) {
		that.getUuids(function(err, result) {
			uuids = result;
			cb(err);
		});
	});

	// Send the message to the queue
	tasks.push(function(cb) {
		const	options	= {'exchange': dataWriter.exchangeName},
			message	= {};

		message.action	= 'rmProducts';
		message.params	= {'uuids': uuids};

		intercom.send(message, options, function(err, msgUuid) {
			if (err) { cb(err); return; }

			dataWriter.emitter.once(msgUuid, cb);
		});
	});

	async.series(tasks, function(err) {
		if (err) { cb(err); return; }

		cb(null, uuids.length);
	});
};

Products.prototype.setAttribute = function(name, value, cb) {
	const	tasks	= [],
		that	= this;

	let	uuids;

	// Get uuids
	tasks.push(function(cb) {
		that.getUuids(function(err, result) {
			uuids = result;
			cb(err);
		});
	});

	// Send the message to the queue
	tasks.push(function(cb) {
		const	options	= {'exchange': dataWriter.exchangeName},
			message	= {};

		message.action	= 'setAttribute';
		message.params	= {};
		message.params.productUuids	= uuids;
		message.params.attributeName	= name;
		message.params.attributeValue	= value;

		intercom.send(message, options, function(err, msgUuid) {
			if (err) { cb(err); return; }

			dataWriter.emitter.once(msgUuid, cb);
		});
	});

	async.series(tasks, function(err) {
		if (err) { cb(err); return; }

		cb(null, uuids.length);
	});
};

exports = module.exports = Products;
