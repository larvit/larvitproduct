'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	dataWriter	= require(__dirname + '/dataWriter.js'),
	helpers	= require(__dirname + '/helpers.js'),
	lUtils	= require('larvitutils'),
	async	= require('async'),
	db	= require('larvitdb');

let	readyInProgress	= false,
	isReady	= false,
	intercom;

function escapeAttributeName(name, arr) {
	if ( ! arr) {
		arr = [];
	}

	return '`' + name.replace(/`/g, '_') + '_____' + arrCount(arr, name) + '`';
}

function arrCount(arr, key) {
	let	counter	= 0;
	for (let i = 0; arr[i] !== undefined; i ++) {
		if (arr[i] === key) counter ++;
	}

	return counter;
}

function ready(cb) {
	const	tasks	= [];

	if (isReady === true) { cb(); return; }

	if (readyInProgress === true) {
		eventEmitter.on('ready', cb);
		return;
	}

	readyInProgress = true;

	// dataWriter handes database migrations etc, make sure its run first
	tasks.push(function (cb) {
		dataWriter.ready(cb);
	});

	// Set intercom after dataWriter is ready
	tasks.push(function (cb) {
		intercom	= require('larvitutils').instances.intercom;
		cb();
	});

	async.series(tasks, function () {
		isReady	= true;
		eventEmitter.emit('ready');
		cb();
	});
}

function Products() {
	this.ready	= ready;
}

Products.prototype.generateWhere = function (cb) {
	const	joinedAttrs	= [],
		attrValues	= [],
		attrNames	= [],
		dbFields	= [],
		that	= this;

	let	whereSql	= '	1\n',
		joinSql	= '';

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
			whereSql += '	AND 0\n';
		} else {
			whereSql += '	AND p.uuid IN (';

			for (let i = 0; that.uuids[i] !== undefined; i ++) {
				whereSql += '?,';
				dbFields.push(lUtils.uuidToBuffer(that.uuids[i]));
			}

			whereSql = whereSql.substring(0, whereSql.length - 1) + ')\n';
		}
	}

	if (that.matchAllAttributes !== undefined) {
		for (const attributeName of Object.keys(that.matchAllAttributes)) {
			let	attributeValues	= that.matchAllAttributes[attributeName];

			if ( ! Array.isArray(attributeValues)) {
				attributeValues = [attributeValues];
			}

			for (const attributeValue of attributeValues) {
				const	escapedAttributeName	= escapeAttributeName(attributeName, joinedAttrs);

				joinedAttrs.push(attributeName);
				joinSql += '	LEFT JOIN product_product_attributes AS ' + escapedAttributeName + '\n';
				joinSql += '		ON	' + escapedAttributeName + '.productUuid	= p.uuid\n';
				joinSql += '		AND	' + escapedAttributeName + '.attributeUuid	= (SELECT uuid FROM product_attributes WHERE name = ?)\n';
				attrNames.push(attributeName);

				if (attributeValue === undefined) {
					whereSql += '	AND ' + escapedAttributeName + '.data IS NOT NULL\n';
				} else {
					whereSql += '	AND ' + escapedAttributeName + '.data = ?\n';
					attrValues.push(attributeValue);
				}
			}
		}
	}

	if (that.matchAnyAttribute !== undefined) {
		let	first	= true;

		whereSql += ' AND (\n';

		for (const attributeName of Object.keys(that.matchAnyAttribute)) {
			const	escapedAttributeName	= escapeAttributeName(attributeName, joinedAttrs);

			let	attributeValues	= that.matchAnyAttribute[attributeName];

			joinedAttrs.push(attributeName);
			joinSql += '	LEFT JOIN product_product_attributes AS ' + escapedAttributeName + '\n';
			joinSql += '		ON	' + escapedAttributeName + '.productUuid	= p.uuid\n';
			joinSql += '		AND	' + escapedAttributeName + '.attributeUuid	= (SELECT uuid FROM product_attributes WHERE name = ?)\n';
			attrNames.push(attributeName);

			if ( ! Array.isArray(attributeValues)) {
				attributeValues = [attributeValues];
			}

			for (const attributeValue of attributeValues) {
				if (first === true) {
					first	= false;
				} else {
					whereSql	+= 'OR ';
				}

				if (attributeValue === undefined) {
					whereSql += escapedAttributeName + '.data IS NOT NULL\n';
				} else {
					whereSql += escapedAttributeName + '.data = ?\n';
					attrValues.push(attributeValue);
				}
			}
		}

		whereSql += ')\n';
	}

	if (that.searchString !== undefined && that.searchString !== '') {
		let	searchStr	= '';

		joinSql	+= '	LEFT JOIN product_search_index AS allAttributes ON allAttributes.productUuid = p.uuid\n';

		whereSql += '	AND MATCH (allAttributes.content) AGAINST(? IN BOOLEAN MODE)\n';

		for (const str of that.searchString.trim().split(' ')) {
			searchStr += '+' + str + ' ';
		}

		attrValues.push(searchStr.trim());
	}

	for (let i = 0; attrNames[i] !== undefined; i ++) {
		dbFields.push(attrNames[i]);
	}

	for (let i = 0; attrValues[i] !== undefined; i ++) {
		dbFields.push(attrValues[i]);
	}

	cb(null, joinSql, whereSql, dbFields);
};

Products.prototype.get = function (cb) {
	const	tasks	= [],
		that	= this;

	let	productsCount	= 0,
		products	= {};

	// Make sure database is ready
	tasks.push(ready);

	// Get basic products
	tasks.push(function (cb) {
		let	countSql	= 'SELECT COUNT(*) AS products\nFROM product_products p\n',
			sql	= 'SELECT p.*\nFROM product_products p\n';

		that.generateWhere(function (err, joinSql, whereSql, dbFields) {
			const	tasks	= [];

			if (err) return cb(err);

			countSql	+= joinSql + 'WHERE\n' + whereSql;
			sql	+= joinSql + 'WHERE\n' + whereSql + 'ORDER BY p.created DESC\n';

			if (that.limit) {
				sql += ' LIMIT ' + parseInt(that.limit);
				if (that.offset) {
					sql += ' OFFSET ' + parseInt(that.offset);
				}
			}

			tasks.push(function (cb) {
				db.query(sql, dbFields, function (err, rows) {
					if (err) return cb(err);

					for (let i = 0; rows[i] !== undefined; i ++) {
						const	row	= rows[i],
							productUuid	= lUtils.formatUuid(row.uuid);

						products[productUuid]	= {};
						products[productUuid].uuid	= productUuid;
						products[productUuid].created	= row.created;
					}

					return cb();
				});
			});

			tasks.push(function (cb) {
				db.query(countSql, dbFields, function (err, rows) {
					if (err) return cb(err);

					productsCount = rows[0].products;
					cb();
				});
			});

			async.parallel(tasks, cb);
		});
	});

	// Get fields
	tasks.push(function (cb) {
		const dbFields = [];

		let sql;

		if ((that.returnAllAttributes !== true && ! that.returnAttributes) || Object.keys(products).length === 0) {
			return cb();
		}

		sql =  'SELECT productUuid, attributeUuid, `data`\n';
		sql += 'FROM product_product_attributes\n';
		sql += 'WHERE\n';
		sql += ' productUuid IN (';

		for (const productUuid of Object.keys(products)) {
			sql += '?,';
			dbFields.push(lUtils.uuidToBuffer(productUuid));
		}

		sql = sql.substring(0, sql.length - 1) + ')\n';

		if (that.returnAllAttributes !== true) {
			sql += '	AND (';

			for (let i = 0; that.returnAttributes[i] !== undefined; i ++) {
				sql += 'attributeUuid = (SELECT uuid FROM product_attributes WHERE name = ?) OR ';
				dbFields.push(that.returnAttributes[i]);
			}

			sql = sql.substring(0, sql.length - 4) + ')\n';
		}

		db.query(sql, dbFields, function (err, rows) {
			if (err) return cb(err);

			for (let i = 0; rows[i] !== undefined; i ++) {
				const	row	= rows[i],
					attributeName	= helpers.getAttributeName(row.attributeUuid);

				row.productUuid = lUtils.formatUuid(row.productUuid);

				if (products[row.productUuid].attributes === undefined) {
					products[row.productUuid].attributes = {};
				}

				if (products[row.productUuid].attributes[attributeName] === undefined) {
					products[row.productUuid].attributes[attributeName] = [];
				}

				products[row.productUuid].attributes[attributeName].push(row.data);
			}

			cb();
		});
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);

		cb(null, products, productsCount);
	});
};

Products.prototype.getUniqeAttributes = function (filters, cb) {
	const	attributes	= {},
		tasks	= [],
		that	= this;

	if (typeof filters === 'function') {
		cb	= filters;
		filters	= undefined;
	}

	if (Array.isArray(filters) && filters.length === 0) {
		return cb(null, {});
	}

	tasks.push(ready);

	tasks.push(function (cb) {
		let	sql;

		sql	=	'SELECT DISTINCT pa.name, ppa.data\n';
		sql	+=	'FROM product_products AS p\n';
		sql	+=	'	JOIN product_product_attributes	AS ppa	ON p.uuid	= ppa.productUuid\n';
		sql	+=	'	JOIN product_attributes	AS pa	ON ppa.attributeUuid	= pa.uuid\n';
		sql	+=	'WHERE 1\n';

		that.generateWhere(function (err, where, dbFields) {
			if (err) return cb(err);

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

			db.query(sql, dbFields, function (err, rows) {
				if (err) return cb(err);

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

	async.series(tasks, function (err) {
		if (err) return cb(err);
		cb(null, attributes);
	});
};

Products.prototype.getUuids = function (cb) {
	const	tasks	= [],
		uuids	= [],
		that	= this;

	// Make sure database is ready
	tasks.push(ready);

	// Get uuids
	tasks.push(function (cb) {
		let	sql	= 'SELECT p.uuid\nFROM product_products p\n';

		that.generateWhere(function (err, joinSql, whereSql, dbFields) {
			if (err) return cb(err);

			sql 	+= joinSql + 'WHERE\n' + whereSql;

			db.query(sql, dbFields, function (err, rows) {
				if (err) return cb(err);

				for (let i = 0; rows[i] !== undefined; i ++) {
					uuids.push(lUtils.formatUuid(rows[i].uuid));
				}

				cb();
			});
		});
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);

		cb(err, uuids);
	});
};

Products.prototype.rm = function (cb) {
	const	tasks	= [],
		that	= this;

	let	uuids;

	// Get uuids
	tasks.push(function (cb) {
		that.getUuids(function (err, result) {
			uuids = result;
			cb(err);
		});
	});

	// Send the message to the queue
	tasks.push(function (cb) {
		const	options	= {'exchange': dataWriter.exchangeName},
			message	= {};

		message.action	= 'rmProducts';
		message.params	= {'uuids': uuids};

		intercom.send(message, options, function (err, msgUuid) {
			if (err) return cb(err);

			dataWriter.emitter.once(msgUuid, cb);
		});
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);

		cb(null, uuids.length);
	});
};

Products.prototype.setAttribute = function (name, value, cb) {
	const	tasks	= [],
		that	= this;

	let	uuids;

	// Get uuids
	tasks.push(function (cb) {
		that.getUuids(function (err, result) {
			uuids = result;
			return cb(err);
		});
	});

	// Send the message to the queue
	tasks.push(function (cb) {
		const	options	= {'exchange': dataWriter.exchangeName},
			message	= {};

		message.action	= 'setAttribute';
		message.params	= {};
		message.params.productUuids	= uuids;
		message.params.attributeName	= name;
		message.params.attributeValue	= value;

		intercom.send(message, options, function (err, msgUuid) {
			if (err) return cb(err);

			dataWriter.emitter.once(msgUuid, cb);
		});
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);

		return cb(null, uuids.length);
	});
};

exports = module.exports = Products;
