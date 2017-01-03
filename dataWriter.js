'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	dbmigration	= require('larvitdbmigration')({'tableName': 'product_db_version', 'migrationScriptsPath': __dirname + '/dbmigration'}),
	helpers	= require(__dirname + '/helpers.js'),
	lUtils	= require('larvitutils'),
	amsync	= require('larvitamsync'),
	async	= require('async'),
	log	= require('winston'),
	db	= require('larvitdb');

let	readyInProgress	= false,
	isReady	= false,
	intercom;

eventEmitter.setMaxListeners(30);

function listenToQueue(retries, cb) {
	const	options	= {'exchange': exports.exchangeName};

	let	listenMethod;

	if (typeof retries === 'function') {
		cb	= retries;
		retries	= 0;
	}

	if (typeof cb !== 'function') {
		cb = function(){};
	}

	if (retries === undefined) {
		retries = 0;
	}

	if (exports.mode === 'master') {
		listenMethod	= 'consume';
		options.exclusive	= true;	// It is important no other client tries to sneak
				// out messages from us, and we want "consume"
				// since we want the queue to persist even if this
				// minion goes offline.
	} else if (exports.mode === 'slave') {
		listenMethod = 'subscribe';
	} else {
		const	err	= new Error('Invalid exports.mode. Must be either "master" or "slave"');
		log.error('larvitproduct: dataWriter.js - listenToQueue() - ' + err.message);
		cb(err);
		return;
	}

	intercom	= require('larvitutils').instances.intercom;

	if ( ! (intercom instanceof require('larvitamintercom')) && retries < 10) {
		retries ++;
		setTimeout(function() {
			listenToQueue(retries, cb);
		}, 50);
		return;
	} else if ( ! (intercom instanceof require('larvitamintercom'))) {
		log.error('larvitproduct: dataWriter.js - listenToQueue() - Intercom is not set!');
		return;
	}

	log.info('larvitproduct: dataWriter.js - listenToQueue() - listenMethod: ' + listenMethod);

	intercom[listenMethod](options, function(message, ack, deliveryTag) {
		exports.ready(function(err) {
			ack(err); // Ack first, if something goes wrong we log it and handle it manually

			if (err) {
				log.error('larvitproduct: dataWriter.js - listenToQueue() - intercom.' + listenMethod + '() - exports.ready() returned err: ' + err.message);
				return;
			}

			if (typeof message !== 'object') {
				log.error('larvitproduct: dataWriter.js - listenToQueue() - intercom.' + listenMethod + '() - Invalid message received, is not an object! deliveryTag: "' + deliveryTag + '"');
				return;
			}

			if (typeof exports[message.action] === 'function') {
				exports[message.action](message.params, deliveryTag, message.uuid);
			} else {
				log.warn('larvitproduct: dataWriter.js - listenToQueue() - intercom.' + listenMethod + '() - Unknown message.action received: "' + message.action + '"');
			}
		});
	}, ready);
}
// Run listenToQueue as soon as all I/O is done, this makes sure the exports.mode can be set
// by the application before listening commences
setImmediate(listenToQueue);

// This is ran before each incoming message on the queue is handeled
function ready(retries, cb) {
	const	tasks	= [];

	if (typeof retries === 'function') {
		cb	= retries;
		retries	= 0;
	}

	if (typeof cb !== 'function') {
		cb = function(){};
	}

	if (retries === undefined) {
		retries	= 0;
	}

	if (isReady === true) { cb(); return; }

	if (readyInProgress === true) {
		eventEmitter.on('ready', cb);
		return;
	}

	intercom	= require('larvitutils').instances.intercom;

	if ( ! (intercom instanceof require('larvitamintercom')) && retries < 10) {
		retries ++;
		setTimeout(function() {
			ready(retries, cb);
		}, 50);
		return;
	} else if ( ! (intercom instanceof require('larvitamintercom'))) {
		log.error('larvitproduct: dataWriter.js - ready() - Intercom is not set!');
		return;
	}

	readyInProgress = true;

	// We are strictly in need of the intercom!
	if ( ! (intercom instanceof require('larvitamintercom'))) {
		const	err	= new Error('larvitutils.instances.intercom is not an instance of Intercom!');
		log.error('larvitproduct: dataWriter.js - ready() - ' + err.message);
		throw err;
	}

	if (exports.mode === 'both' || exports.mode === 'slave') {
		log.verbose('larvitproduct: dataWriter.js - ready() - exports.mode: "' + exports.mode + '", so read');

		tasks.push(function(cb) {
			amsync.mariadb({'exchange': exports.exchangeName + '_dataDump'}, cb);
		});
	}

	// Migrate database
	tasks.push(function(cb) {
		dbmigration(function(err) {
			if (err) {
				log.error('larvitproduct: dataWriter.js - ready() - Database error: ' + err.message);
			}

			cb(err);
		});
	});

	async.series(tasks, function(err) {
		if (err) {
			return;
		}

		isReady	= true;
		eventEmitter.emit('ready');

		if (exports.mode === 'both' || exports.mode === 'master') {
			runDumpServer(cb);
		} else {
			cb();
		}
	});
}

function rmProducts(params, deliveryTag, msgUuid) {
	const	productUuids	= params.uuids,
		productUuidBufs	= [],
		tasks	= [];

	for (let i = 0; productUuids[i] !== undefined; i ++) {
		productUuidBufs.push(lUtils.uuidToBuffer(productUuids[i]));
	}

	if (productUuids.length === 0) {
		exports.emitter.emit(msgUuid, null);
		return;
	}

	// Delete attributes
	tasks.push(function(cb) {
		let	sql	= 'DELETE FROM product_product_attributes WHERE productUuid IN (';

		for (let i = 0; productUuidBufs[i] !== undefined; i ++) {
			sql += '?,';
		}

		sql = sql.substring(0, sql.length - 1) + ');';

		db.query(sql, productUuidBufs, cb);
	});

	// Delete product
	tasks.push(function(cb) {
		let	sql	= 'DELETE FROM product_products WHERE uuid IN (';

		for (let i = 0; productUuidBufs[i] !== undefined; i ++) {
			sql += '?,';
		}

		sql = sql.substring(0, sql.length - 1) + ');';

		db.query(sql, productUuidBufs, cb);
	});

	async.series(tasks, function(err) {
		exports.emitter.emit(msgUuid, err);
	});
}

function runDumpServer(cb) {
	const	options	= {'exchange': exports.exchangeName + '_dataDump'},
		args	= [];

	if (db.conf.host) {
		args.push('-h');
		args.push(db.conf.host);
	}

	args.push('-u');
	args.push(db.conf.user);

	if (db.conf.password) {
		args.push('-p' + db.conf.password);
	}

	args.push('--single-transaction');
	args.push('--hex-blob');
	args.push(db.conf.database);

	// Tables
	args.push('product_attributes');
	args.push('product_db_version');
	args.push('product_products');
	args.push('product_product_attributes');

	options.dataDumpCmd = {
		'command':	'mysqldump',
		'args':	args
	};

	options['Content-Type'] = 'application/sql';

	new amsync.SyncServer(options, cb);
}

function setAttribute(params, deliveryTag, msgUuid) {
	const	tasks	= [];

	if (params.productUuids.length === 0) {
		exports.emitter.emit(msgUuid, null);
		return;
	}

	// Remove this attribute from the given products
	tasks.push(function(cb) {
		const	dbFields	= [params.attributeName];

		let	sql	= 'DELETE FROM product_product_attributes WHERE attributeUuid = (SELECT uuid FROM product_attributes WHERE name = ?) AND productUuid IN (';

		for (let i = 0; params.productUuids[i] !== undefined; i ++) {
			sql += '?,';
			dbFields.push(lUtils.uuidToBuffer(params.productUuids[i]));
		}

		sql = sql.substring(0, sql.length - 1) + ');';

		db.query(sql, dbFields, cb);
	});

	// Make sure the new attribute exists
	tasks.push(function(cb) {
		helpers.getAttributeUuidBuffers([params.attributeName], cb);
	});

	// Set the new attribute value
	tasks.push(function(cb) {
		const	dbFields	= [params.attributeName, params.attributeValue];

		let	sql	= 'INSERT INTO product_product_attributes (productUuid, attributeUuid, data) ';

		sql += 'SELECT uuid, (SELECT uuid FROM product_attributes WHERE name = ?), ? FROM product_products WHERE uuid IN (';

		for (let i = 0; params.productUuids[i] !== undefined; i ++) {
			sql += '?,';
			dbFields.push(lUtils.uuidToBuffer(params.productUuids[i]));
		}

		sql = sql.substring(0, sql.length - 1) + ');';

		db.query(sql, dbFields, cb);
	});

	async.series(tasks, function(err) {
		exports.emitter.emit(msgUuid, err);
	});
}

function writeProduct(params, deliveryTag, msgUuid) {
	const	productAttributes	= params.attributes,
		productUuid	= params.uuid,
		productUuidBuf	= lUtils.uuidToBuffer(productUuid),
		created	= params.created,
		tasks	= [];

	let	attributeUuidsByName;

	if (lUtils.formatUuid(productUuid) === false || productUuidBuf === false) {
		const err = new Error('Invalid productUuid: "' + productUuid + '"');
		log.error('larvitproduct: ./dataWriter.js - writeProduct() - ' + err.message);
		exports.emitter.emit(productUuid, err);
		return;
	}

	// Make sure the base product row exists
	tasks.push(function(cb) {
		const	sql	= 'INSERT IGNORE INTO product_products (uuid, created) VALUES(?,?)';

		db.query(sql, [productUuidBuf, created], cb);
	});

	// Clean out old attribute data
	tasks.push(function(cb) {
		db.query('DELETE FROM product_product_attributes WHERE productUuid = ?', [productUuidBuf], cb);
	});

	// By now we have a clean database, lets insert stuff!

	// Get all attribute uuids
	tasks.push(function(cb) {
		helpers.getAttributeUuidBuffers(Object.keys(productAttributes), function(err, result) {
			attributeUuidsByName = result;
			cb(err);
		});
	});

	// Insert attributes
	tasks.push(function(cb) {
		const	dbFields	= [];

		let	sql	= 'INSERT INTO product_product_attributes (productUuid, attributeUuid, `data`) VALUES';

		for (const fieldName of Object.keys(productAttributes)) {
			if ( ! (productAttributes[fieldName] instanceof Array)) {
				productAttributes[fieldName] = [productAttributes[fieldName]];
			}

			for (let i = 0; productAttributes[fieldName][i] !== undefined; i ++) {
				const	attributeData	= productAttributes[fieldName][i];
				sql += '(?,?,?),';
				dbFields.push(productUuidBuf);
				dbFields.push(attributeUuidsByName[fieldName]);
				dbFields.push(attributeData);
			}
		}

		sql = sql.substring(0, sql.length - 1) + ';';

		db.query(sql, dbFields, cb);
	});

	async.series(tasks, function(err) {
		exports.emitter.emit(msgUuid, err);
	});
}

function writeAttribute(params, deliveryTag, msgUuid) {
	const	uuid	= params.uuid,
		name	= params.name;

	db.query('INSERT IGNORE INTO product_attributes (uuid, name) VALUES(?,?)', [lUtils.uuidToBuffer(uuid), name], function(err) {
		exports.emitter.emit(msgUuid, err);
	});
}

exports.emitter	= new EventEmitter();
exports.exchangeName	= 'larvitproduct';
exports.listenToQueue	= listenToQueue;
exports.mode	= 'slave'; // or "master"
exports.ready	= ready;
exports.rmProducts	= rmProducts;
exports.setAttribute	= setAttribute;
exports.writeAttribute	= writeAttribute;
exports.writeProduct	= writeProduct;
