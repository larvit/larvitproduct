'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	dbmigration	= require('larvitdbmigration')({'tableName': 'product_db_version', 'migrationScriptsPath': __dirname + '/dbmigration'}),
	intercom	= require('larvitutils').instances.intercom,
	helpers	= require(__dirname + '/helpers.js'),
	lUtils	= require('larvitutils'),
	amsync	= require('larvitamsync'),
	async	= require('async'),
	log	= require('winston'),
	db	= require('larvitdb');

let	readyInProgress	= false,
	isReady	= false;

// This is ran before each incoming message on the queue is handeled
function ready(cb) {
	const	tasks	= [];

	if (isReady === true) { cb(); return; }

	if (readyInProgress === true) {
		eventEmitter.on('ready', cb);
		return;
	}

	readyInProgress = true;

	// We are strictly in need of the intercom!
	if ( ! (intercom instanceof require('larvitamintercom'))) {
		const	err	= new Error('larvitutils.instances.intercom is not an instance of Intercom!');
		log.error('larvitproduct: dataWriter.js - ' + err.message);
		throw err;
	}

	if (exports.mode === 'both' || exports.mode === 'slave') {
		log.verbose('larvitproduct: dataWriter.js: exports.mode: "' + exports.mode + '", so read');

		tasks.push(function(cb) {
			amsync.mariadb({'exchange': exports.exchangeName + '_dataDump'}, cb);
		});
	}

	// Migrate database
	tasks.push(function(cb) {
		dbmigration(function(err) {
			if (err) {
				log.error('larvitproduct: dataWriter.js: Database error: ' + err.message);
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

function rmProduct(params, deliveryTag, msgUuid) {
	const	productUuid	= params.uuid,
		productUuidBuf	= lUtils.uuidToBuffer(productUuid),
		tasks	= [];

	// Delete attributes
	tasks.push(function(cb) {
		db.query('DELETE FROM product_product_attributes WHERE productUuid = ?;', [productUuidBuf], cb);
	});

	// Delete product
	tasks.push(function(cb) {
		db.query('DELETE FROM product_products WHERE uuid = ?;', [productUuidBuf], cb);
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
exports.mode	= 'both'; // Other options being "slave" and "master"
exports.ready	= ready;
exports.rmProduct	= rmProduct;
exports.writeAttribute	= writeAttribute;
exports.writeProduct	= writeProduct;

intercom.subscribe({'exchange': exports.exchangeName}, function(message, ack, deliveryTag) {
	exports.ready(function(err) {
		ack(err); // Ack first, if something goes wrong we log it and handle it manually

		if (err) {
			log.error('larvitproduct: dataWriter.js - intercom.subscribe() - exports.ready() returned err: ' + err.message);
			return;
		}

		if (typeof message !== 'object') {
			log.error('larvitproduct: dataWriter.js - intercom.subscribe() - Invalid message received, is not an object! deliveryTag: "' + deliveryTag + '"');
			return;
		}

		if (typeof exports[message.action] === 'function') {
			exports[message.action](message.params, deliveryTag, message.uuid);
		} else {
			log.warn('larvitproduct: dataWriter.js - intercom.subscribe() - Unknown message.action received: "' + message.action + '"');
		}
	});
});
