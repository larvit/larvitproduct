'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	dbmigration	= require('larvitdbmigration')({'tableName': 'product_db_version', 'migrationScriptsPath': __dirname + '/dbmigration'}),
	intercom	= require('larvitutils').instances.intercom,
	helpers	= require(__dirname + '/helpers.js'),
	lUtils	= require('larvitutils'),
	async	= require('async'),
	log	= require('winston'),
	db	= require('larvitdb');

let	readyInProgress	= false,
	isReady	= false;

function loadDataDump(cb) {
	let	dumpReceived	= false,
		retries	= 0,
		msgUuid;

	function requestDataDump() {
		const	message	= {'gief': 'data'},
			options	= {'exchange': exports.exchangeName + '_dataDump'};

		log.verbose('larvitproduct: dataWriter.js - requestDataDump() - Running');

		intercom.send(message, options, function(err, result) {
			msgUuid = result;
		});

		setTimeout(function() {
			if (dumpReceived === false && retries < 5) {
				log.verbose('larvitproduct: dataWriter.js - requestDataDump() - No dump received, retrying retrynr: ' + (retries + 1));
				retries ++;
				requestDataDump();
			} else if (dumpReceived === false && exports.mode === 'slave') {
				const	err	= new Error('No dump received and retries exhausted, failing to start since exports.mode: "' + exports.mode + '"');

				log.error('larvitproduct: dataWriter.js - requestDataDump() - ' + err.message);
				throw err;
			} else if (dumpReceived === false) {
				log.verbose('larvitproduct: dataWriter.js - requestDataDump() - No dump received and retries exhausted, starting anyway since exports.mode: "' + exports.mode + '"');
				cb();
			}
		}, 5000);
	}

	intercom.subscribe({'exchange': exports.exchangeName + '_dataDump'}, function(message, ack) {
		ack();

		// Ignore all incoming messages if dump have already been received
		if (dumpReceived === true) {
			return;
		}

		// Ignore all messages not for us
		if (message.dataDumpForUuid !== msgUuid) {
			return;
		}

		dumpReceived = true;


// Handle dump here


	}, function(err) {
		if (err) { cb(err); return; }

		requestDataDump();
	});
}

// This is ran before each incoming message on the queue is handeled
function ready(cb) {
	const	tasks	= [];

	if (isReady === true) { cb(); return; }

	if (readyInProgress === true) {
		eventEmitter.on('ready', cb);
		return;
	}

	readyInProgress = true;

	if (exports.mode === 'both' || exports.mode === 'slave') {
		log.verbose('larvitproduct: dataWriter.js: exports.mode: "' + exports.mode + '", so read');
		tasks.push(loadDataDump);
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
		cb();
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
