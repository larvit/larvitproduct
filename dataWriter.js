'use strict';

const	EventEmitter	= require('events').EventEmitter,
	intercom	= require('larvitutils').instances.intercom,
	helpers	= require(__dirname + '/helpers.js'),
	lUtils	= require('larvitutils'),
	async	= require('async'),
	log	= require('winston'),
	db	= require('larvitdb');

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
		helpers.getAttributeUuids(Object.keys(productAttributes), function(err, result) {
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

	db.query('INSERT IGNORE INTO product_attributes (uuid, name) VALUES(?,?)', [uuid, name], function(err) {
		exports.emitter.emit(msgUuid, err);
	});
}

exports.emitter	= new EventEmitter();
exports.exchangeName	= 'larvitproduct';
exports.rmProduct	= rmProduct;
exports.writeProduct	= writeProduct;
exports.writeAttribute	= writeAttribute;

intercom.subscribe({'exchange': exports.exchangeName}, function(message, ack, deliveryTag) {
	ack(); // Ack first, if something goes wrong we log it and handle it manually

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
