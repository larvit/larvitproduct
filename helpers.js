'use strict';

const	dataWriter	= require(__dirname + '/dataWriter.js'),
	stripBom	= require('strip-bom'),
	uuidLib	= require('uuid'),
	lUtils	= require('larvitutils'),
	async	= require('async'),
	log	= require('winston'),
	db	= require('larvitdb');

let intercom;

function getAttributeName(uuid) {
	const	uuidBuffer	= typeof uuid === 'object' ? uuid : lUtils.uuidToBuffer(uuid),
		uuidHex	= uuidBuffer.toString('hex');

	for (let i = 0; exports.attributes[i] !== undefined; i ++) {
		if (exports.attributes[i].uuid.toString('hex') === uuidHex) {
			return exports.attributes[i].name;
		}
	}

	return undefined;
}

function getAttributeUuidBuffer(attributeName, cb) {
	// Remove unprintable space
	attributeName = stripBom(String(attributeName));

	for (let i = 0; exports.attributes[i] !== undefined; i ++) {
		if (exports.attributes[i].name === attributeName) {
			cb(null, exports.attributes[i].uuid);
			return;
		}
	}

	// If we get down here, the field does not exist, create it and rerun
	ready(function (err) {
		const	options	= {'exchange': dataWriter.exchangeName},
			message	= {};

		if (err) { cb(err); return; }

		message.action	= 'writeAttribute';
		message.params	= {};

		message.params.uuid	= uuidLib.v1();
		message.params.name	= attributeName;

		intercom.send(message, options, function (err, msgUuid) {
			if (err) { cb(err); return; }

			dataWriter.emitter.once(msgUuid, function (err) {
				if (err) { cb(err); return; }

				loadAttributesToCache(function (err) {
					if (err) { cb(err); return; }

					getAttributeUuidBuffer(attributeName, cb);
				});
			});
		});
	});
};

/**
 * Get attribute uuids by names
 *
 * @param arr	attributeNames array of strings
 * @param func	cb(err, object with names as key and uuids as values)
 */
function getAttributeUuidBuffers(attributeNames, cb) {
	const	fieldUuidsByName	= {},
		tasks	= [];

	for (let i = 0; attributeNames[i] !== undefined; i ++) {
		const	attributeName = attributeNames[i];

		tasks.push(function (cb) {
			getAttributeUuidBuffer(attributeName, function (err, fieldUuid) {
				if (err) { cb(err); return; }

				fieldUuidsByName[attributeName] = fieldUuid;
				cb();
			});
		});
	}

	async.parallel(tasks, function (err) {
		if (err) { cb(err); return; }

		cb(null, fieldUuidsByName);
	});
};

function getAttributeValues(attributeName, cb) {
	const	dbFields	=	[attributeName],
		sql	=	'SELECT DISTINCT `data`\n' +
				'FROM product_product_attributes\n' +
				'WHERE attributeUuid = (SELECT uuid FROM product_attributes WHERE name = ?)';

	db.query(sql, dbFields, function (err, rows) {
		const	values = [];

		if (err) { cb(err); return; }

		for (let i = 0; rows[i] !== undefined; i ++) {
			values.push(rows[i].data);
		}

		cb(null, values);
	});
}

function loadAttributesToCache(cb) {
	if (typeof cb !== 'function') {
		cb = function () {};
	}

	ready(function (err) {
		if (err) return cb(err);

		db.query('SELECT * FROM product_attributes ORDER BY name;', function (err, rows) {
			if (err) {
				log.error('larvitproduct: helpers.js - loadAttributesToCache() - Database error: ' + err.message);
				return;
			}

			// Empty the previous cache
			exports.attributes.length = 0;

			// Load the new values
			for (let i = 0; rows[i] !== undefined; i ++) {
				exports.attributes.push(rows[i]);
			}

			cb();
		});
	});
}
loadAttributesToCache();

function ready(cb) {
	setImmediate(function () {
		dataWriter.ready(function (err) {
			intercom	= require('larvitutils').instances.intercom;
			cb(err);
		});
	});
}

exports.attributes	= [];
exports.getAttributeName	= getAttributeName;
exports.getAttributeUuidBuffer	= getAttributeUuidBuffer;
exports.getAttributeUuidBuffers	= getAttributeUuidBuffers;
exports.getAttributeValues	= getAttributeValues;
exports.loadAttributesToCache	= loadAttributesToCache;
