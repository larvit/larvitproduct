'use strict';

const	dataWriter	= require(__dirname + '/dataWriter.js'),
	intercom	= require('larvitutils').instances.intercom,
	uuidLib	= require('node-uuid'),
	async	= require('async'),
	log	= require('winston'),
	db	= require('larvitdb');

function getAttributeUuid(attributeName, cb) {
	for (let i = 0; exports.attributes[i] !== undefined; i ++) {
		if (exports.attributes[i].name === attributeName) {
			cb(null, exports.attributes[i].uuid);
			return;
		}
	}

	// If we get down here, the field does not exist, create it and rerun
	(function() {
		const	options	= {'exchange': dataWriter.exchangeName},
			message	= {};

		message.action	= 'writeAttribute';
		message.params	= {};

		message.params.uuid	= uuidLib.v1();
		message.params.name	= attributeName;

		intercom.send(message, options, function(err, msgUuid) {
			if (err) { cb(err); return; }

			dataWriter.emitter.once(msgUuid, function(err) {
				if (err) { cb(err); return; }

				loadAttributesToCache(function(err) {
					if (err) { cb(err); return; }

					getAttributeUuid(attributeName, cb);
				});
			});
		});
	})();
};

/**
 * Get attribute uuids by names
 *
 * @param arr	attributeNames array of strings
 * @param func	cb(err, object with names as key and uuids as values)
 */
function getAttributeUuids(attributeNames, cb) {
	const	fieldUuidsByName	= {},
		tasks	= [];

	for (let i = 0; attributeNames[i] !== undefined; i ++) {
		const	attributeName = attributeNames[i];

		tasks.push(function(cb) {
			getAttributeUuid(attributeName, function(err, fieldUuid) {
				if (err) { cb(err); return; }

				fieldUuidsByName[attributeName] = fieldUuid;
				cb();
			});
		});
	}

	async.parallel(tasks, function(err) {
		if (err) { cb(err); return; }

		cb(null, fieldUuidsByName);
	});
};

function loadAttributesToCache(cb) {
	db.query('SELECT * FROM product_attributes ORDER BY name;', function(err, rows) {
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
}

exports.attributes	= [];
exports.getAttributeUuid	= getAttributeUuid;
exports.getAttributeUuids	= getAttributeUuids;
exports.loadAttributesToCache	= loadAttributesToCache;
