'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	dbMigration	= require('larvitdbmigration')({'tableName': 'product_db_version', 'migrationScriptsPath': __dirname + '/dbmigration'}),
	dataWriter	= require(__dirname + '/dataWriter.js'),
	intercom	= require('larvitutils').instances.intercom,
	Products	= require(__dirname + '/products.js'),
	helpers	= require(__dirname + '/helpers.js'),
	uuidLib	= require('uuid'),
	async	= require('async'),
	log	= require('winston');

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
		dbMigration(function(err) {
			if (err) {
				log.error('larvitproduct: product.js: Database error: ' + err.message);
				return;
			}

			cb();
		});
	});

	// Load attributes
	tasks.push(helpers.loadAttributesToCache);

	async.series(tasks, function() {
		isReady	= true;
		eventEmitter.emit('ready');
		cb();
	});
}

function Product(options) {
	if (options === undefined) {
		options = {};
	}

	// If options is a string, assume it is an uuid
	if (typeof options === 'string') {
		this.uuid	= options;
		options	= {};
	} else if (options.uuid !== undefined) {
		this.uuid	= options.uuid;
	} else {
		this.uuid	= uuidLib.v1();
		log.verbose('larvitproduct: product.js: Product() - New Product - Creating Product with uuid: ' + this.uuid);
	}

	this.created	= options.created;
	this.attributes	= options.attributes;
	this.ready	= ready; // To expose to the outside world

	if (this.attributes	=== undefined) { this.attributes	= {};	}
	if (this.created	=== undefined) { this.created	= new Date();	}
}

Product.prototype.loadFromDb = function(cb) {
	const	products	= new Products(),
		tasks	= [],
		that	= this;

	tasks.push(ready);

	tasks.push(function(cb) {
		products.uuids	= [that.uuid];
		products.returnAllAttributes	= true;
		products.get(function(err, result) {
			if (err) { cb(err); return; }

			if (Object.keys(result).length) {
				for (const productUuid of Object.keys(result)) {
					for (const attr of Object.keys(result[productUuid])) {
						that[attr] = result[productUuid][attr];
					}
				}
			}

			cb();
		});
	});

	async.series(tasks, cb);
};

Product.prototype.getAttributeUuidBuffer	= helpers.getAttributeUuidBuffer;
Product.prototype.getAttributeUuidBuffers	= helpers.getAttributeUuidBuffers;

Product.prototype.rm = function(cb) {
	const	options	= {'exchange': dataWriter.exchangeName},
		message	= {},
		that	= this;

	message.action	= 'rmProduct';
	message.params	= {};

	message.params.uuid	= that.uuid;

	intercom.send(message, options, function(err, msgUuid) {
		if (err) { cb(err); return; }

		dataWriter.emitter.once(msgUuid, cb);
	});
};

// Saving the product object to the database.
Product.prototype.save = function(cb) {
	const	tasks	= [],
		that	= this;

	// Await database readiness
	tasks.push(ready);

	tasks.push(function(cb) {
		const	options	= {'exchange': dataWriter.exchangeName},
			message	= {};

		message.action	= 'writeProduct';
		message.params	= {};

		message.params.uuid	= that.uuid;
		message.params.created	= that.created;
		message.params.attributes	= that.attributes;

		intercom.send(message, options, function(err, msgUuid) {
			if (err) { cb(err); return; }

			dataWriter.emitter.once(msgUuid, cb);
		});
	});

	tasks.push(function(cb) {
		that.loadFromDb(cb);
	});

	async.series(tasks, cb);
};

exports = module.exports = Product;
