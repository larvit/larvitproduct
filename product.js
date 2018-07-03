'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	topLogPrefix	= 'larvitproduct: product.js: ',
	dataWriter	= require(__dirname + '/dataWriter.js'),
	helpers	= require(__dirname + '/helpers.js'),
	uuidLib	= require('uuid'),
	imgLib	= require('larvitimages'),
	async	= require('async'),
	log	= require('winston');

let	readyInProgress	= false,
	isReady	= false,
	intercom,
	es;

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

	// Set dataWriter.intercom and es after dataWriter is ready
	tasks.push(function (cb) {
		intercom	= dataWriter.intercom;
		es	= dataWriter.elasticsearch;
		cb();
	});

	async.series(tasks, function () {
		isReady	= true;
		eventEmitter.emit('ready');
		cb();
	});
}

function Product(options) {
	const	logPrefix	= topLogPrefix + 'Product() - ';

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
		log.verbose(logPrefix + 'New Product - Creating Product with uuid: ' + this.uuid);
	}

	this.created	= options.created;
	this.attributes	= options.attributes;
	this.ready	= ready; // To expose to the outside world

	if (this.attributes	=== undefined) { this.attributes	= {};	}
	if (this.created	=== undefined) { this.created	= new Date();	}
}

Product.prototype.loadFromDb = function (cb) {
	const	logPrefix	= topLogPrefix + 'Product.prototype.loadFromDb() - uuid: ' + this.uuid + ' - ',
		tasks	= [],
		that	= this;

	let	esResult;

	tasks.push(ready);

	// Get basic product info
	tasks.push(function (cb) {
		es.get({
			'index':	dataWriter.esIndexName,
			'type':	'product',
			'id':	that.uuid
		}, function (err, result) {
			if (err && err.status === 404) {
				log.debug(logPrefix + 'No product found in database');
				esResult	= false;
				return cb();
			} else if (err) {
				log.error(logPrefix + 'es.get() - err: ' + err.message);
				return cb(err);
			}

			esResult	= result;
			cb();
		});
	});

	tasks.push(function (cb) {
		helpers.formatEsResult(esResult, function (err, result) {
			if (err) return cb(err);

			if (result && result.uuid) {
				that.uuid	= result.uuid;
			}

			if (result && result.created) {
				that.created	= result.created;
			}

			if (result && result.attributes) {
				that.attributes	= result.attributes;
			}

			if (result && result.images) {
				that.images	= result.images;
			}

			if (result && result.files) {
				that.files = result.files;
			}

			cb();
		});
	});

	async.series(tasks, cb);
};

Product.prototype.getAttributeUuidBuffer	= helpers.getAttributeUuidBuffer;
Product.prototype.getAttributeUuidBuffers	= helpers.getAttributeUuidBuffers;

Product.prototype.rm = function (cb) {
	const	options	= {'exchange': dataWriter.exchangeName},
		message	= {},
		that	= this;

	message.action	= 'rmProducts';
	message.params	= {};

	message.params.uuids	= [that.uuid];

	intercom.send(message, options, function (err, msgUuid) {
		if (err) return cb(err);

		dataWriter.emitter.once(msgUuid, cb);
	});
};

Product.prototype.rmImages = function (cb) {
	const	logPrefix	= topLogPrefix + 'rmImages() - ',
		that	= this;

	if ( ! that.uuid) {
		const	err	= new Error('Missing product uuid');
		log.warn(logPrefix + err.message);
		return cb(err);
	}

	helpers.getImagesForProducts([that], function (err) {
		const	tasks	= [];

		if (err) return cb(err);

		if ( ! Array.isArray(that.images)) {
			const	err	= new Error('that.images is not an array!');
			log.warn(logPrefix + err.message);
			return cb(err);
		}

		for (let i = 0; that.images[i] !== undefined; i ++) {
			const	image	= that.images[i];

			tasks.push(function (cb) {
				const	options	= {'exchange': dataWriter.exchangeName},
					message	= {},
					that	= this;

				message.action	= 'rmProductImage';
				message.params	= {};

				message.params.productUuid	= that.uuid;
				message.params.imageUuid	= image.uuid;

				intercom.send(message, options, function (err, msgUuid) {
					if (err) return cb(err);

					dataWriter.emitter.once(msgUuid, cb);
				});
			});

			tasks.push(function (cb) {
				imgLib.rmImage(image.uuid, cb);
			});
		}

		async.parallel(tasks, cb);
	});
};

// Saving the product object to the database.
Product.prototype.save = function (cb) {
	const	tasks	= [],
		that	= this;

	// Await database readiness
	tasks.push(ready);

	tasks.push(function (cb) {
		const	options	= {'exchange': dataWriter.exchangeName},
			message	= {};

		message.action	= 'writeProduct';
		message.params	= {};

		message.params.uuid	= that.uuid;
		message.params.created	= that.created;
		message.params.attributes	= that.attributes;

		intercom.send(message, options, function (err, msgUuid) {
			if (err) return cb(err);

			dataWriter.emitter.once(msgUuid, cb);
		});
	});

	tasks.push(function (cb) {
		that.loadFromDb(cb);
	});

	async.series(tasks, cb);
};

Product.prototype.saveImage = function saveImage(data, cb) {
	const	logPrefix	= topLogPrefix + 'saveImage() - ',
		that	= this;

	if ( ! that.uuid) {
		const	err	= new Error('Missing product uuid');
		log.warn(logPrefix + err.message);
		return cb(err);
	}

	imgLib.saveImage(data, function (err, result) {
		const	options	= {'exchange': dataWriter.exchangeName},
			message	= {};

		if (err) return cb(err);

		message.action	= 'addProductImage';
		message.params	= {};

		message.params.productUuid	= that.uuid;
		message.params.imageUuid	= result.uuid;

		intercom.send(message, options, function (err, msgUuid) {
			if (err) return cb(err);

			dataWriter.emitter.once(msgUuid, cb);
		});
	});
};

exports = module.exports = Product;
