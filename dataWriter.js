'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	topLogPrefix	= 'larvitproduct: dataWriter.js - ',
	DbMigration	= require('larvitdbmigration'),
	stripBom	= require('strip-bom'),
	lUtils	= require('larvitutils'),
	amsync	= require('larvitamsync'),
	async	= require('async'),
	log	= require('winston'),
	fs	= require('fs'),
	tmpDir	= require('os').tmpdir(),
	uuidLib	= require('uuid'),
	_	= require('lodash');

let	readyInProgress	= false,
	isReady	= false,
	intercom,
	es;

eventEmitter.setMaxListeners(30);

function listenToQueue(retries, cb) {
	const	logPrefix	= topLogPrefix + 'listenToQueue() - ',
		options	= {'exchange': exports.exchangeName};

	let	listenMethod;

	if (typeof retries === 'function') {
		cb	= retries;
		retries	= 0;
	}

	if (typeof cb !== 'function') {
		cb = function (){};
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
	} else if (exports.mode === 'slave' || exports.mode === 'noSync') {
		listenMethod = 'subscribe';
	} else {
		const	err	= new Error('Invalid exports.mode. Must be either "master", "slave" or "noSync"');
		log.error(logPrefix + err.message);
		cb(err);
		return;
	}

	intercom	= require('larvitutils').instances.intercom;

	if ( ! (intercom instanceof require('larvitamintercom')) && retries < 10) {
		retries ++;
		setTimeout(function () {
			listenToQueue(retries, cb);
		}, 50);
		return;
	} else if ( ! (intercom instanceof require('larvitamintercom'))) {
		log.error(logPrefix + 'Intercom is not set!');
		return;
	}

	log.info(logPrefix + 'listenMethod: ' + listenMethod);

	intercom.ready(function (err) {
		if (err) {
			log.error(logPrefix + 'intercom.ready() err: ' + err.message);
			return;
		}

		intercom[listenMethod](options, function (message, ack, deliveryTag) {
			exports.ready(function (err) {
				ack(err); // Ack first, if something goes wrong we log it and handle it manually

				if (err) {
					log.error(logPrefix + 'intercom.' + listenMethod + '() - exports.ready() returned err: ' + err.message);
					return;
				}

				if (typeof message !== 'object') {
					log.error(logPrefix + 'intercom.' + listenMethod + '() - Invalid message received, is not an object! deliveryTag: "' + deliveryTag + '"');
					return;
				}

				if (typeof exports[message.action] === 'function') {
					exports[message.action](message.params, deliveryTag, message.uuid);
				} else {
					log.warn(logPrefix + 'intercom.' + listenMethod + '() - Unknown message.action received: "' + message.action + '"');
				}
			});
		}, ready);
	});
}
// Run listenToQueue as soon as all I/O is done, this makes sure the exports.mode can be set
// by the application before listening commences
setImmediate(listenToQueue);

// This is ran before each incoming message on the queue is handeled
function ready(retries, cb) {
	const	logPrefix	= topLogPrefix + 'ready() - ',
		tasks	= [];

	if (typeof retries === 'function') {
		cb	= retries;
		retries	= 0;
	}

	if (typeof cb !== 'function') {
		cb = function (){};
	}

	if (retries === undefined) {
		retries	= 0;
	}

	if (isReady === true) { cb(); return; }

	if (readyInProgress === true) {
		eventEmitter.on('ready', cb);
		return;
	}

	intercom	= lUtils.instances.intercom;

	if ( ! (intercom instanceof require('larvitamintercom')) && retries < 10) {
		retries ++;
		setTimeout(function () {
			ready(retries, cb);
		}, 50);
		return;
	} else if ( ! (intercom instanceof require('larvitamintercom'))) {
		log.error(logPrefix + 'Intercom is not set!');
		return;
	}

	es	= lUtils.instances.elasticsearch;

	if (es === undefined && retries < 10) {
		retries ++;
		setTimeout(function () {
			ready(retries, cb);
		}, 50);
		return;
	} else if (es === undefined) {
		log.error(logPrefix + 'Elasticsearch is not set!');
		return;
	}

	readyInProgress = true;

	tasks.push(function (cb) {
		es.ping(function (err) {
			if (err) {
				log.error(logPrefix + 'es.ping() - ' + err.message);
			}

			cb(err);
		});
	});

	// Run database migrations
	tasks.push(function (cb) {
		const	options	= {};

		let dbMigration;

		options.dbType	= 'elasticsearch';
		options.dbDriver	= es;
		options.tableName	= 'larvitproduct_db_version';
		options.migrationScriptsPath	= __dirname + '/dbmigration';
		dbMigration	= new DbMigration(options);

		dbMigration.run(cb);
	});

	if (exports.mode === 'slave') {
		log.verbose(logPrefix + 'exports.mode: "' + exports.mode + '", so read');

		tasks.push(function (cb) {
			// one callback for every command. 
			const client = new amsync.SyncClient({'exchange': exports.exchangeName + '_dataDump'}, function (err) { console.log(err) });

			syncServer.handleHttpReq_original = syncServer.handleHttpReq;

			syncServer.handleHttpReq = function (req, res) {
				
			};

			cb();
		});

		//tasks.push(function (cb) {
		//	amsync.mariadb({'exchange': exports.exchangeName + '_dataDump'}, cb);
		//});
	}

	if (exports.mode === 'noSync') {
		log.warn(logPrefix + 'exports.mode: "' + exports.mode + '", never run this mode in production!');
	}

	async.series(tasks, function (err) {
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
		body	= [];

	if (productUuids.length === 0) {
		exports.emitter.emit(msgUuid, null);
		return;
	}

	for (let i = 0; productUuids[i] !== undefined; i ++) {
		body.push({'delete': {'_index': 'larvitproduct', '_type': 'product', '_id': productUuids[i]}});
	}

	es.bulk({'body': body}, function (err) {
		exports.emitter.emit(msgUuid, err);
	});
}

function runDumpServer(cb) {
	return cb();
	const	options	= {'exchange': exports.exchangeName + '_dataDump'},
		args	= [];

	options.dataDumpCmd = [];

	if (lUtils.instances.elasticsearch !== undefined) {

		let server = new amsync.SyncServer(options, cb);
		
		server.handleHttpReq_original = server.handleHttpReq;

		server.handleHttpReq = function(req, res) {

			res.setHeader('Content-Type', 'application/json');

			syncServer.options.dataDumpCmd = {
				'command': 'elasticdump',
				'args': ['--input=http://' + lUtils.instances.elasticsearch.host + '/larvitproduct', '--output=$']
			};

			if (req.url === '/mapping') {
				syncServer.options.dataDumpCmd.args.push('--type=mapping');
			} else if (req.url === '/data') {
				syncServer.options.dataDumpCmd.args.push('--type=data');
			} else if (req.url === '/analyzer') {
				syncServer.options.dataDumpCmd.args.push('--type=analyzer');
			} else {
				res.status(400);
				res.send('Invalid url');
			}

			// Run the original request handler
			syncServer.handleHttpReq_original(req, res);
		};
	} else {

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
		args.push('product_search_index');

		options.dataDumpCmd.push({
			'command':	'mysqldump',
			'args':	args
		});

		options['Content-Type'] = 'application/sql';

		new amsync.SyncServer(options, cb);
	}	
}

function writeProduct(params, deliveryTag, msgUuid) {
	const	productAttributes	= params.attributes,
		productUuid	= params.uuid,
		logPrefix	= topLogPrefix + 'writeProduct() - ',
		created	= params.created,
		tasks	= [];

	if (lUtils.formatUuid(productUuid) === false) {
		const err = new Error('Invalid productUuid: "' + productUuid + '"');
		log.error(logPrefix + err.message);
		exports.emitter.emit(productUuid, err);
		return;
	}

	tasks.push(function (cb) {
		const	body	= {'created': created};

		_.merge(body, productAttributes);

		// Make sure all attributes are arrays of strings
		for (let attributeName of Object.keys(body)) {
			if (attributeName === 'created') {
				continue;
			}

			// Clean BOM from attributeName
			if (stripBom(attributeName) !== attributeName) {
				body[stripBom(attributeName)] = body[attributeName];
				delete body[attributeName];
				attributeName = stripBom(attributeName);
			}

			if ( ! Array.isArray(body[attributeName])) {
				body[attributeName] = [body[attributeName]];
			}

			body[attributeName] = body[attributeName].map(function (val) {
				return String(val);
			});
		}

		es.index({
			'index':	'larvitproduct',
			'id':	productUuid,
			'type':	'product',
			'body':	body
		}, function (err) {
			if (err) {
				log.error(logPrefix + 'Could not write product to elasticsearch: ' + err.message);
				return cb(err);
			}

			cb();
		});
	});

	async.series(tasks, function (err) {
		exports.emitter.emit(msgUuid, err);
	});
}

exports.emitter	= new EventEmitter();
exports.exchangeName	= 'larvitproduct';
exports.listenToQueue	= listenToQueue;
exports.mode	= 'slave'; // or "master"
exports.ready	= ready;
exports.rmProducts	= rmProducts;
exports.writeProduct	= writeProduct;
