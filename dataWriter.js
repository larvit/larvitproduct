'use strict';

const	elasticdumpPath	= require('larvitfs').getPathSync('bin/elasticdump'),
	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	topLogPrefix	= 'larvitproduct: dataWriter.js - ',
	DbMigration	= require('larvitdbmigration'),
	stripBom	= require('strip-bom'),
	uuidLib	= require('uuid'),
	request	= require('request'),
	lUtils	= require('larvitutils'),
	amsync	= require('larvitamsync'),
	spawn	= require('child_process').spawn,
	async	= require('async'),
	log	= require('winston'),
	fs	= require('fs'),
	os	= require('os'),
	_	= require('lodash');

let	readyInProgress	= false,
	isReady	= false,
	intercom,
	esUrl,
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
		return cb(err);
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

	if (exports.mode !== 'slave' && exports.mode !== 'master' && exports.mode !== 'noSync') {
		const	err	= new Error('dataWriter.mode must be "slave", "master" or "noSync"');
		log.error(logPrefix + err.message);
		return cb(err);
	}

	if (isReady === true) return cb();

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

	esUrl	= 'http://' + lUtils.instances.elasticsearch.transport._config.host + '/larvitproduct/product/';

	readyInProgress = true;

	// Check so elasticsearch is answering ping
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
			const	exchangeName	= exports.exchangeName + '_dataDump',
				tmpFileName	= os.tmpdir() + '/larvitproduct_data_' + uuidLib.v4(),
				tasks	= [];

			// Pipe mapping directly to elasticdump
			tasks.push(function (cb) {
				new amsync.SyncClient({'exchange': exchangeName + '_mapping' }, function (err, res) {
					const ed = spawn(elasticdumpPath, ['--input=$', '--output=http://' + lUtils.instances.elasticsearch.transport._config.host + '/larvitproduct', '--type=mapping']);

					if (err) {
						log.warn(logPrefix + 'Sync failed for mapping: ' + err.message);
						return cb(err);
					}

					ed.stdin.setEncoding('utf-8');
					res.pipe(ed.stdin);

					res.on('error', function (err) {
						throw err; // Is logged upstream, but should stop app execution
					});

					res.on('end', function (err) {
						ed.stdin.end();
						if (err) {
							log.warn(logPrefix + 'Error while res.on(close): ' + err.message);
						}
						cb(err);
					});
				});
			});

			// Save data to file first, since it stops mid-way when piped directly for some reason
			tasks.push(function (cb) {
				new amsync.SyncClient({'exchange': exchangeName + '_data' }, function (err, res) {
					if (err) {
						log.warn(logPrefix + 'Sync failed for data: ' + err.message);
						return cb(err);
					}

					res.pipe(fs.createWriteStream(tmpFileName));

					res.on('error', function (err) {
						throw err; // Is logged upstream, but should stop app execution
					});

					res.on('end', function (err) {
						if (err) {
							log.warn(logPrefix + 'Error while res.on(close): ' + err.message);
						}
						cb(err);
					});
				});
			});

			tasks.push(function (cb) {
				const ed = spawn(elasticdumpPath, ['--input=' + tmpFileName, '--output=http://' + lUtils.instances.elasticsearch.transport._config.host + '/larvitproduct', '--type=data']);

				ed.stdout.on('data', function (chunk) {
					log.verbose(logPrefix + 'stdout: ' + chunk);
				});

				ed.stderr.on('data', function (chunk) {
					log.warn(logPrefix + 'stderr: ' + chunk);
				});

				ed.on('error', function (err) {
					log.warn(logPrefix + 'Error on reading data to elasticsearch: ' + err.message);
				});

				ed.on('close', cb);
			});

			// Remove temp file
			tasks.push(function (cb) {
				fs.unlink(tmpFileName, function (err) {
					if (err) {
						log.warn(logPrefix + 'Could not remove file: "' + tmpFileName + '", err: ' + err.message);
					} else {
						log.verbose(logPrefix + 'Removed file: "' + tmpFileName + '"');
					}
					cb(err);
				});
			});

			async.series(tasks, cb);
		});
	}

	async.series(tasks, function (err) {
		if (err) return;

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
	// Is logged upstream, but should stop app execution
	es.bulk({'body': body}, function (err) {
		exports.emitter.emit(msgUuid, err);
	});
}

function runDumpServer(cb) {
	const	logPrefix	= topLogPrefix + 'runDumpServer() - ';

	if (lUtils.instances.elasticsearch !== undefined) {
		const	subTasks	= [],
			exchangeName	= exports.exchangeName + '_dataDump',
			dataDumpCmd = {
				'command': elasticdumpPath,
				'args': ['--input=http://' + lUtils.instances.elasticsearch.transport._config.host + '/larvitproduct', '--output=$']
			};

		subTasks.push(function (cb) {
			const	options	= {};

			options.exchange	= exchangeName + '_mapping';
			options.dataDumpCmd	= _.cloneDeep(dataDumpCmd);
			options['Content-Type']	= 'application/json';
			options.dataDumpCmd.args.push('--type=mapping');
			new amsync.SyncServer(options, cb);
		});

		subTasks.push(function (cb) {
			const	options	= {};

			options.exchange	= exchangeName + '_data';
			options.dataDumpCmd	= _.cloneDeep(dataDumpCmd);
			options['Content-Type']	= 'application/json';
			options.dataDumpCmd.args.push('--type=data');
			new amsync.SyncServer(options, cb);
		});

		async.series(subTasks, cb);
	} else {
		log.warn(logPrefix + 'Elasticsearch must be configured!');
	}
}

function updateByQuery(params, deliveryTag, msgUuid) {
	const	logPrefix	= topLogPrefix + 'updateByQuery() - Url: "' + esUrl + '_update_by_query" - ',
		url	= esUrl + '_update_by_query';

	request.post({'url': url, 'body': params.updateBody, 'json': true}, function (err, response, body) {
		if (err) {
			log.error(logPrefix + 'err: ' + err.message);
			exports.emitter.emit(msgUuid, err);
			return;
		}

		if (response.statusCode !== 200) {
			const	err	= new Error('non-200 statusCode: ' + response.statusCode + ' response body: "' + JSON.stringify(body) + '"');
			log.error(logPrefix + err.message);
			exports.emitter.emit(msgUuid, err);
			return;
		}

		log.verbose(logPrefix + 'ran with updateBody: "' + JSON.stringify(params.updateBody) + '"');
		exports.emitter.emit(msgUuid);
	});
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
		exports.emitter.emit(msgUuid, err);
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
exports.mode	= false; // 'slave' or 'master' or 'noSync'
exports.ready	= ready;
exports.rmProducts	= rmProducts;
exports.updateByQuery	= updateByQuery;
exports.writeProduct	= writeProduct;
