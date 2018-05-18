'use strict';

const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	topLogPrefix	= 'larvitproduct: dataWriter.js - ',
	DbMigration	= require('larvitdbmigration'),
	Intercom	= require('larvitamintercom'),
	stripBom	= require('strip-bom'),
	checkKey	= require('check-object-key'),
	uuidLib	= require('uuid'),
	request	= require('request'),
	lUtils	= require('larvitutils'),
	amsync	= require('larvitamsync'),
	spawn	= require('child_process').spawn,
	async	= require('async'),
	log	= require('winston'),
	Lfs	= require('larvitfs'),
	lfs	= new Lfs(),
	fs	= require('fs'),
	os	= require('os'),
	_	= require('lodash');

let	readyInProgress	= false,
	isReady	= false,
	elasticdumpPath	= lfs.getPathSync('bin/elasticdump');

eventEmitter.setMaxListeners(30);

function listenToQueue(retries, cb) {
	const	logPrefix	= topLogPrefix + 'listenToQueue() - ',
		options	= {'exchange': exports.exchangeName},
		tasks	= [];

	let	listenMethod;

	if (typeof retries === 'function') {
		cb	= retries;
		retries	= 0;
	}

	if (typeof cb !== 'function') {
		cb	= function () {};
	}

	if (retries === undefined) {
		retries	= 0;
	}

	tasks.push(function (cb) {
		checkKey({
			'obj':	exports,
			'objectKey':	'mode',
			'validValues':	['master', 'slave', 'noSync'],
			'default':	'noSync'
		}, function (err, warning) {
			if (warning) log.warn(logPrefix + warning);
			cb(err);
		});
	});

	tasks.push(function (cb) {
		checkKey({
			'obj':	exports,
			'objectKey':	'intercom',
			'default':	new Intercom('loopback interface'),
			'defaultLabel':	'loopback interface'
		}, function (err, warning) {
			if (warning) log.warn(logPrefix + warning);
			cb(err);
		});
	});

	tasks.push(function (cb) {
		if (exports.mode === 'master') {
			listenMethod	= 'consume';
			options.exclusive	= true;	// It is important no other client tries to sneak
			//		// out messages from us, and we want "consume"
			//		// since we want the queue to persist even if this
			//		// app goes offline.
		} else if (exports.mode === 'slave' || exports.mode === 'noSync') {
			listenMethod = 'subscribe';
		} else {
			const	err	= new Error('Invalid exports.mode. Must be either "master", "slave" or "noSync"');
			log.error(logPrefix + err.message);
			return cb(err);
		}

		log.info(logPrefix + 'listenMethod: ' + listenMethod);

		cb();
	});

	tasks.push(function (cb) {
		exports.intercom.ready(function (err) {
			if (err) {
				log.error(logPrefix + 'intercom.ready() err: ' + err.message);
				return;
			}

			exports.intercom[listenMethod](options, function (message, ack, deliveryTag) {
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
			}, function (err) {
				if (err) return cb(err);
				ready(cb);
			});
		});
	});

	async.series(tasks, cb);
}
// Run listenToQueue as soon as all I/O is done, this makes sure the exports.mode can be set
// by the application before listening commences
setImmediate(listenToQueue);

// This is ran before each incoming message on the queue is handeled
function ready(cb) {
	const	logPrefix	= topLogPrefix + 'ready() - ',
		tasks	= [];

	if (typeof cb !== 'function') {
		cb	= function () {};
	}

	if (isReady === true) return cb();

	if (readyInProgress === true) {
		eventEmitter.on('ready', cb);
		return;
	}

	readyInProgress = true;

	tasks.push(function (cb) {
		const	tasks	= [];

		tasks.push(function (cb) {
			checkKey({
				'obj':	exports,
				'objectKey':	'mode',
				'validValues':	['master', 'slave', 'noSync'],
				'default':	'noSync'
			}, function (err, warning) {
				if (warning) log.warn(logPrefix + warning);
				cb(err);
			});
		});

		tasks.push(function (cb) {
			checkKey({
				'obj':	exports,
				'objectKey':	'intercom',
				'default':	new Intercom('loopback interface'),
				'defaultLabel':	'loopback interface'
			}, function (err, warning) {
				if (warning) log.warn(logPrefix + warning);
				cb(err);
			});
		});

		tasks.push(function (cb) {
			checkKey({
				'obj':	exports,
				'objectKey':	'elasticsearch'
			}, function (err, warning) {
				if (warning) log.warn(logPrefix + warning);
				cb(err);
			});
		});

		async.parallel(tasks, cb);
	});

	// Check so elasticsearch is answering ping
	tasks.push(function (cb) {
		exports.elasticsearch.ping(function (err) {
			if (err) {
				log.error(logPrefix + 'exports.elasticsearch.ping() - ' + err.message);
			}

			cb(err);
		});
	});

	// Resolve real index name from alias
	// We do this because Elasticsearch does NOT work the same way when speaking to an alias as when speaking to an index. FAKE NEWS ffs!
	tasks.push(function (cb) {
		request({
			'url':	'http://' + exports.elasticsearch.transport._config.host + '/_cat/aliases?v',
			'json':	true
		}, function (err, response, result) {
			if (err) {
				log.error(logPrefix + err.message);
				return cb(err);
			}

			for (let i = 0; result[i] !== undefined; i ++) {
				if (result[i].alias === exports.esIndexName) {
					const	err	= new Error('Index name must be the real index, not an alias. This is due to ES working differently with aliases and indexes');
					log.error(logPrefix + err.message);
					return cb(err);
				}
			}

			cb();
		});
	});

	// Make sure index exists
	tasks.push(function (cb) {
		exports.elasticsearch.indices.create({'index': exports.esIndexName}, function (err) {
			if (err) {
				if (err.message.substring(0, 32) === '[index_already_exists_exception]') {
					log.debug(logPrefix + 'Index alreaxy exists, is cool');
					return cb();
				}

				log.error(logPrefix + 'exports.elasticsearch.indices.create() - ' + err.message);
			}

			cb(err);
		});
	});

	if (exports.mode === 'slave') {
		log.verbose(logPrefix + 'exports.mode: "' + exports.mode + '", so read');

		tasks.push(function (cb) {
			const	exchangeName	= exports.exchangeName + '_dataDump',
				tmpFileName	= os.tmpdir() + '/larvitproduct_data_' + uuidLib.v4(),
				tasks	= [];

			// Pipe mapping directly to elasticdump
			tasks.push(function (cb) {
				const	options	= {};

				options.exchange	= exchangeName + '_mapping';
				options.intercom	= exports.intercom;

				new amsync.SyncClient(options, function (err, res) {
					const ed = spawn(elasticdumpPath, ['--input=$', '--output=http://' + exports.elasticsearch.transport._config.host + '/' + exports.esIndexName, '--type=mapping']);

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
				const	options	= {};

				options.exchange	= exchangeName + '_data';
				options.intercom	= exports.intercom;

				new amsync.SyncClient(options, function (err, res) {
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
				const ed = spawn(elasticdumpPath, ['--input=' + tmpFileName, '--output=http://' + exports.elasticsearch.transport._config.host + '/' + exports.esIndexName, '--type=data']);

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

	// Run database migrations
	tasks.push(function (cb) {
		const	options	= {};

		let dbMigration;

		options.dbType	= 'elasticsearch';
		options.dbDriver	= exports.elasticsearch;
		options.tableName	= exports.esIndexName + '_db_version';
		options.migrationScriptsPath	= __dirname + '/dbmigration';
		dbMigration	= new DbMigration(options);

		dbMigration.run(cb);
	});

	// Make sure elasticsearch index is up to date
	tasks.push(function (cb) {
		request.post('http://' + exports.elasticsearch.transport._config.host + '/_refresh', function (err, response, body) {
			if (err) {
				log.error(logPrefix + 'Could not refresh elasticsearch index, err: ' + err.message);
				return cb(err);
			}

			if (response.statusCode !== 200) {
				const	err	= new Error('Could not refresh elasticsearch index, got statusCode: "' + response.statusCode + '"');
				log.error(logPrefix + err.message);
				console.log(body);
				return cb(err);
			}

			cb(err);
		});
	});

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
		body.push({'delete': {'_index': exports.esIndexName, '_type': 'product', '_id': productUuids[i]}});
	}
	// Is logged upstream, but should stop app execution
	exports.elasticsearch.bulk({'body': body}, function (err) {
		exports.emitter.emit(msgUuid, err);
	});
}

function runDumpServer(cb) {
	const	logPrefix	= topLogPrefix + 'runDumpServer() - ';

	if (exports.elasticsearch !== undefined) {
		const	subTasks	= [],
			exchangeName	= exports.exchangeName + '_dataDump',
			dataDumpCmd = {
				'command': elasticdumpPath,
				'args': ['--input=http://' + exports.elasticsearch.transport._config.host + '/' + exports.esIndexName, '--output=$']
			};

		subTasks.push(function (cb) {
			const	options	= {};

			options.exchange	= exchangeName + '_mapping';
			options.dataDumpCmd	= _.cloneDeep(dataDumpCmd);
			options['Content-Type']	= 'application/json';
			options.intercom	= exports.intercom;
			options.dataDumpCmd.args.push('--type=mapping');
			options.amsync = {
				'host':	exports.amsync ? exports.amsync.host	: null,
				'maxPort':	exports.amsync ? exports.amsync.maxPort	: null,
				'minPort':	exports.amsync ? exports.amsync.minPort	: null
			};

			new amsync.SyncServer(options, cb);
		});

		subTasks.push(function (cb) {
			const	options	= {};

			options.exchange	= exchangeName + '_data';
			options.dataDumpCmd	= _.cloneDeep(dataDumpCmd);
			options['Content-Type']	= 'application/json';
			options.intercom	= exports.intercom;
			options.dataDumpCmd.args.push('--type=data');
			options.amsync = {
				'host':	exports.amsync ? exports.amsync.host	: null,
				'maxPort':	exports.amsync ? exports.amsync.maxPort	: null,
				'minPort':	exports.amsync ? exports.amsync.minPort	: null
			};

			new amsync.SyncServer(options, cb);
		});

		async.series(subTasks, cb);
	} else {
		log.warn(logPrefix + 'Elasticsearch must be configured!');
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
		exports.emitter.emit(msgUuid, err);
		return;
	}

	tasks.push(function (cb) {
		const	body	= {'created': created};

		_.merge(body, productAttributes);

		// Filter product attributes
		for (let attributeName of Object.keys(body)) {
			// Delete empty properties
			if (
				body[attributeName] === undefined
				|| body[attributeName] === ''
				|| body[attributeName] === null
			) {
				delete body[attributeName];
				continue;
			} else if (Array.isArray(body[attributeName])) {
				for (let i = 0; body[attributeName][i] !== undefined; i ++) {
					const val = body[attributeName][i];
					if (val === undefined || val === '' || val === null) {
						body[attributeName].splice(i, 1);
						i --;
					}
				}

				if (body[attributeName].length === 0) {
					delete body[attributeName];
					continue;
				}
			}

			// Clean BOM from attributeName
			if (stripBom(attributeName) !== attributeName) {
				body[stripBom(attributeName)]	= body[attributeName];
				delete body[attributeName];
				attributeName	= stripBom(attributeName);
			}

			// No concrete values are allowed, write them as arrays
			if ( ! Array.isArray(body[attributeName])) {
				body[attributeName]	= [body[attributeName]];
			}
		}

		exports.elasticsearch.index({
			'index':	exports.esIndexName,
			'id':	productUuid,
			'type':	'product',
			'body':	body
		}, function (err) {
			if (err) {
				log.info(logPrefix + 'Could not write product to elasticsearch: ' + err.message);
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
exports.amsync	= undefined;
exports.ready	= ready;
exports.rmProducts	= rmProducts;
exports.writeProduct	= writeProduct;
