'use strict';

const EventEmitter = require('events').EventEmitter;
const topLogPrefix = 'larvitproduct: dataWriter.js - ';
const DbMigration = require('larvitdbmigration');
const Intercom = require('larvitamintercom');
const stripBom = require('strip-bom');
const checkKey	= require('check-object-key');
const uuidLib	= require('uuid');
const request	= require('request');
const LUtils	= require('larvitutils');
const amsync	= require('larvitamsync');
const spawn	= require('child_process').spawn;
const async	= require('async');
const Lfs	= require('larvitfs');
const lfs	= new Lfs();
const fs	= require('fs');
const os	= require('os');
const _	= require('lodash');

const elasticdumpPath = lfs.getPathSync('bin/elasticdump');

/**
 * 
 * @param   {obj}  options - {log, mode, intercom, esIndexName, elasticsearch, amsync}
 * @param   {func} cb	   - callback
 * @returns {any}          - on error, return cb(err)
 */
function DataWriter(options, cb) {
	const that = this;

	that.readyInProgress = false;
	that.isReady = false;
	that.exchangeName = 'larvitproduct';

	for (const key of Object.keys(options)) {
		that[key] = options[key];
	}

	if (! that.log) {
		const tmpLUtils = new LUtils();

		that.log = new tmpLUtils.Log();
	}
	that.lUtils = new LUtils({'log': that.log});

	if (! that.intercom) return cb(new Error('Required option "intercom" is missing'));
	if (! that.esIndexName) return cb(new Error('Required option "esIndexName" is missing'));
	if (! that.elasticsearch) return cb(new Error('Required option "elasticsearch" is missing'));

	if (! that.mode) {
		that.log.info(topLogPrefix + 'No "mode" option given, defaulting to "nySync"');
		that.mode = 'noSync';
	} else if (['noSync', 'master', 'slave'].indexOf(that.mode) === - 1) {
		const err = new Error('Invalid "mode" option given: "' + that.mode + '"');

		that.log.error(topLogPrefix + err.message);

		return cb(err);
	}

	that.readyEventEmitter = new EventEmitter();
	that.readyEventEmitter.setMaxListeners(30);

	that.emitter = new EventEmitter();

	that.listenToQueue(cb);
}

DataWriter.prototype.checkSettings = function checkSettings(cb) {
	const that = this;
	const logPrefix	= topLogPrefix + 'checkSettings() - ';
	const tasks	= [];

	tasks.push(function (cb) {
		checkKey({
			'obj': that,
			'objectKey': 'mode',
			'validValues': ['master', 'slave', 'noSync'],
			'default': 'noSync'
		}, function (err, warning) {
			if (warning) that.log.warn(logPrefix + warning);
			cb(err);
		});
	});

	tasks.push(function (cb) {
		checkKey({
			'obj': that,
			'objectKey': 'intercom',
			'default': new Intercom('loopback interface'),
			'defaultLabel': 'loopback interface'
		}, function (err, warning) {
			if (warning) that.log.warn(logPrefix + warning);
			cb(err);
		});
	});

	tasks.push(function (cb) {
		checkKey({
			'obj': that,
			'objectKey': 'elasticsearch'
		}, function (err, warning) {
			if (warning) that.log.warn(logPrefix + warning);
			cb(err);
		});
	});

	async.parallel(tasks, cb);
};

DataWriter.prototype.listenToQueue = function listenToQueue(retries, cb) {
	const that = this;
	const logPrefix	= topLogPrefix + 'listenToQueue() - ';
	const options = {'exchange': that.exchangeName};
	const tasks = [];

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
		that.checkSettings(cb);
	});

	tasks.push(function (cb) {
		if (that.mode === 'master') {
			listenMethod	= 'consume';
			options.exclusive	= true;	// It is important no other client tries to sneak
			//		// out messages from us, and we want "consume"
			//		// since we want the queue to persist even if this
			//		// app goes offline.
		} else if (that.mode === 'slave' || that.mode === 'noSync') {
			listenMethod = 'subscribe';
		} else {
			const err = new Error('Invalid mode. Must be either "master", "slave" or "noSync"');

			that.log.error(logPrefix + err.message);

			return cb(err);
		}

		that.log.info(logPrefix + 'listenMethod: ' + listenMethod);

		cb();
	});

	tasks.push(function (cb) {
		that.intercom.ready(function (err) {
			if (err) {
				that.log.error(logPrefix + 'intercom.ready() err: ' + err.message);

				return cb(err);
			}

			that.intercom[listenMethod](options, function (message, ack, deliveryTag) {
				that.ready(function (err) {
					ack(err); // Ack first, if something goes wrong we log it and handle it manually

					if (err) {
						that.log.error(logPrefix + 'intercom.' + listenMethod + '() - ready() returned err: ' + err.message);

						return cb(err);
					}

					if (typeof message !== 'object') {
						that.log.error(logPrefix + 'intercom.' + listenMethod + '() - Invalid message received, is not an object! deliveryTag: "' + deliveryTag + '"');

						return cb(err);
					}

					if (typeof that[message.action] === 'function') {
						that[message.action](message.params, deliveryTag, message.uuid);
					} else {
						that.log.warn(logPrefix + 'intercom.' + listenMethod + '() - Unknown message.action received: "' + message.action + '"');
					}
				});
			}, function (err) {
				if (err) return cb(err);

				that.ready(cb);
			});
		});
	});

	async.series(tasks, cb);
};

// This is ran before each incoming message on the queue is handeled
DataWriter.prototype.ready = function ready(cb) {
	const that = this;
	const logPrefix	= topLogPrefix + 'ready() - ';
	const tasks	= [];

	if (typeof cb !== 'function') {
		cb	= function () {};
	}

	if (that.isReady === true) return cb();

	if (that.readyInProgress === true) {
		that.readyEventEmitter.on('ready', cb);

		return;
	}

	that.readyInProgress = true;

	tasks.push(function (cb) {
		that.checkSettings(cb);
	});

	// Check so elasticsearch is answering ping
	tasks.push(function (cb) {
		that.elasticsearch.ping(function (err) {
			if (err) {
				that.log.error(logPrefix + 'elasticsearch.ping() - ' + err.message);
			}

			cb(err);
		});
	});

	// Resolve real index name from alias
	// We do this because Elasticsearch does NOT work the same way when speaking to an alias as when speaking to an index. FAKE NEWS ffs!
	tasks.push(function (cb) {
		request({
			'url': 'http://' + that.elasticsearch.transport._config.host + '/_cat/aliases?v',
			'json':	true
		}, function (err, response, result) {
			if (err) {
				that.log.error(logPrefix + err.message);

				return cb(err);
			}

			for (let i = 0; result[i] !== undefined; i ++) {
				if (result[i].alias === that.esIndexName) {
					const err = new Error('Index name must be the real index, not an alias. This is due to ES working differently with aliases and indexes');

					that.log.error(logPrefix + err.message);

					return cb(err);
				}
			}

			cb();
		});
	});

	// Make sure index exists
	tasks.push(function (cb) {
		that.elasticsearch.indices.create({'index': that.esIndexName}, function (err) {
			if (err) {
				if (
					err.message.substring(0, 32) === '[index_already_exists_exception]'
					|| err.message.substring(0, 35) === '[resource_already_exists_exception]'
				) {
					that.log.debug(logPrefix + 'Index alreaxy exists, is cool');

					return cb();
				}

				that.log.error(logPrefix + 'elasticsearch.indices.create() - ' + err.message);
			}

			cb(err);
		});
	});

	// Set esConf
	tasks.push(function (cb) {
		that.esConf = that.elasticsearch.transport._config;
		that.esConf.indexName = that.esIndexName;
		cb();
	});

	if (that.mode === 'slave') {
		that.log.verbose(logPrefix + 'mode: "' + that.mode + '", so read');

		tasks.push(function (cb) {
			const exchangeName = that.exchangeName + '_dataDump';
			const tmpFileName = os.tmpdir() + '/larvitproduct_data_' + uuidLib.v4();
			const tasks	= [];

			// Pipe mapping directly to elasticdump
			tasks.push(function (cb) {
				const options = {};

				options.exchange	= exchangeName + '_mapping';
				options.intercom	= that.intercom;

				new amsync.SyncClient(options, function (err, res) {
					const ed = spawn(elasticdumpPath, ['--input=$', '--output=http://' + that.esConf.host + '/' + that.esIndexName, '--type=mapping']);

					if (err) {
						that.log.warn(logPrefix + 'Sync failed for mapping: ' + err.message);

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
							that.log.warn(logPrefix + 'Error while res.on(close): ' + err.message);
						}
						cb(err);
					});
				});
			});

			// Save data to file first, since it stops mid-way when piped directly for some reason
			tasks.push(function (cb) {
				const options = {};

				options.exchange = exchangeName + '_data';
				options.intercom = that.intercom;

				new amsync.SyncClient(options, function (err, res) {
					if (err) {
						that.log.warn(logPrefix + 'Sync failed for data: ' + err.message);

						return cb(err);
					}

					res.pipe(fs.createWriteStream(tmpFileName));

					res.on('error', function (err) {
						throw err; // Is logged upstream, but should stop app execution
					});

					res.on('end', function (err) {
						if (err) {
							that.log.warn(logPrefix + 'Error while res.on(close): ' + err.message);
						}
						cb(err);
					});
				});
			});

			tasks.push(function (cb) {
				const ed = spawn(elasticdumpPath, ['--input=' + tmpFileName, '--output=http://' + that.esConf.host + '/' + that.esIndexName, '--type=data']);

				ed.stdout.on('data', function (chunk) {
					that.log.verbose(logPrefix + 'stdout: ' + chunk);
				});

				ed.stderr.on('data', function (chunk) {
					that.log.warn(logPrefix + 'stderr: ' + chunk);
				});

				ed.on('error', function (err) {
					that.log.warn(logPrefix + 'Error on reading data to elasticsearch: ' + err.message);
				});

				ed.on('close', cb);
			});

			// Remove temp file
			tasks.push(function (cb) {
				fs.unlink(tmpFileName, function (err) {
					if (err) {
						that.log.warn(logPrefix + 'Could not remove file: "' + tmpFileName + '", err: ' + err.message);
					} else {
						that.log.verbose(logPrefix + 'Removed file: "' + tmpFileName + '"');
					}
					cb(err);
				});
			});

			async.series(tasks, cb);
		});
	}

	// Run database migrations
	tasks.push(function (cb) {
		const options = {};

		options.dbType	= 'elasticsearch';
		options.url = 'http://' + that.esConf.host;
		options.indexName = that.esIndexName + '_db_version';
		options.migrationScriptsPath = __dirname + '/dbmigration';
		options.productIndexName = that.esConf.indexName;
		options.log = that.log;

		const dbMigration = new DbMigration(options);

		dbMigration.run(cb);
	});

	// Make sure elasticsearch index is up to date
	tasks.push(function (cb) {
		request.post('http://' + that.esConf.host + '/_refresh', function (err, response, body) {
			if (err) {
				that.log.error(logPrefix + 'Could not refresh elasticsearch index, err: ' + err.message);

				return cb(err);
			}

			if (response.statusCode !== 200) {
				const err = new Error('Could not refresh elasticsearch index, got statusCode: "' + response.statusCode + '"');

				that.log.error(logPrefix + err.message + ', body: ' + body);

				return cb(err);
			}

			cb(err);
		});
	});

	async.series(tasks, function (err) {
		if (err) return cb(err);

		that.isReady = true;
		that.readyEventEmitter.emit('ready');

		if (that.mode === 'master') {
			that.runDumpServer(cb);
		} else {
			cb();
		}
	});
};

DataWriter.prototype.rmProducts = function rmProducts(params, deliveryTag, msgUuid) {
	const that = this;
	const productUuids = params.uuids;
	const reqOptions = {};
	const logPrefix	= topLogPrefix + 'rmProducts() - ';

	if (productUuids.length === 0) {
		that.emitter.emit(msgUuid, null);

		return;
	}

	reqOptions.url = 'http://' + that.esConf.host + '/_bulk';
	reqOptions.method = 'POST';
	reqOptions.body	= '';
	reqOptions.headers	= {};
	reqOptions.headers['content-type']	= 'application/x-ndjson';

	for (let i = 0; productUuids[i] !== undefined; i ++) {
		reqOptions.body += '{"delete":{"_index":"' + that.esConf.indexName + '","_type":"product","_id":"' + productUuids[i] + '"}}\n';
	}

	request(reqOptions, function (err, response, body) {
		if (err) {
			that.log.error(logPrefix + 'Could not run bulk request to ES, err: ' + err.message);

			return that.emitter.emit(msgUuid, err);
		}

		if (response.statusCode !== 200) {
			const err = new Error('Non-200 statusCode gotten from ES: "' + response.statusCode + '", body: "' + body + '"');

			that.log.error(logPrefix + err.message);

			return that.emitter.emit(msgUuid, err);
		}

		that.emitter.emit(msgUuid, err);
	});
};

DataWriter.prototype.runDumpServer = function runDumpServer(cb) {
	const that = this;
	const logPrefix	= topLogPrefix + 'runDumpServer() - ';

	if (that.elasticsearch !== undefined) {
		const subTasks = [];
		const exchangeName = that.exchangeName + '_dataDump';
		const dataDumpCmd = {
			'command': elasticdumpPath,
			'args': ['--input=http://' + that.esConf.host + '/' + that.esIndexName, '--output=$']
		};

		subTasks.push(function (cb) {
			const options = {};

			options.log = that.log;
			options.exchange = exchangeName + '_mapping';
			options.dataDumpCmd	= _.cloneDeep(dataDumpCmd);
			options['Content-Type']	= 'application/json';
			options.intercom = that.intercom;
			options.dataDumpCmd.args.push('--type=mapping');
			options.amsync = {
				'host': that.amsync ? that.amsync.host : null,
				'maxPort': that.amsync ? that.amsync.maxPort : null,
				'minPort': that.amsync ? that.amsync.minPort : null
			};

			new amsync.SyncServer(options, cb);
		});

		subTasks.push(function (cb) {
			const options = {};

			options.log = that.log;
			options.exchange = exchangeName + '_data';
			options.dataDumpCmd	= _.cloneDeep(dataDumpCmd);
			options['Content-Type']	= 'application/json';
			options.intercom = that.intercom;
			options.dataDumpCmd.args.push('--type=data');
			options.amsync = {
				'host': that.amsync ? that.amsync.host	: null,
				'maxPort': that.amsync ? that.amsync.maxPort	: null,
				'minPort': that.amsync ? that.amsync.minPort	: null
			};

			new amsync.SyncServer(options, cb);
		});

		async.series(subTasks, cb);
	} else {
		that.log.warn(logPrefix + 'Elasticsearch must be configured!');
	}
};

DataWriter.prototype.writeProduct = function writeProduct(params, deliveryTag, msgUuid) {
	const that = this;
	const productAttributes = params.attributes;
	const productUuid = params.uuid;
	const logPrefix	= topLogPrefix + 'writeProduct() - ';
	const created = params.created;
	const tasks = [];

	if (that.lUtils.formatUuid(productUuid) === false) {
		const err = new Error('Invalid productUuid: "' + productUuid + '"');

		that.log.error(logPrefix + err.message);
		that.emitter.emit(msgUuid, err);

		return;
	}

	tasks.push(function (cb) {
		const body = {'created': created};

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
			if (! Array.isArray(body[attributeName])) {
				body[attributeName]	= [body[attributeName]];
			}
		}

		that.elasticsearch.index({
			'index': that.esIndexName,
			'id': productUuid,
			'type': 'product',
			'body': body
		}, function (err) {
			if (err) {
				that.log.info(logPrefix + 'Could not write product to elasticsearch: ' + err.message);

				return cb(err);
			}

			cb();
		});
	});

	async.series(tasks, function (err) {
		that.emitter.emit(msgUuid, err);
	});
};

exports = module.exports = DataWriter;
