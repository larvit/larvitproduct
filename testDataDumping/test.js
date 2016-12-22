'use strict';

const	uuidValidate	= require('uuid-validate'),
	Intercom	= require('larvitamintercom'),
	uuidLib	= require('uuid'),
	request	= require('request'),
	assert	= require('assert'),
	lUtils	= require('larvitutils'),
	spawn	= require('child_process').spawn,
	async	= require('async'),
	http	= require('http'),
	log	= require('winston'),
	db	= require('larvitdb'),
	fs	= require('fs'),
	os	= require('os');

let	dumpIntercom,
	productLib;

// Set up winston
log.remove(log.transports.Console);
/**/log.add(log.transports.Console, {
	'level':	'warn',
	'colorize':	true,
	'timestamp':	true,
	'json':	false
});/**/

before(function(done) {
this.timeout(60000);
//	this.timeout(10000);
	const	tasks	= [];

	let	intercomConfigFile;

	// Run DB Setup
	tasks.push(function(cb) {
		let confFile;

		if (process.env.DBCONFFILE === undefined) {
			confFile = __dirname + '/../config/db_test.json';
		} else {
			confFile = process.env.DBCONFFILE;
		}

		log.verbose('DB config file: "' + confFile + '"');

		// First look for absolute path
		fs.stat(confFile, function(err) {
			if (err) {

				// Then look for this string in the config folder
				confFile = __dirname + '/../config/' + confFile;
				fs.stat(confFile, function(err) {
					if (err) throw err;
					log.verbose('DB config: ' + JSON.stringify(require(confFile)));
					db.setup(require(confFile), cb);
				});

				return;
			}

			log.verbose('DB config: ' + JSON.stringify(require(confFile)));
			db.setup(require(confFile), cb);
		});
	});

	// Check for empty db
	tasks.push(function(cb) {
		db.query('SHOW TABLES', function(err, rows) {
			if (err) throw err;

			if (rows.length) {
				throw new Error('Database is not empty. To make a test, you must supply an empty database!');
			}

			cb();
		});
	});

	// Setup intercom
	tasks.push(function(cb) {
		if (process.env.INTCONFFILE === undefined) {
			intercomConfigFile = __dirname + '/../config/amqp_test.json';
		} else {
			intercomConfigFile = process.env.INTCONFFILE;
		}

		log.verbose('Intercom config file: "' + intercomConfigFile + '"');

		// First look for absolute path
		fs.stat(intercomConfigFile, function(err) {
			if (err) {

				// Then look for this string in the config folder
				intercomConfigFile = __dirname + '/../config/' + intercomConfigFile;
				fs.stat(intercomConfigFile, function(err) {
					if (err) throw err;
					log.verbose('Intercom config: ' + JSON.stringify(require(intercomConfigFile)));
					lUtils.instances.intercom = new Intercom(require(intercomConfigFile).default);
					lUtils.instances.intercom.on('ready', cb);
				});

				return;
			}

			log.verbose('Intercom config: ' + JSON.stringify(require(intercomConfigFile)));
			lUtils.instances.intercom = new Intercom(require(intercomConfigFile).default);
			lUtils.instances.intercom.on('ready', cb);
		});
	});

	// Setup dump intercom
	tasks.push(function(cb) {
		dumpIntercom = new Intercom(require(intercomConfigFile).default);
		dumpIntercom.on('ready', cb);
	});

	// Mock dump server
	tasks.push(function(cb) {
		const	nics	= os.networkInterfaces();

		dumpIntercom.subscribe({'exchange': 'larvitproduct_dataDump'}, function(message, ack) {
			const	token	= uuidLib.v4();

			let	serverTimeout,
				server;

			ack();

			if (message.action !== 'reqestDump') {
				return;
			}

			function handleReq(req, res) {
				console.log('INCOMING REQUESsts!1');
				console.log(req.headers);

				const	process	= spawn('cat', [__dirname + '/exampleDump.sql']);

				let	headersWritten	= false;

				process.stdout.on('data', function(data) {
					if (headersWritten === false) {
						res.writeHead(200, {
							'Connection':	'Transfer-Encoding',
							'Content-Type':	'application/sql',
							'Transfer-Encoding':	'chunked'
						});

						headersWritten = true;
					}

					res.write(data.toString());
				});

				process.stderr.on('data', function(data) {
					console.log('ERROR!!!');
					console.log(data.toString());
				});

				process.on('close', function() {
					res.end();
//clearTimeout(serverTimeout);
//server.close();
				});

				process.on('error', function(err) {
					res.writeHead(500, { 'Content-Type':	'text/plain' });
					res.end('Process error: ' + err.message);
				});
			}

			server	= http.createServer(handleReq);
			server.listen(0);

			server.on('listening', function() {
				const	servedPort	= server.address().port,
					message	= {'action': 'dumpResponse', 'endpoints': []};

				for (const nic of Object.keys(nics)) {
					for (let i = 0; nics[nic][i] !== undefined; i ++) {
						const	nicAddress	= nics[nic][i];

						if (nicAddress.internal === false) {
							message.endpoints.push({
								'family':	nicAddress.family,
								'host':	nicAddress.address,
								'port':	servedPort,
								'token':	token
							});
						}
					}
				}

				dumpIntercom.send(message, {'exchange': 'larvitproduct_dataDump'});
			});

//serverTimeout = setTimeout(function() {
//	server.close();
//}, 60000);
		}, cb);
	});

	// Pretend to be a client
	tasks.push(function(cb) {
		const	tasks	= [];

		function handleIncHttp(res) {
			const	tmpFileName	= os.tmpdir() + '/tmp_dump.sql',
				tmpFile	= fs.createWriteStream(tmpFileName);



			let	mysqlClient;

			if (res.statusCode !== 200) {
				conosle.log('non-200-statuscode: ' + res.statusCode);
				cb(new Error('ikke 200 svar'));
				return;
			}

			mysqlClient	= spawn('mysql', ['-h', '172.17.0.2', '-u', 'root', '-pwkdjkc', 'test']);

			res.on('data', function(chunk) {
				mysqlClient.stdin.write(chunk);
			});

			res.on('end', function() {
				mysqlClient.stdin.write(';quit');
				console.log('EEEEND');
				cb();
			});
		}

		function getHttpData(reqOptions) {
			const	tmpFileName	= os.tmpdir() + '/tmp_dump.sql',
				tmpFile	= fs.createWriteStream(tmpFileName),
				req	= http.request(reqOptions, handleIncHttp);


var file = fs.createWriteStream("file.jpg");
var request = http.request(reqOptions, function(res) {
  res.pipe(file);
});


			req.on('error', function(err) {
				console.log('ERROrr on request');
				console.log(err);
				cb(err);
			});

			req.end();
		}

		function handleIncMessage(message, ack) {
			const	reqOptions	= {'headers': {}};

			ack();

			if (message.action !== 'dumpResponse') {
				return;
			}

			if ( ! message.endpoints) {

								throw new Error('Fail fanns inga endpoints');
			}

			reqOptions.headers.token	= message.endpoints[0].token;
			reqOptions.host	= message.endpoints[0].host;
			reqOptions.port	= message.endpoints[0].port;

			getHttpData(reqOptions);
		}

		// Subscribe to data dumps
		tasks.push(function(cb) {
			lUtils.instances.intercom.subscribe({'exchange': 'larvitproduct_dataDump'}, handleIncMessage, cb);
		});

		// Send dump request
		tasks.push(function(cb) {
			lUtils.instances.intercom.send({'action': 'reqestDump'}, {'exchange': 'larvitproduct_dataDump'}, cb);
		});

		async.series(tasks, function(err) {
			if (err) {
				console.log('error...');
				console.log(err);
			}

			// cb() should be ran from the callbacks from the remote and not here
		});
	});

	async.series(tasks, done);
});

describe('foo', function() {
	this.timeout(60000);

	it('bar', function(done) {
		setTimeout(function() {
			done();
		}, 190000);
	});
});

after(function(done) {
	db.removeAllTables(done);
});
