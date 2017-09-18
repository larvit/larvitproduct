'use strict';

const	logPrefix	= 'larvitproduct: dbmigration/1.js: ',
	request	= require('request'),
	prodLib	= require(__dirname + '/../index.js'),
	lUtils	= require('larvitutils'),
	log	= require('winston');

exports = module.exports = function (cb) {
	const	reqObj	= {};

	reqObj.url	= 'http://' + lUtils.instances.elasticsearch.transport._config.host + '/' + prodLib.dataWriter.esIndexName + '/_settings';
	reqObj.method	= 'PUT';
	reqObj.json	= {'index.mapping.total_fields.limit': 2000};

	request(reqObj, function (err, response, body) {
		if (err) {
			log.error(logPrefix + 'Could not complete migration, err: ' + err.message);
			return cb(err);
		}

		if (response.statusCode !== 200) {
			const	err	= new Error('Could not complete migration, got statusCode: "' + response.statusCode + '"');
			log.error(logPrefix + err.message);
			console.log(body);
			return cb(err);
		}

		cb();
	});
};
