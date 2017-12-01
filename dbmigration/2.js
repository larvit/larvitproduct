'use strict';

const	logPrefix	= 'larvitproduct: dbmigration/2.js: ',
	request	= require('request'),
	prodLib	= require(__dirname + '/../index.js'),
	log	= require('winston');

exports = module.exports = function (cb) {
	const	reqObj	= {};

	reqObj.url	= 'http://' + prodLib.dataWriter.elasticsearch.transport._config.host + '/' + prodLib.dataWriter.esIndexName + '/product/_mapping';
	reqObj.method	= 'PUT';
	reqObj.json	= true;
	reqObj.body	= {
		'properties': {
			'name': { 'type': 'text',	'fields': { 'keyword': { 'type': 'keyword' } }  }
		}
	};	

	request(reqObj, function (err, response) {
		if (err) {
			log.error(logPrefix + 'Could not complete migration, err: ' + err.message);
			return cb(err);
		}

		if (response.statusCode !== 200) {
			const	err	= new Error('Could not complete migration, got statusCode: "' + response.statusCode + '"');
			log.error(logPrefix + err.message);
			return cb(err);
		}

		cb();
	});
};