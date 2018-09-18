'use strict';

const logPrefix = 'larvitproduct: dbmigration/2.js: ';
const request = require('request');

exports = module.exports = function (cb) {
	const log = this.log;
	const reqObj = {};

	reqObj.url	= this.options.url + '/' + this.options.indexName + '/product/_mapping';
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
