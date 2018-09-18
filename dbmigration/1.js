'use strict';

const logPrefix = 'larvitproduct: dbmigration/1.js: ';
const request = require('request');

exports = module.exports = function (cb) {
	const log = this.log;
	const reqObj = {};

	reqObj.url	= this.options.url + '/' + this.options.indexName + '/_settings';
	reqObj.method	= 'PUT';
	reqObj.json	= {'index.mapping.total_fields.limit': 2000};

	request(reqObj, function (err, response) {
		if (err) {
			log.error(logPrefix + 'Could not complete migration, err: ' + err.message);

			return cb(err);
		}

		if (response.statusCode !== 200) {
			const err = new Error('Could not complete migration, got statusCode: "' + response.statusCode + '"');

			log.error(logPrefix + err.message);

			return cb(err);
		}

		cb();
	});
};
