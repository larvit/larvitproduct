'use strict';

const	log	= require('winston');

exports = module.exports = function (cb) {
	const	es	= this.options.dbDriver;

	es.indices.create({'index': 'larvitproduct'}, function (err) {
		if (err) {
			log.error(logPrefix + 'es.indices.create() - ' + err.message);
		}

		cb(err);
	});
};
