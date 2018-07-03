'use strict';

const	request	= require('requestretry'),
	prodLib	= require('../index.js'),
	lUtils	= require('larvitutils'),
	imgLib	= require('larvitimages'),
	async	= require('async'),
	db	= require('larvitdb');

exports = module.exports = function (cb) {
	const	esConf	= prodLib.dataWriter.elasticsearch.transport._config,
		tasks	= [];

	esConf.indexName	= prodLib.dataWriter.esIndexName;

	tasks.push(function (cb) {
		imgLib.dataWriter.ready(cb);
	});

	// Create the image mapping type
	tasks.push(function (cb) {
		const	reqOptions	= {};

		reqOptions.method	= 'PUT';
		reqOptions.url	= 'http://' + esConf.host + '/' + esConf.indexName + '/_mapping/products_images';
		reqOptions.json	= true;
		reqOptions.body	= {'properties': {}};
		reqOptions.body.properties.productUuid	= {'type': 'keyword'};
		reqOptions.body.properties.imageUuid	= {'type': 'keyword'};

		request(reqOptions, function (err, response, body) {
			if (err) return cb(err);

			if (response.statusCode !== 200) {
				throw new Error('Non-200 statusCode from ES: "' + response.statusCode + '", body: ' + JSON.stringify(body));
			}

			cb();
		});
	});

	// Fill the mapping table with data
	tasks.push(function (cb) {
		db.query('SELECT uuid, slug FROM images_images WHERE slug LIKE "product_%" AND LENGTH(slug) >= 44', function (err, rows) {
			const	tasks	= [];

			if (err) return cb(err);

			for (let i = 0; rows[i] !== undefined; i ++) {
				const	row	= rows[i],
					imageUuid	= lUtils.formatUuid(row.uuid),
					productUuid	= lUtils.formatUuid(row.slug.substring(8, 44));

				tasks.push(function (cb) {
					const	reqOptions	= {};

					reqOptions.method	= 'PUT';
					reqOptions.url	= 'http://' + esConf.host + '/' + esConf.indexName + '/products_images/' + productUuid + '_' + imageUuid;
					reqOptions.json	= true;
					reqOptions.body	= {'productUuid': productUuid, 'imageUuid': imageUuid};

					request(reqOptions, cb);
				});
			}

			async.parallelLimit(tasks, 20, cb);
		});
	});

	async.series(tasks, cb);
};
