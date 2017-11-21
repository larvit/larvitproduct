'use strict';

const	async	= require('async'),
	productLib	= require(__dirname + '/../../index.js'),
	log	= require('winston'),
	request	= require('request'),
	logPrefix = 'larvitproduct ./controllers/products/list.js - ';

function fillSearchBody(obj) {
	if (obj.query.bool	=== undefined) { obj.query.bool	= {}; }
	if (obj.query.bool.must	=== undefined) { obj.query.bool.must	= []; }
}

exports.run = function (req, res, cb) {
	const	tasks	= [],
		data	= {'global': res.globalData},
		esUrl	= 'http://' + productLib.dataWriter.elasticsearch.transport._config.host,
		searchBody	= { 'query': {} };

	// Make sure the user have the correct rights
	// This is set in larvitadmingui controllerGlobal
	if ( ! res.adminRights) {
		cb(new Error('Invalid rights'), req, res, {});
		return;
	}

	data.global.menuControllerName	= 'products';
	data.pagination	= {};
	data.pagination.urlParsed	= data.global.urlParsed;
	data.pagination.elementsPerPage	= 100;

	searchBody.size	= data.pagination.elementsPerPage;
	searchBody.from	= parseInt(data.global.urlParsed.query.offset)	|| 0;

	if (isNaN(searchBody.from) || searchBody.from < 0) {
		searchBody.from = 0;
	}

	if (data.global.urlParsed.query.filterAttributeName && data.global.urlParsed.query.filterAttributeName !== '') {
		const	term	= {'term': {}};

		fillSearchBody(searchBody);

		term.term[data.global.urlParsed.query.filterAttributeName] = data.global.urlParsed.query.filterAttributeValue;
		searchBody.query.bool.must.push(term);
	}

	if (data.global.urlParsed.query.search) {
		fillSearchBody(searchBody);

		searchBody.query.bool.must.push({
			'match': {'_all': data.global.urlParsed.query.search}
		});
	}


	tasks.push(function (cb) {

		request({'url': esUrl + '/' + productLib.dataWriter.esIndexName + '/product/_search', 'json': true, 'body': searchBody}, function (err, response, body) {
			if (err) return cb(err);

			if (response.statusCode !== 200) {
				const	err	= new Error('non-200 statusCode: ' + response.statusCode + ' from url: "' + esUrl + '/' + productLib.dataWriter.esIndexName + '/product/_search" with body: "' + JSON.stringify(searchBody) + '"');
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			data.products	= body.hits;
			data.pagination.totalElements	= body.hits.total;

			cb();
		});
	});

	// Get all available keywords
	tasks.push(function (cb) {
		productLib.helpers.getKeywords(function (err, keywords) {
			if (err) return cb(err);

			keywords.sort(function (a, b) {
				return a.localeCompare(b, 'en', {'sensitivity': 'base'});
			});

			data.productAttributes	= keywords;
			cb();
		});
	});

	// Get all available booleans
	tasks.push(function (cb) {
		productLib.helpers.getBooleans(function (err, booleans) {
			if (err) return cb(err);

			data.productAttributes = data.productAttributes.concat(booleans);

			data.productAttributes.sort(function (a, b) {
				return a.localeCompare(b, 'en', {'sensitivity': 'base'});
			});

			cb();
		});
	});

	async.series(tasks, function (err) {
		cb(err, req, res, data);
	});
};
