'use strict';

const	csvParse	= require('csv-parse'),
	Products	= require(__dirname + '/products.js'),
	Product	= require(__dirname + '/product.js'),
	async	= require('async'),
	utf8	= require('to-utf-8'),
	log	= require('winston'),
	fs	= require('fs');

exports.fromFile = function fromFile(filePath, options, cb) {
	const	products	= new Products(),
		colHeads	= [],
		tasks	= [];

	if (options === undefined) {
		options	= {};
		cb	= function(){};
	}

	if (typeof options === 'function') {
		cb	= options;
		options	= {};
	}

	if (typeof cb !== 'function') {
		cb = function(){};
	}

	if (options.renameFields === undefined) {
		options.renameFields = {};
	}

	fs.createReadStream(filePath)
		.pipe(utf8())
		.pipe(csvParse(options.csvOptions))
		.on('data', function(csvRow) {
			tasks.push(function(cb) {
				const	attributes	= {},
					tasks	= [];

				let	product;

				if (colHeads.length === 0) {
					for (let i = 0; csvRow[i] !== undefined; i ++) {
						let	colName	= csvRow[i];

						if (options.renameFields[colName] !== undefined) {
							colName = options.renameFields[colName];
						}

						colHeads.push(colName);
					}

					return;
				}

				for (let i = 0; csvRow[i] !== undefined; i ++) {
					let	fieldVal	= csvRow[i];

					if (typeof options.formatFields[colHeads[i]] === 'function' && fieldVal !== undefined) {
						fieldVal = options.formatFields[colHeads[i]](fieldVal);
					}

					attributes[colHeads[i]] = fieldVal;
				}

				// Check if we already have a product in the database
				tasks.push(function(cb) {
					if (options.replaceByField) {
						if ( ! attributes[options.replaceByField]) {
							const	err	= new Error('replaceByField: "' + options.replaceByField + '" is entered, but product does not have this field');
							log.warn('larvitproduct: ./importer.js - fromFile() - Ignoring product since replaceByField "' + options.replaceByField + '" is missing');
							cb(err);
							return;
						}

						products.matchAllAttributes	= {};
						products.matchAllAttributes[options.replaceByField] = attributes[options.replaceByField];

						products.limit	= 1;
						products.get(function(err, productList, matchedProducts) {
							if (matchedProducts === 0) {
								product = new Product();
								cb();
								return;
							}

							if (matchedProducts > 1) {
								log.warn('larvitproduct: ./importer.js - fromFile() - Multiple products matched "' + options.replaceByField + '" = "' + attributes[options.replaceByField] + '"');
							}

							if ( ! productList) {
								const	err	= new Error('Invalid productList object returned from products.get()');
								log.error('larvitproduct: ./importer.js - fromFile() - ' + err.message);
								cb(err);
								return;
							}

							product = new Product(Object.keys(productList)[0]);
							product.loadFromDb(cb);
						});
					} else {
						product = new Product();
						cb();
					}
				});

				tasks.push(function(cb) {
					product.attributes = attributes;

					product.save(function(err) {
						if (err) {
							log.warn('larvitproduct: ./importer.js - fromFile() - Could not save product: ' + err.message);
						}

						cb(err);
					});
				});

				async.series(tasks, function(err) {
					if ( ! err) {
						log.verbose('larvitproduct: ./importer.js - fromFile() - Imported product uuid: ' + product.uuid);
					}

					cb(); // Never report back an error, since that will break the import of the other products
				});
			});
		})
		.on('error', function(err) {
			log.warn('larvitproduct: ./importer.js - fromFile() - Could not parse csv: ' + err.message);
			cb(err);
			return;
		})
		.on('end', function(err) {
			if (err) {
				log.warn('larvitproduct: ./importer.js - fromFile() - Err on end: ' + err.message);
			}

			async.parallelLimit(tasks, 100, function() {
				fs.unlink(filePath, function(err) {
					if (err) {
						log.warn('larvitproduct: ./importer.js - fromFile() - fs.unlink() - err: ' + err.message);
					}

					cb(err);
				});
			});
		});
};
