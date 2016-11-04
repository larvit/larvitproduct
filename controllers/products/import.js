'use strict';

const	productLib	= require(__dirname + '/../../index.js'),
	csvParse	= require('csv-parse'),
	Products	= productLib.Products,
	Product	= productLib.Product,
	async	= require('async'),
	log	= require('winston'),
	fs	= require('fs');

function parseAndSave(filePath, req) {
	const	products	= new Products(),
		colHeads	= [];

	fs.createReadStream(filePath)
		.pipe(csvParse())
		.on('data', function(csvRow) {
			const	attributes	= {},
				tasks	= [];

			let	product;

			if (colHeads.length === 0) {
				for (let i = 0; csvRow[i] !== undefined; i ++) {
					colHeads.push(csvRow[i]);
				}

				return;
			}

			for (let i = 0; csvRow[i] !== undefined; i ++) {
				attributes[colHeads[i]] = csvRow[i];
			}

			// Check if we already have a product in the database
			tasks.push(function(cb) {
				if (req.formFields.replaceByField) {
					if ( ! attributes[req.formFields.replaceByField]) {
						const	err	= new Error('replaceByField: "' + req.formFields.replaceByField + '" is entered, but product does not have this field');
						log.warn('larvitproduct: ./controllers/products/import.js - parseAndSave() - Ignoring product since replaceByField "' + req.formFields.replaceByField + '" is missing');
						cb(err);
						return;
					}

					products.matchAllAttributes	= {};
					products.matchAllAttributes[req.formFields.replaceByField] = attributes[req.formFields.replaceByField];

					products.limit	= 1;
					products.get(function(err, productList, matchedProducts) {
						if (matchedProducts === 0) {
							product = new Product();
							cb();
							return;
						}

						if (matchedProducts > 1) {
							log.warn('larvitproduct: ./controllers/products/import.js - parseAndSave() - Multiple products matched "' + req.formFields.replaceByField + '" = "' + attributes[req.formFields.replaceByField] + '"');
						}

						if ( ! productList) {
							const	err	= new Error('Invalid productList object returned from products.get()');
							log.error('larvitproduct: ./controllers/products/import.js - parseAndSave() - ' + err.message);
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
						log.warn('larvitproduct: ./controllers/products/import.js - parseAndSave() - Could not save product: ' + err.message);
					}

					cb(err);
				});
			});

			async.series(tasks, function(err) {
				if ( ! err) {
					log.verbose('larvitproduct: ./controllers/products/import.js - parseAndSave() - Imported product uuid: ' + product.uuid);
				}
			});
		})
		.on('error', function(err) {
			log.warn('larvitproduct: ./controllers/products/import.js - parseAndSave() - Could not parse csv: ' + err.message);
			//cb(err);
			return;
		})
		.on('end', function(err) {
			if (err) {
				log.warn('larvitproduct: ./controllers/products/import.js - parseAndSave() - Err on end: ' + err.message);
			}

			fs.unlink(filePath, function(err) {
				if (err) {
					log.warn('larvitproduct: ./controllers/products/import.js - parseAndSave() - fs.unlink() - err: ' + err.message);
				}
			});
		});
}

exports.run = function(req, res, cb) {
	const	tasks	= [],
		data	= {'global': res.globalData};

	// Make sure the user have the correct rights
	// This is set in larvitadmingui controllerGlobal
	if ( ! res.adminRights) {
		cb(new Error('Invalid rights'), req, res, {});
		return;
	}

	data.global.menuControllerName = 'products';

	if (data.global.formFields.import !== undefined && req.formFiles !== undefined && req.formFiles.file !== undefined && req.formFiles.file.size) {
		tasks.push(function(cb) {
			parseAndSave(req.formFiles.file.path, req);

			log.verbose('larvitproduct: ./controllers/products/import.js - Import file uploaded with size: ' + req.formFiles.file.size);

			data.global.messages = ['File upload complete, importing.'];

			cb();
		});
	}

	async.series(tasks, function(err) {
		cb(err, req, res, data);
	});
};
