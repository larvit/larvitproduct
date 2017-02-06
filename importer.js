'use strict';

const	Products	= require(__dirname + '/products.js'),
	Product	= require(__dirname + '/product.js'),
	fastCsv	= require('fast-csv'),
	async	= require('async'),
	log	= require('winston'),
	fs	= require('fs');

log.context	= 'larvitproduct: ./importer.js - ';

/**
 * Import from file
 *
 * @param str filePath
 * @param obj options	{
 *		'formatCols':	{'colName': function},	// Will be applied to all values of selected column
 *		'ignoreCols':	['colName1', 'colName2'],	// Will not write these cols to database
 *		'ignoreTopRows':	0,	// Number of top rows to ignore before treating it as the top row
 *		'noNew':	boolean	// Option to create products that did not exist before
 *		'parserOptions':	obj	// Will be forwarded to fast-csv
 *		'renameCols':	{'oldName': 'newName'},	// Rename columns, using first row as names
 *		'replaceByCols':	['col1', 'col2'],	// With erase all previous product data where BOTH these attributes/columns matches
 *		'staticColHeads':	{'4': 'foo', '7': 'bar'},	// Manually set the column names for 4 to "foo" and 7 to "bar". Counting starts at 0
 *		'staticCols':	{'colName': colValues, 'colName2': colValues ...},	// Will extend the columns with this
 *		'updateByCols':	['col1', 'col2'],	// With update product data where BOTH these attributes/columns matches
 *	}
 * @param func cb(err, [productUuid1, productUuid2]) the second array is a list of all added/altered products
 */
exports.fromFile = function fromFile(filePath, options, cb) {
	const	alteredProductUuids	= [],
		fileStream	= fs.createReadStream(filePath),
		csvStream	= fastCsv(options.parserOptions),
		products	= new Products(),
		colHeads	= [],
		tasks	= [];

	let	currentRowNr;

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

	if (options.ignoreCols	=== undefined) { options.ignoreCols	= [];	}
	if (options.ignoreTopRows	=== undefined) { options.ignoreTopRows	= 0;	}
	if (options.dbMethod	=== undefined) { options.dbMethod	= 'update';	}
	if (options.renameCols	=== undefined) { options.renameCols	= {};	}
	if (options.staticColHeads	=== undefined) { options.staticColHeads	= {};	}

	if ( ! (options.ignoreCols instanceof Array)) {
		options.ignoreCols = [options.ignoreCols];
	}

	if (options.replaceByCols) {
		if ( ! (options.replaceByCols instanceof Array)) {
			options.replaceByCols = [options.replaceByCols];
		}
		options.findByCols	= options.replaceByCols;
	}

	if (options.updateByCols) {
		if ( ! (options.updateByCols instanceof Array)) {
			options.updateByCols = [options.updateByCols];
		}
		options.findByCols	= options.updateByCols;
	}

	fileStream.pipe(csvStream);
	csvStream.on('data', function(csvRow) {
		tasks.push(function(cb) {
			const	attributes	= {},
				tasks	= [];

			let	product;

			if (currentRowNr === undefined) {
				currentRowNr = 0;
			} else {
				currentRowNr ++;
			}

			// Set colHeads and rename cols if applicable
			if (currentRowNr === options.ignoreTopRows) {
				for (let i = 0; csvRow[i] !== undefined; i ++) {
					let	colName	= csvRow[i];

					if (options.staticColHeads[i] !== undefined) {
						colName = options.staticColHeads[i];
					} else if (options.renameCols[colName] !== undefined) {
						colName = options.renameCols[colName];
					} else if (options.colHeadToLowerCase === true) {
						colName = colName.toLowerCase();
					}

					colHeads.push(colName);
				}

				// Manually add the static column heads
				if (options.staticCols) {
					for (const colName of Object.keys(options.staticCols)) {
						colHeads.push(colName);
					}
				}

				cb();
				return;
			} else if (currentRowNr < options.ignoreTopRows) {
				cb();
				return;
			}

			// Manually add the static column values
			if (options.staticCols) {
				for (const colName of Object.keys(options.staticCols)) {
					csvRow.push(options.staticCols[colName]);
				}
			}

			for (let i = 0; csvRow[i] !== undefined; i ++) {
				let	colVal	= csvRow[i];

				if (colHeads[i] === '' && colVal === '') {
					continue;
				} else if (colHeads[i] === '') {
					log.warn(log.context + 'fromFile() - Ignoring column ' + i + ' on rowNr: ' + currentRowNr + ' since no column header was found');
					continue;
				}

				if (options.ignoreCols.indexOf(colHeads[i]) === - 1) {
					attributes[colHeads[i]] = colVal;
				}
			}

			// Format cols in the order the object is given to us
			if (options.formatCols !== undefined) {
				for (const colName of Object.keys(options.formatCols)) {
					if (typeof options.formatCols[colName] !== 'function') {
						log.warn(log.context + 'fromFile() - options.formatCols[' + colName + '] is not a function');
						continue;
					}
					attributes[colName] = options.formatCols[colName](attributes[colName], attributes);
				}
			}

			// Check if we should ignore this row
			tasks.push(function(cb) {
				if ( ! options.findByCols) {
					cb();
					return;
				}

				for (let i = 0; options.findByCols[i] !== undefined; i ++) {
					if ( ! attributes[options.findByCols[i]]) {
						const err = new Error('Missing attribute value for "' + options.findByCols[i] + '" rowNr: ' + currentRowNr);

						log.verbose('fromFile() - ' + err.message);
						cb(err);
						return;
					}
				}

				cb();
			});

			// Check if we already have a product in the database
			tasks.push(function(cb) {
				if ( ! options.findByCols && options.noNew === true) {
					const	err	= new Error('findByCols is not set and we should not create any new products. This means no product will ever be created.');
					log.verbose(log.context + 'fromFile() - ' + err.message);
					cb(err);
					return;
				}

				if (options.findByCols) {
					for (let i = 0; options.findByCols[i] !== undefined; i ++) {
						const	col	= options.findByCols[i];

						if ( ! attributes[col]) {
							const	err	= new Error('replaceByCol: "' + col + '" is entered, but product does not have this col');
							log.warn(log.context + 'fromFile() - Ignoring product since replaceByCol "' + col + '" is missing on rowNr: ' + currentRowNr);
							cb(err);
							return;
						}
					}

					products.matchAllAttributes	= {};
					for (let i = 0; options.findByCols[i] !== undefined; i ++) {
						const	col	= options.findByCols[i];

						products.matchAllAttributes[col]	= attributes[col];
					}

					products.limit	= 1;
					products.get(function(err, productList, matchedProducts) {
						if (matchedProducts === 0 && options.noNew === true) {
							const	err	= new Error('No matching product found and options.noNew === true');
							log.verbose(log.context + 'fromFile() - ' + err.message);
							cb(err);
							return;
						} else if (matchedProducts === 0) {
							product = new Product();
							cb();
							return;
						}

						if (matchedProducts > 1) {
							log.warn(log.context + 'fromFile() - Multiple products matched "' + JSON.stringify(options.findByCols) + '"');
						}

						if ( ! productList) {
							const	err	= new Error('Invalid productList object returned from products.get()');
							log.error(log.context + 'fromFile() - ' + err.message);
							cb(err);
							return;
						}

						product = new Product(Object.keys(productList)[0]);
						product.loadFromDb(cb);
					});
				} else if (options.noNew !== true) {
					product = new Product();
					cb();
				} else {
					const	err	= new Error('No product found to be updated or replaced and no new products should be created due to noNew !== true');
					log.verbose(log.context + 'fromFile() - ' + err.message);
					cb(err);
				}
			});

			// Assign product attributes and save
			tasks.push(function(cb) {
				if (options.updateByCols) {
					if ( ! product.attributes) {
						product.attributes = {};
					}

					for (const colName of Object.keys(attributes)) {
						product.attributes[colName] = attributes[colName];
					}
				} else {
					product.attributes = attributes;
				}

				product.save(function(err) {
					if (err) {
						log.warn(log.context + 'fromFile() - Could not save product: ' + err.message);
					} else {
						alteredProductUuids.push(product.uuid);
					}

					cb(err);
				});
			});

			async.series(tasks, function(err) {
				if ( ! err) {
					log.verbose(log.context + 'fromFile() - Imported product uuid: ' + product.uuid);
				}

				cb(); // Never report back an error, since that will break the import of the other products
			});
		});
	});

	csvStream.on('end', function() {
		async.parallelLimit(tasks, 100, function() {
			cb(null, alteredProductUuids);
		});
	});

	csvStream.on('error', function(err) {
		log.warn(log.context + 'fromFile() - Could not parse csv: ' + err.message);
		cb(err);
		return;
	});
};
