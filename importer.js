'use strict';

const	Products	= require(__dirname + '/products.js'),
	Product	= require(__dirname + '/product.js'),
	fastCsv	= require('fast-csv'),
	async	= require('async'),
	log	= require('winston'),
	fs	= require('fs');

/**
 * Import from file
 *
 * @param str filePath
 * @param obj options	{
 *		'formatCols':	{'colName': function},	// Will be applied to all values of selected column
 *		'ignoreCols':	['colName1', 'colName2'],	// Will not write these cols to database
 *		'ignoreTopRows':	0,	// Number of top rows to ignore before treating it as the top row
 *		'renameCols':	{'oldName': 'newName'},	// Rename columns, using first row as names
 *		'replaceByCols':	['col1', 'col2'],	// With erase all previous product data where BOTH these attributes/columns matches
 *		'staticColHeads':	{'4': 'foo', '7': 'bar'},	// Manually set the column names for 4 to "foo" and 7 to "bar". Counting starts at 0
 *		'staticCols':	{'colName': colValues, 'colName2': colValues ...}	// Will extend the columns with this
 *		'updateByCols':	['col1', 'col2'],	// With update product data where BOTH these attributes/columns matches
 *	}
 * @param func cb(err, [productUuid1, productUuid2]) the second array is a list of all added/altered products
 */
exports.fromFile = function fromFile(filePath, options, cb) {
	const	alteredProductUuids	= [],
		fileStream	= fs.createReadStream(filePath),
		csvStream	= fastCsv(),
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
					}

					colHeads.push(colName);
				}

				// Manually add the static column heads
				if (options.staticCols) {
					for (const colName of Object.keys(options.staticCols)) {
						colHeads.push(colName);
					}
				}

				return;
			} else if (currentRowNr < options.ignoreTopRows) {
				return;
			}

			// Manually add the static column values
			if (options.staticCols) {
				for (const colName of Object.keys(options.staticCols)) {
					csvRow.push(options.staticCols[colName]);
				}
			}

			// Format cols
			for (let i = 0; csvRow[i] !== undefined; i ++) {
				let	colVal	= csvRow[i];

				if (colHeads[i] === '' && colVal === '') {
					continue;
				} else if (colHeads[i] === '') {
					log.warn('larvitproduct: ./importer.js - fromFile() - Ignoring column ' + i + ' on rowNr: ' + currentRowNr + ' since no column header was found');
					continue;
				}

				if (options.formatCols !== undefined) {
					if (typeof options.formatCols[colHeads[i]] === 'function' && colVal !== undefined) {
						colVal = options.formatCols[colHeads[i]](colVal, csvRow);
					}
				}

				if (options.ignoreCols.indexOf(colHeads[i]) === - 1) {
					attributes[colHeads[i]] = colVal;
				}
			}

			// Check if we should ignore this row
			tasks.push(function(cb) {
				for (let i = 0; options.findByCols[i] !== undefined; i ++) {
					if ( ! attributes[options.findByCols[i]]) {
						const err = new Error('Missing attribute value for "' + options.findByCols[i] + '" rowNr: ' + currentRowNr);

						log.verbose('larvitproduct: ./importer.js - fromFile() - ' + err.message);
						cb(err);
						return;
					}
				}

				cb();
			});

			// Check if we already have a product in the database
			tasks.push(function(cb) {
				if (options.findByCols) {
					for (let i = 0; options.findByCols[i] !== undefined; i ++) {
						const	col	= options.findByCols[i];

						if ( ! attributes[col]) {
							const	err	= new Error('replaceByCol: "' + col + '" is entered, but product does not have this col');
							log.warn('larvitproduct: ./importer.js - fromFile() - Ignoring product since replaceByCol "' + col + '" is missing on rowNr: ' + currentRowNr);
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
						if (matchedProducts === 0) {
							product = new Product();
							cb();
							return;
						}

						if (matchedProducts > 1) {
							log.warn('larvitproduct: ./importer.js - fromFile() - Multiple products matched "' + JSON.stringify(options.findByCols) + '"');
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
						log.warn('larvitproduct: ./importer.js - fromFile() - Could not save product: ' + err.message);
					} else {
						alteredProductUuids.push(product.uuid);
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
	});

	csvStream.on('end', function() {
		async.parallelLimit(tasks, 100, function() {
			fs.unlink(filePath, function(err) {
				if (err) {
					log.warn('larvitproduct: ./importer.js - fromFile() - fs.unlink() - err: ' + err.message);
				}

				cb(err, alteredProductUuids);
			});
		});
	});

	csvStream.on('error', function(err) {
		log.warn('larvitproduct: ./importer.js - fromFile() - Could not parse csv: ' + err.message);
		cb(err);
		return;
	});
};
