'use strict';

const	topLogPrefix	= 'larvitproduct: importer.js - ',
	dataWriter	= require(__dirname + '/dataWriter.js'),
	Product	= require(__dirname + '/product.js'),
	fastCsv	= require('fast-csv'),
	lUtils	= require('larvitutils'),
	async	= require('async'),
	log	= require('winston'),
	fs	= require('fs');

let	es;

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
 *		'removeColValsContaining':	['N/A', ''],	// Will remove the column value if it exactly matches one or more options in the array
 *	}
 * @param func cb(err, [productUuid1, productUuid2]) the second array is a list of all added/altered products
 */
exports.fromFile = function fromFile(filePath, options, cb) {
	dataWriter.ready(function (err) {
		const	alteredProductUuids	= [],
			logPrefix	= topLogPrefix + 'fromFile() - ',
			fileStream	= fs.createReadStream(filePath),
			csvStream	= fastCsv(options.parserOptions),
			colHeads	= [],
			tasks	= [];

		let	currentRowNr;

		if (err) return cb(err);

		// Make sure es is set
		es	= lUtils.instances.elasticsearch;

		if (options === undefined) {
			options	= {};
			cb	= function (){};
		}

		if (typeof options === 'function') {
			cb	= options;
			options	= {};
		}

		if (typeof cb !== 'function') {
			cb = function (){};
		}

		if (options.ignoreCols	=== undefined) { options.ignoreCols	= [];	}
		if (options.ignoreTopRows	=== undefined) { options.ignoreTopRows	= 0;	}
		if (options.removeColValsContaining	=== undefined) { options.removeColValsContaining	= [];	}
		if (options.renameCols	=== undefined) { options.renameCols	= {};	}
		if (options.staticColHeads	=== undefined) { options.staticColHeads	= {};	}

		if ( ! Array.isArray(options.ignoreCols)) {
			options.ignoreCols = [options.ignoreCols];
		}

		if ( ! Array.isArray(options.removeColValsContaining)) {
			options.removeColValsContaining	= [options.removeColValsContaining];
		}

		if (options.replaceByCols) {
			if ( ! Array.isArray(options.replaceByCols)) {
				options.replaceByCols = [options.replaceByCols];
			}
			options.findByCols	= options.replaceByCols;
		}

		if (options.updateByCols) {
			if ( ! Array.isArray(options.updateByCols)) {
				options.updateByCols = [options.updateByCols];
			}
			options.findByCols	= options.updateByCols;
		}

		fileStream.pipe(csvStream);
		csvStream.on('data', function (csvRow) {
			tasks.push(function (cb) {
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
							if (colHeads.indexOf(colName) === - 1) {
								colHeads.push(colName);
							}
						}
					}

					return cb();
				} else if (currentRowNr < options.ignoreTopRows) {
					return cb();
				}

				for (let i = 0; colHeads[i] !== undefined; i ++) {
					let	colVal	= csvRow[i];

					if (colHeads[i] === '' && colVal === '') {
						continue;
					} else if (colHeads[i] === '') {
						log.warn(logPrefix + 'Ignoring column ' + i + ' on rowNr: ' + currentRowNr + ' since no column header was found');
						continue;
					} else if (colVal === undefined && options.staticCols[colHeads[i]] !== undefined) {
						colVal = options.staticCols[colHeads[i]];
					}

					if (options.ignoreCols.indexOf(colHeads[i]) === - 1 && options.removeColValsContaining.indexOf(colVal) === - 1) {
						attributes[colHeads[i]] = colVal;
					}
				}

				// Format cols in the order the object is given to us
				if (options.formatCols !== undefined) {
					for (const colName of Object.keys(options.formatCols)) {
						if (typeof options.formatCols[colName] !== 'function') {
							log.warn(logPrefix + 'options.formatCols[' + colName + '] is not a function');
							continue;
						}

						if (colHeads.indexOf(colName) === - 1) {
							continue;
						}

						tasks.push(function (cb) {
							options.formatCols[colName](attributes[colName], attributes, function (err, result) {
								if (err) {
									log.warn(logPrefix + 'options.formatCols[' + colName + '] err: ' + err.message);
								}

								attributes[colName] = result;
								cb(err);
							});
						});
					}
				}

				// Check if we should ignore this row
				tasks.push(function (cb) {
					if ( ! options.findByCols) {
						return cb();
					}

					for (let i = 0; options.findByCols[i] !== undefined; i ++) {
						if ( ! attributes[options.findByCols[i]]) {
							const err = new Error('Missing attribute value for "' + options.findByCols[i] + '" rowNr: ' + currentRowNr);

							log.verbose(logPrefix + err.message);
							return cb(err);
						}
					}

					cb();
				});

				// Check if we already have a product in the database
				tasks.push(function (cb) {
					if ( ! options.findByCols && options.noNew === true) {
						const	err	= new Error('findByCols is not set and we should not create any new products. This means no product will ever be created.');
						log.verbose(logPrefix + err.message);
						return cb(err);
					}

					if (options.findByCols) {
						const	terms	= [];

						for (let i = 0; options.findByCols[i] !== undefined; i ++) {
							const	term	= {'term': {}},
								col	= options.findByCols[i];

							if ( ! attributes[col]) {
								const	err	= new Error('findByCols: "' + col + '" is entered, but product does not have this col');
								log.warn(logPrefix + 'Ignoring product since replaceByCol "' + col + '" is missing on rowNr: ' + currentRowNr);
								return cb(err);
							}

							term.term[col] = attributes[col];

							terms.push(term);
						}

						es.search({
							'index':	'larvitproduct',
							'type':	'product',
							'body': {
								'query': {
									'constant_score': {
										'filter': {
											'bool': {
												'must': terms
											}
										}
									}
								}
							}
						}, function (err, result) {
							if (err) {
								log.warn(logPrefix + 'findByCols es.search err: ' + err.message);
								return cb(err);
							}

							if (result.hits.total === 0 && options.noNew === true) {
								const	err	= new Error('No matching product found and options.noNew === true');
								log.verbose(logPrefix + err.message);
								return cb(err);
							} else if (result.hits.total === 0) {
								product = new Product();
								return cb();
							}

							if (result.hits.total > 1) {
								const	err	= new Error('found more than 1 hits (' + result.hits.total + ') for findByCols: "' + JSON.stringify(options.findByCols) + '"');
								log.warn(logPrefix + 'Ignoring product due to multiple target replacements/updates. ' + err.message);
								return cb(err);
							}

							product = new Product(result.hits.hits[0]._id);
							product.loadFromDb(cb);
						});
					} else if (options.noNew !== true) {
						product = new Product();
						cb();
					} else {
						const	err	= new Error('No product found to be updated or replaced and no new products should be created due to noNew !== true');
						log.verbose(logPrefix + err.message);
						cb(err);
					}
				});

				// Assign product attributes and save
				tasks.push(function (cb) {
					if (options.updateByCols) {
						if ( ! product.attributes) {
							product.attributes = {};
						}

						for (const colName of Object.keys(attributes)) {
							if (attributes[colName] !== undefined) {
								product.attributes[colName] = attributes[colName];
							}
						}
					} else {
						product.attributes = attributes;
					}

					// Trim all attributes
					for (const attributeName of Object.keys(product.attributes)) {
						if (Array.isArray(product.attributes[attributeName])) {
							for (let i = 0; product.attributes[attributeName][i] !== undefined; i ++) {
								if (typeof product.attributes[attributeName][i] === 'string') {
									product.attributes[attributeName][i] = product.attributes[attributeName][i].trim();
								}
							}
						} else if (typeof product.attributes[attributeName] === 'string') {
							product.attributes[attributeName] = product.attributes[attributeName].trim();
						}
					}

					product.save(function (err) {
						if (err) {
							log.warn(logPrefix + 'Could not save product: ' + err.message);
						} else {
							alteredProductUuids.push(product.uuid);
						}

						cb(err);
					});
				});

				async.series(tasks, function (err) {
					if ( ! err) {
						log.verbose(logPrefix + 'Imported product uuid: ' + product.uuid);
					}

					cb(); // Never report back an error, since that will break the import of the other products
				});
			});

			if (tasks.length >= 100) {
				csvStream.pause();
				async.parallel(tasks, function () {
					tasks.length = 0;
					csvStream.resume();
				});
			}
		});

		csvStream.on('end', function () {
			// Run possible remaining tasks
			async.parallel(tasks, function () {
				cb(null, alteredProductUuids);
			});
		});

		csvStream.on('error', function (err) {
			log.warn(logPrefix + 'Could not parse csv: ' + err.message);
			return cb(err);
		});
	});
};
