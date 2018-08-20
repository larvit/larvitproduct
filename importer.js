'use strict';

const	topLogPrefix	= 'larvitproduct: importer.js - ',
	dataWriter	= require(__dirname + '/dataWriter.js'),
	Product	= require(__dirname + '/product.js'),
	request	= require('request'),
	fastCsv	= require('fast-csv'),
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
 *		'removeValWhereEmpty': boolean, // Removes the value on the product if the column value is empty (an empty string or undefined)
 *		'hooks':	{'afterEachCsvRow': func}
 *	}
 * @param func cb(err, [productUuid1, productUuid2]) the second array is a list of all added/altered products
 */
exports.fromFile = function fromFile(filePath, options, cb) {
	const	alteredProductUuids	= [],
		logPrefix	= topLogPrefix + 'fromFile() - ',
		errors	= [],
		tasks	= [];

	let	mapping;

	// Make sure ES is ready
	tasks.push(function (cb) {
		dataWriter.ready(function (err) {
			// Make sure es is set
			es	= dataWriter.elasticsearch;
			cb(err);
		});
	});

	// Get ES mapping
	tasks.push(function (cb) {
		const	url	= 'http://' + es.transport._config.host + '/' + dataWriter.esIndexName + '/_mapping/product';

		request({'url': url, 'json': true}, function (err, response, body) {
			if (err) {
				log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);
				return cb(err);
			}

			if (response.statusCode !== 200) {
				const	err	= new Error('non-200 statusCode: ' + response.statusCode);
				log.warn(logPrefix + err.message);
				return cb(err);
			}

			mapping	= body[dataWriter.esIndexName].mappings.product.properties;

			cb();
		});
	});

	// Do the import
	tasks.push(function (cb) {
		const	fileStream	= fs.createReadStream(filePath),
			logPrefix	= topLogPrefix + 'fromFile() - ',
			csvStream	= fastCsv(options.parserOptions),
			colHeads	= [],
			tasks	= [];

		let	currentRowNr;

		if (options === undefined) {
			options	= {};
			cb	= function () {};
		}

		if (typeof options === 'function') {
			cb	= options;
			options	= {};
		}

		if (typeof cb !== 'function') {
			cb = function () {};
		}

		if (options.ignoreCols	=== undefined) { options.ignoreCols	= [];	}
		if (options.ignoreTopRows	=== undefined) { options.ignoreTopRows	= 0;	}
		if (options.removeColValsContaining	=== undefined) { options.removeColValsContaining	= [];	}
		if (options.renameCols	=== undefined) { options.renameCols	= {};	}
		if (options.staticColHeads	=== undefined) { options.staticColHeads	= {};	}
		if (options.hooks	=== undefined) { options.hooks	= {};	}
		if (options.removeValWhereEmpty !== true) { options.removeValWhereEmpty = false;	}

		if ( ! Array.isArray(options.ignoreCols)) {
			options.ignoreCols	= [options.ignoreCols];
		}

		if ( ! Array.isArray(options.removeColValsContaining)) {
			options.removeColValsContaining	= [options.removeColValsContaining];
		}

		if (options.replaceByCols) {
			if ( ! Array.isArray(options.replaceByCols)) {
				options.replaceByCols	= [options.replaceByCols];
			}
			options.findByCols	= options.replaceByCols;
		}

		if (options.updateByCols) {
			if ( ! Array.isArray(options.updateByCols)) {
				options.updateByCols	= [options.updateByCols];
			}
			options.findByCols	= options.updateByCols;
		}

		fileStream.pipe(csvStream);
		csvStream.on('data', function (csvRow) {
			const	fullRow	= {};

			tasks.push(function (cb) {
				const	attributes	= {},
					tasks	= [];

				let	product;

				if (currentRowNr === undefined) {
					currentRowNr	= 0;
				} else {
					currentRowNr ++;
				}

				// Set colHeads and rename cols if applicable
				if (currentRowNr === options.ignoreTopRows) {
					for (let i = 0; csvRow[i] !== undefined; i ++) {
						let	colName	= String(csvRow[i]).trim();

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
						log.info(logPrefix + 'Ignoring column ' + i + ' on rowNr: ' + currentRowNr + ' since no column header was found');
						continue;
					} else if (colVal === undefined && options.staticCols[colHeads[i]] !== undefined) {
						colVal	= options.staticCols[colHeads[i]];
					}

					if (options.ignoreCols.indexOf(colHeads[i]) === - 1 && options.removeColValsContaining.indexOf(colVal) === - 1) {
						// we need file and image column data to import the images or files, but we do not want to save that info as an attribute on the product
						attributes[colHeads[i]]	= colVal;
					}

					fullRow[colHeads[i]]	= colVal;
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
									const	rowError	= {};

									rowError.type	= 'row error';
									rowError.time	= new Date();
									rowError.column	= colName;
									rowError.message	= err.message;

									errors.push(rowError);

									log.debug(logPrefix + 'options.formatCols[' + colName + '] err: ' + err.message);
								}

								attributes[colName]	= result;
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
							const	err	= new Error('Missing attribute value for "' + options.findByCols[i] + '" rowNr: ' + currentRowNr);

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
								log.info(logPrefix + 'Ignoring product since replaceByCol "' + col + '" is missing on rowNr: ' + currentRowNr);
								return cb(err);
							}

							if (mapping && mapping[col] && mapping[col].type === 'keyword') {
								term.term[col]	= String(attributes[col]).trim();
							} else if (
								mapping
								&& mapping[col]
								&& mapping[col].fields
								&& mapping[col].fields.keyword
								&& mapping[col].fields.keyword.type === 'keyword'
							) {
								term.term[col + '.keyword'] = String(attributes[col]).trim();
							} else {
								const	err	= new Error('No keyword found for column "' + col + '" so it can not be used to find products by');
								log.warn(logPrefix + err.message);
								return cb(err);
							}

							terms.push(term);
						}

						request({
							'method':	'GET',
							'url':	'http://' + es.transport._config.host + '/' + dataWriter.esIndexName + '/product/_search',
							'json':	true,
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
						}, function (err, response, result) {
							if (err) {
								log.warn(logPrefix + 'findByCols es.search err: ' + err.message);
								return cb(err);
							}

							if (response.statusCode !== 200) {
								const	err	= new Error('ES returned non-200 status code: "' + response.statusCode + '", reason: "' + result.error ? result.error.reason : '' + '"');
								log.warn(logPrefix + err.message);
								return cb(err);
							}

							if (result.hits.total === 0 && options.noNew === true) {
								const	err	= new Error('No matching product found and options.noNew === true');
								log.verbose(logPrefix + err.message);
								return cb(err);
							} else if (result.hits.total === 0) {
								product	= new Product();
								return cb();
							}

							if (result.hits.total > 1) {
								const	err	= new Error('found more than 1 hits (' + result.hits.total + ') for findByCols: "' + JSON.stringify(options.findByCols) + '"');
								log.info(logPrefix + 'Ignoring product due to multiple target replacements/updates. ' + err.message);
								return cb(err);
							}

							if ( ! result || ! result.hits || ! result.hits.hits || ! result.hits.hits[0]) {
								const	err	= new Error('Invalid response from Elasticsearch. Full response: ' + JSON.stringify(result));
								log.warn(logPrefix + err.message);
								return cb(err);
							}

							product	= new Product(result.hits.hits[0]._id);
							product.loadFromDb(cb);
						});
					} else if (options.noNew !== true) {
						product	= new Product();
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
							product.attributes	= {};
						}

						for (const colName of Object.keys(attributes)) {
							if (options.removeValWhereEmpty) {
								if (attributes[colName] === '') {
									delete product.attributes[colName];
								} else if (attributes[colName] !== undefined) {
									product.attributes[colName]	= attributes[colName];
								}
							} else {
								if (attributes[colName] !== undefined) {
									product.attributes[colName]	= attributes[colName];
								} 
							}
						}
					} else {
						product.attributes	= attributes;
					}

					// Trim all attributes
					for (const attributeName of Object.keys(product.attributes)) {
						if (Array.isArray(product.attributes[attributeName])) {
							for (let i = 0; product.attributes[attributeName][i] !== undefined; i ++) {
								if (typeof product.attributes[attributeName][i] === 'string') {
									product.attributes[attributeName][i]	= product.attributes[attributeName][i].trim();
								}
							}
						} else if (typeof product.attributes[attributeName] === 'string') {
							product.attributes[attributeName]	= product.attributes[attributeName].trim();
						}

						if (product[attributeName] === undefined) {
							delete product[attributeName];
						}
					}

					product.save(function (err) {
						if (err) {
							log.info(logPrefix + 'Could not save product: ' + err.message);
							errors.push({
								'type':	'save error',
								'time':	new Date(),
								'message':	err.message
							});
						} else {
							alteredProductUuids.push(product.uuid);
						}

						cb(err);
					});
				});

				if (typeof options.hooks.afterEachCsvRow === 'function') {
					tasks.push(function (cb) {
						options.hooks.afterEachCsvRow({
							'currentRowNr':	currentRowNr,
							'colHeads':	colHeads,
							'product':	product,
							'csvRow':	csvRow,
							'fullRow':	fullRow,
							'csvStream':	csvStream
						}, cb);
					});
				}

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
					tasks.length	= 0;
					csvStream.resume();
				});
			}
		});

		csvStream.on('end', function () {
			// Run possible remaining tasks
			async.parallel(tasks, function () {
				cb(null, alteredProductUuids, errors);
			});
		});

		csvStream.on('error', function (err) {
			log.warn(logPrefix + 'Could not parse csv: ' + err.message);
			return cb(err);
		});
	});

	async.series(tasks, function (err) {
		cb(err, alteredProductUuids, errors);
	});
};
