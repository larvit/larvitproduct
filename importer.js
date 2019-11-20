'use strict';

const topLogPrefix = 'larvitproduct: importer.js - ';
const Product = require(__dirname + '/product.js');
const request = require('request');
const fastCsv = require('fast-csv');
const async = require('async');
const fs = require('fs');

/**
 *
 * @param   {obj}  options - {log, productLib}
 * @param   {func} cb      - callback
 * @returns {*}            - on error, returns cb(err)
 */
function Importer(options, cb) {
	const that = this;

	for (const key of Object.keys(options)) {
		that[key] = options[key];
	}

	if (! that.log) {
		const tmpLUtils = new LUtils();

		that.log = new tmpLUtils.Log();
	}

	if (! that.productLib) {
		return cb(new Error('Required option "productLib" is missing'));
	}

	that.dataWriter = that.productLib.dataWriter;
	that.es = that.dataWriter.elasticsearch;

	cb();
}

/**
 *
 * @param {obj} product - product {uuid, attributes}
 */
function fixProductAttributes(product) {
	for (const attributeName of Object.keys(product.attributes)) {
		if (product.attributes[attributeName] === undefined) {
			delete product.attributes[attributeName];
		}

		if (! Array.isArray(product.attributes[attributeName])) {
			product.attributes[attributeName] = [product.attributes[attributeName]];
		}

		for (let i = 0; i < product.attributes[attributeName].length; ++ i) {
			if (typeof product.attributes[attributeName][i] === 'string') {
				product.attributes[attributeName][i] = product.attributes[attributeName][i].trim();
			}
		}
	}
}

/**
 * Import from file
 *
 * @param {str} filePath path to file
 * @param {obj} options	{
 *		'formatCols':	{'colName': function},	// Will be applied to all values of selected column
 *		'ignoreCols':	['colName1', 'colName2'],	// Will not write these cols to database
 *		'ignoreTopRows':	0,	// Number of top rows to ignore before treating it as the top row
 *		'noNew':	boolean	// Option to create products that did not exist before
 *		'parserOptions':	obj	// Will be forwarded to fast-csv
 *		'renameCols':	{'oldName': 'newName'},	// Rename columns, using first row as names
 *		'replaceByCols':	['col1', 'col2'],	// With erase all previous product data where BOTH these attributes/columns matches
 *		'staticColHeads':	{'4': 'foo', '7': 'bar'},	// Manually set the column names for 4 to "foo" and 7 to "bar". Counting starts at 0
 *		'staticCols':	{'colName': colValues, 'colName2': colValues ...},	// Will extend the columns with this
 *		'defaultAttributes'	{'colName': colValues, 'colName2': colValues ...},	// Default attributes for new products
 *		'updateByCols':	['col1', 'col2'],	// With update product data where BOTH these attributes/columns matches
 *		'removeColValsContaining':	['N/A', ''],	// Will remove the column value if it exactly matches one or more options in the array
 *		'removeValWhereEmpty': boolean, // Removes the value on the product if the column value is empty (an empty string or undefined)
 *		'hooks':	{'afterEachCsvRow': func}
 *		'created':	string // When (and if) a new product is created, that products propery 'created' will be set to this value
 *		'forbiddenUpdateFieldsMultipleHits':	Array containing fields not allowed to be present in attributes if multiple products are to be updated (* is used as a wildcard)
 *	}
 * @param {func} cb callback(err, [productUuid1, productUuid2]) the second array is a list of all added/altered products
 */
Importer.prototype.fromFile = function fromFile(filePath, options, cb) {
	const that = this;
	const alteredProductUuids	= [];
	const logPrefix = topLogPrefix + 'fromFile() - ';
	const errors = [];
	const tasks = [];

	let	mapping;

	// Make sure datawriter is ready
	tasks.push(function (cb) {
		that.dataWriter.ready(cb);
	});

	// Get ES mapping
	tasks.push(function (cb) {
		const url = 'http://' + that.es.transport._config.host + '/' + that.dataWriter.esIndexName + '/_mapping/product';

		request({'url': url, 'json': true}, function (err, response, body) {
			if (err) {
				that.log.warn(logPrefix + 'Could not get mappings when calling. err: ' + err.message);

				return cb(err);
			}

			if (response.statusCode !== 200) {
				const	err	= new Error('non-200 statusCode: ' + response.statusCode);

				that.log.warn(logPrefix + err.message);

				return cb(err);
			}

			mapping	= body[that.dataWriter.esIndexName].mappings.product.properties;

			cb();
		});
	});

	// Do the import
	tasks.push(function (cb) {
		const fileStream = fs.createReadStream(filePath);
		const logPrefix = topLogPrefix + 'fromFile() - ';
		const csvStream = fastCsv(options.parserOptions);
		const colHeads = [];
		const tasks = [];

		let	currentRowNr;
		let endOfStream = false;
		let processingRows = false;

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

		if (! Array.isArray(options.ignoreCols)) {
			options.ignoreCols	= [options.ignoreCols];
		}

		if (! Array.isArray(options.removeColValsContaining)) {
			options.removeColValsContaining	= [options.removeColValsContaining];
		}

		if (options.replaceByCols) {
			if (! Array.isArray(options.replaceByCols)) {
				options.replaceByCols	= [options.replaceByCols];
			}
			options.findByCols	= options.replaceByCols;
		}

		if (options.updateByCols) {
			if (! Array.isArray(options.updateByCols)) {
				options.updateByCols	= [options.updateByCols];
			}
			options.findByCols	= options.updateByCols;
		}

		fileStream.pipe(csvStream);
		csvStream.on('data', function (csvRow) {
			const	fullRow	= {};

			tasks.push(function (cb) {
				const attributes = {};
				const tasks = [];

				let	products = [];

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
						that.log.info(logPrefix + 'Ignoring column ' + i + ' on rowNr: ' + currentRowNr + ' since no column header was found');
						continue;
					} else if (colVal === undefined && options.staticCols[colHeads[i]] !== undefined) {
						colVal	= options.staticCols[colHeads[i]];
					}

					if (options.ignoreCols.indexOf(colHeads[i]) === - 1 && options.removeColValsContaining.indexOf(colVal) === - 1) {
						// We need file and image column data to import the images or files, but we do not want to save that info as an attribute on the product
						attributes[colHeads[i]]	= colVal;
					}

					fullRow[colHeads[i]]	= colVal;
				}

				// Format cols in the order the object is given to us
				if (options.formatCols !== undefined) {
					for (const colName of Object.keys(options.formatCols)) {
						if (typeof options.formatCols[colName] !== 'function') {
							that.log.warn(logPrefix + 'options.formatCols[' + colName + '] is not a function');
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

									that.log.debug(logPrefix + 'options.formatCols[' + colName + '] err: ' + err.message);
								}

								attributes[colName]	= result;
								cb(err);
							});
						});
					}
				}

				// Check if we should ignore this row
				tasks.push(function (cb) {
					if (! options.findByCols) {
						return cb();
					}

					for (let i = 0; options.findByCols[i] !== undefined; i ++) {
						if (! attributes[options.findByCols[i]]) {
							const err = new Error('Missing attribute value for "' + options.findByCols[i] + '" rowNr: ' + currentRowNr);

							that.log.verbose(logPrefix + err.message);

							return cb(err);
						}
					}

					cb();
				});

				// Check if we already have a product in the database
				tasks.push(function (cb) {
					if (! options.findByCols && options.noNew === true) {
						const err = new Error('findByCols is not set and we should not create any new products. This means no product will ever be created.');

						that.log.verbose(logPrefix + err.message);

						return cb(err);
					}

					if (options.findByCols) {
						const	terms	= [];

						for (let i = 0; options.findByCols[i] !== undefined; i ++) {
							const term = {'term': {}};
							const col = options.findByCols[i];

							if (! attributes[col]) {
								const err = new Error('findByCols: "' + col + '" is entered, but product does not have this col');

								that.log.info(logPrefix + 'Ignoring product since replaceByCol "' + col + '" is missing on rowNr: ' + currentRowNr);

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
							} else if (mapping && mapping[col]) {
								term.term[col]	= String(attributes[col]).trim();
							} else {
								const err = new Error('No keyword found for column "' + col + '" so it can not be used to find products by');

								that.log.warn(logPrefix + err.message);

								return cb(err);
							}

							terms.push(term);
						}

						request({
							'method': 'GET',
							'url': 'http://' + that.es.transport._config.host + '/' + that.dataWriter.esIndexName + '/product/_search',
							'json': true,
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
								that.log.warn(logPrefix + 'findByCols that.es.search err: ' + err.message);

								return cb(err);
							}

							if (response.statusCode !== 200) {
								const err = new Error('ES returned non-200 status code: "' + response.statusCode + '", reason: "' + result.error ? result.error.reason : '' + '"');

								that.log.warn(logPrefix + err.message);

								return cb(err);
							}

							if (! result || ! result.hits) {
								const err = new Error('Invalid response from Elasticsearch. Full response: ' + JSON.stringify(result));

								that.log.warn(logPrefix + err.message);

								errors.push({
									'type': 'save error',
									'time': new Date(),
									'message': err.message
								});

								return cb(err);
							}

							if (result.hits.total === 0 && options.noNew === true) {
								const err = new Error('No matching product found and options.noNew === true');

								that.log.verbose(logPrefix + err.message);

								return cb(err);
							} else if (result.hits.total === 0) {
								let product = new Product({'productLib': that.productLib});

								product.attributes = {};

								if (options.created) {
									product.created = options.created;
								}

								if (options.defaultAttributes) {
									for (const colName of Object.keys(options.defaultAttributes)) {
										if (options.defaultAttributes[colName] !== undefined) {
											product.attributes[colName]	= options.defaultAttributes[colName];
										}
									}
								}

								products.push(product);

								return cb();
							}

							if (result.hits.total > 1 && options.forbiddenUpdateFieldsMultipleHits) {
								const forbiddenAttributes = options.forbiddenUpdateFieldsMultipleHits.filter(x => x.indexOf('*') === - 1);
								const wildCardAttributes = options.forbiddenUpdateFieldsMultipleHits.filter(x => x.indexOf('*') !== - 1);
								const wildCardContains = wildCardAttributes.filter(x => x.startsWith('*') && x.endsWith('*'));
								const wildCartEndsWith = wildCardAttributes.filter(x => wildCardContains.indexOf(x) === - 1 && x.startsWith('*'));
								const wildCardStartsWith = wildCardAttributes.filter(x => wildCardContains.indexOf(x) === - 1 && x.endsWith('*'));

								for (const attr of Object.keys(attributes)) {
									let lowerAttr = attr.toLowerCase();
									let forbiddenAttributeFound = (forbiddenAttributes.map(x => x.toLowerCase()).indexOf(lowerAttr) > - 1);

									forbiddenAttributeFound = forbiddenAttributeFound || wildCardStartsWith.map(x => x.replace(/\*/g, '')).some(x => lowerAttr.startsWith(x.toLowerCase()));
									forbiddenAttributeFound = forbiddenAttributeFound || wildCartEndsWith.map(x => x.replace(/\*/g, '')).some(x => lowerAttr.endsWith(x.toLowerCase()));
									forbiddenAttributeFound = forbiddenAttributeFound || wildCardContains.map(x => x.replace(/\*/g, '')).some(x => lowerAttr.indexOf(x.toLowerCase()) !== - 1);

									if (forbiddenAttributeFound) {
										const err = new Error('Update not possible; multiple products found and "' + attr + '" is one of the attriblutes');

										that.log.warn(logPrefix + err.message);

										errors.push({
											'type': 'save error',
											'time': new Date(),
											'message': err.message
										});

										return cb(err);
									}
								}
							}

							const subTasks = [];

							for (const hit in result.hits.hits) {
								let product = new Product({'productLib': that.productLib, 'uuid': result.hits.hits[hit]._id});

								products.push(product);

								subTasks.push(function (cb) {
									product.loadFromDb(function (err) {
										if (err) {
											log.warn(logPrefix + 'Import failed, failed to load product with uuid "' + hit._id + '": ' + err.message);

											return cb(err);
										}

										cb();
									});
								});
							}

							async.series(subTasks, (err) => cb(err));
						});
					} else if (options.noNew !== true) {
						products.push(new Product({'productLib': that.productLib}));
						cb();
					} else {
						const err = new Error('No product found to be updated or replaced and no new products should be created due to noNew !== true');

						that.log.verbose(logPrefix + err.message);

						cb(err);
					}
				});

				// Assign product attributes fix them
				tasks.push(function (cb) {
					for (let i = 0; i < products.length; i ++) {
						if (options.updateByCols) {
							if (! products[i].attributes) {
								products[i].attributes	= {};
							}

							for (const colName of Object.keys(attributes)) {
								if (options.removeValWhereEmpty) {
									// This attribute will exist but will be set to undefined when the value is empty in the csv
									if (attributes[colName] === '' || attributes[colName] === undefined) {
										delete products[i].attributes[colName];
									} else if (attributes[colName] !== undefined) {
										products[i].attributes[colName]	= attributes[colName];
									}
								} else if (attributes[colName] !== undefined) {
									products[i].attributes[colName]	= attributes[colName];
								}
							}
						} else {
							products[i].attributes	= attributes;
						}

						fixProductAttributes(products[i]);
					}

					cb();
				});

				// Call afterEachCsvRow hook, this must be done before save!!!
				if (typeof options.hooks.afterEachCsvRow === 'function') {
					tasks.push(function (cb) {
						const subTasks = [];

						for (let i = 0; i < products.length; i ++) {
							subTasks.push(function (cb) {
								options.hooks.afterEachCsvRow({
									'currentRowNr':	currentRowNr,
									'colHeads':	colHeads,
									'product':	products[i],
									'csvRow':	csvRow,
									'fullRow':	fullRow,
									'csvStream':	csvStream
								}, cb);
							});
						}

						async.series(subTasks, (err) => cb(err));
					});
				}

				// Save (have to fix attributes again since afterEachCsvRow hook could have modified them)
				tasks.push(function (cb) {
					const subTasks = [];

					for (let i = 0; i < products.length; i ++) {
						subTasks.push(function (cb) {
							fixProductAttributes(products[i]);

							products[i].save(function (err) {
								if (err) {
									that.log.info(logPrefix + 'Could not save product: ' + err.message);
									errors.push({
										'type': 'save error',
										'time': new Date(),
										'message': err.message
									});
								} else {
									alteredProductUuids.push(products[i].uuid);
								}

								cb(err);
							});
						});
					}

					async.series(subTasks, (err) => cb(err));
				});

				async.series(tasks, function (err) {
					if (! err) {
						that.log.verbose(logPrefix + 'Imported product(s) uuid: ' + products.map(x => x.uuid).join(', '));
					}

					cb(); // Never report back an error, since that will break the import of the other products
				});
			});

			if (tasks.length >= 100) {
				csvStream.pause();
				processingRows = true;
				async.parallel(tasks, function () {
					tasks.length = 0;
					processingRows = false;
					csvStream.resume();

					if (endOfStream) {
						cb(null, alteredProductUuids, errors);
					}
				});
			}
		});

		csvStream.on('end', function () {
			endOfStream = true;

			// Run possible remaining tasks if they are not already running
			if (! processingRows) {
				async.parallel(tasks, function () {
					cb(null, alteredProductUuids, errors);
				});
			}
		});

		csvStream.on('error', function (err) {
			that.log.warn(logPrefix + 'Could not parse csv: ' + err.message);

			return cb(err);
		});
	});

	async.series(tasks, function (err) {
		cb(err, alteredProductUuids, errors);
	});
};

exports = module.exports = Importer;
