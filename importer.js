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
 *
 * @param   {array} cols       - columns to loop through
 * @param   {obj} attributes   - column values
 * @param   {array} mapping    - mapping columns
 * @returns {obj}              - {'err': Error(), 'missingAttributes': [], 'missingMapping': [], 'terms': [] }
 *
 */
function colsToESTerms(cols, attributes, mapping) {
	const returnObj = {'err': undefined, 'missingAttributes': [], 'missingMapping': [], 'terms': [] };

	for (let i = 0; cols[i] !== undefined; i ++) {
		const term = {'term': {}};
		const col = cols[i];

		if (! attributes[col]) {
			returnObj.missingAttributes.push(col);

			continue;
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
			returnObj.missingMapping.push(col);

			continue;
		}

		returnObj.terms.push(term);
	}

	return returnObj;
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
 *		'findByCols':	['col1', 'col2'],	// columns used to find products
 *		'staticColHeads':	{'4': 'foo', '7': 'bar'},	// Manually set the column names for 4 to "foo" and 7 to "bar". Counting starts at 0
 *		'staticCols':	{'colName': colValues, 'colName2': colValues ...},	// Will extend the columns with this
 *		'defaultAttributes'	{'colName': colValues, 'colName2': colValues ...},	// Default attributes for new products
 *		'removeColValsContaining':	['N/A', ''],	// Will remove the column value if it exactly matches one or more options in the array
 *		'removeValWhereEmpty': boolean, // Removes the value on the product if the column value is empty (an empty string or undefined)
 *		'hooks':	{'afterEachCsvRow': func}
 *		'created':	string // When (and if) a new product is created, that products propery 'created' will be set to this value
 *		'forbiddenUpdateFieldsMultipleHits':	Array containing fields not allowed to be present in attributes if multiple products are to be updated (* is used as a wildcard)
 *		'multipleHitsErrorProductDisplayAttributes':	Array containing product attributers to be shown in the message if forbiddenUpdateFieldsMultipleHits triggers an error
 *		'filterMatchedProducts':	func. return: {products: [], err: Error(), errors: ['error1', 'error2']}
 *		'removeOldAttributes':	boolean, // Removes attributes that are not provided from a product
 *		'findByAdditionalCols': ['col1', 'col2'],	// additional columns used to find products. (findByCols OR findByAdditionalCols)
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

		fileStream.pipe(csvStream);
		csvStream.on('data', function (csvRow) {
			const	fullRow	= {};

			tasks.push(function (cb) {
				const attributes = {};
				const tasks = [];

				let	products = [];
				let	additionalProductIds = [];

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

							errors.push({
								'type': 'save error',
								'time': new Date(),
								'message': err.message
							});

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
						const returnObj = colsToESTerms(options.findByCols, attributes, mapping);
						let additionalReturnObj;

						if (returnObj.missingAttributes && returnObj.missingAttributes.length !== 0) {
							const err = new Error('findByCols: "' + returnObj.missingAttributes.join(', ') + '" entered, but product does not have this col');

							return cb(err);
						} else if (returnObj.missingMapping && returnObj.missingMapping.length !== 0) {
							const err = new Error('No mapping found for column(s) "' + returnObj.missingMapping.join(', ') + '" so it/they can not be used to find products by');

							return cb(err);
						}

						if (returnObj && returnObj.err) {
							that.log.verbose(logPrefix + returnObj.err.message);

							return cb(returnObj.err);
						}
						if (! returnObj.terms) {
							const err = new Error('No terms, can not search ES');

							return cb(err);
						}

						if (options.findByAdditionalCols) {
							additionalReturnObj = colsToESTerms(options.findByAdditionalCols, attributes, mapping);

							if (additionalReturnObj.missingAttributes && additionalReturnObj.missingAttributes.length !== 0) {
								const err = new Error('findByAdditionalCols: "' + additionalReturnObj.missingAttributes.join(', ') + '" entered, but product does not have this col');

								return cb(err);
							} else if (additionalReturnObj.missingMapping && additionalReturnObj.missingMapping.length !== 0) {
								const err = new Error('No mapping found for column(s) "' + additionalReturnObj.missingMapping.join(', ') + '" so it/they can not be used to find products by');

								return cb(err);
							}

							if (additionalReturnObj && additionalReturnObj.err) {
								that.log.verbose(logPrefix + additionalReturnObj.err.message);

								return cb(additionalReturnObj.err);
							}
						}

						const esQueryObject = {
							'method': 'POST',
							'url': 'http://' + that.es.transport._config.host + '/' + that.dataWriter.esIndexName + '/product/_msearch'
						};

						esQueryObject.headers = {};
						esQueryObject.headers['Content-Type'] = 'application/json';
						esQueryObject.body = '{}\n{"query": {"constant_score": {"filter": {"bool": {"must": ' + JSON.stringify(returnObj.terms) + '}}}}}';

						if (additionalReturnObj && additionalReturnObj.terms) {
							esQueryObject.body += '\n{}\n{"query": {"constant_score": {"filter": {"bool": {"must": ' + JSON.stringify(additionalReturnObj.terms) + '}}}}}';
						}

						esQueryObject.body += '\n';
						request(esQueryObject, function (err, response, resultStr) {
							const subTasks = [];

							let resultJson;
							let result;
							let additionalResult;

							if (err) {
								that.log.warn(logPrefix + 'es.search err: ' + err.message);

								return cb(err);
							}

							try {
								resultJson = JSON.parse(resultStr);
							// eslint-disable-next-line no-unused-vars
							} catch (err) {
								resultJson = undefined;
							}

							if (response.statusCode !== 200) {
								const reason = (resultJson && resultJson.error && resultJson.error.reason) ? resultJson.error.reason : 'unknown reason';
								const err = new Error('ES returned non-200 status code: "' + response.statusCode + '", reason: "' + reason + '"');

								that.log.warn(logPrefix + err.message);

								return cb(err);
							}

							if (! resultJson || ! resultJson.responses || ! resultJson.responses.length) {
								err = new Error('Failed to parse ES result to json. result: ' + resultStr);
								that.log.warn(logPrefix + 'es.search err: ' + err.message);

								return cb(err);
							}

							result = resultJson.responses[0];
							additionalResult = resultJson.responses.length > 1 ? resultJson.responses[1] : undefined;

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

							if (additionalResult && additionalResult.hits && additionalResult.hits.hits && additionalResult.hits.hits.length) {
								additionalProductIds = additionalResult.hits.hits.map(x => x._id);
							}

							for (const hit in result.hits.hits) {
								let product = new Product({'productLib': that.productLib, 'uuid': result.hits.hits[hit]._id});

								products.push(product);

								subTasks.push(function (cb) {
									product.loadFromDb(function (err) {
										if (err) {
											that.log.warn(logPrefix + 'Import failed, failed to load product with uuid "' + hit._id + '": ' + err.message);

											return cb(err);
										}
										cb();
									});
								});
							}

							async.series(subTasks, (err) => {
								return cb(err);
							});
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

				tasks.push(function (cb) {
					if (typeof options.filterMatchedProducts === 'function') {
						const returnObject = options.filterMatchedProducts({'products': products, 'additionalProductIds': additionalProductIds, 'findByCols': options.findByCols, 'attributes': attributes});

						if (returnObject.errors !== undefined) {
							for (const error of returnObject.errors) {
								errors.push({
									'type': 'row error',
									'time': new Date(),
									'message': error + ' - rowNr: ' + currentRowNr
								});
							}
						}

						if (returnObject.err) {
							that.log.warn(logPrefix + returnObject.err.message);

							return cb(returnObject.err);
						}

						products = returnObject.products;
					}

					if (products.length === 0 && options.noNew === true) {
						const attributeString = attributes && Object.keys(attributes).length ? Object.entries(attributes).map(x => x[0] + ': "' + x[1] + '"')
							.join(', ') : undefined;
						const err = new Error('No products found to update - ' + (attributeString ? 'attributes: ' + attributeString + ' | ' : '') + 'action: "' + options.action + '" | findByCols: "' + options.findByCols.join('", "') + '"');

						that.log.verbose(logPrefix + err.message);

						return cb(err);
					} else if (products.length === 0) {
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
					}

					if (products.length > 1 && options.forbiddenUpdateFieldsMultipleHits) {
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
								const productAttributes = options.multipleHitsErrorProductDisplayAttributes ? ' (' + options.multipleHitsErrorProductDisplayAttributes.map(function (e) { return String(e) + ': ' + products.map(x => x.attributes[e]).join(', '); }).join(' | ') + ')' : '';

								const err = new Error('Update not possible; multiple products found and "' + attr + '" is one of the attriblutes.' + productAttributes);

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

					cb();
				});

				if (typeof options.beforeAssigningAttributes === 'function') {
					tasks.push(function (cb) {
						options.beforeAssigningAttributes({'products': products, 'attributes': attributes}, cb);
					});
				}

				// Assign product attributes fix them
				tasks.push(function (cb) {
					for (let i = 0; i < products.length; i ++) {
						if (options.removeOldAttributes) {
							products[i].attributes	= attributes;
						} else {
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
