'use strict';

const	productLib	= require(__dirname + '/../../index.js'),
	leftPad	= require('left-pad'),
	imgLib	= require('larvitimages'),
	async	= require('async'),
	log	= require('winston');

exports.run = function (req, res, cb) {
	const	logPrefix	= 'larvitproduct: ./controllers/products/edit.js: run() - ',
		tasks	= [],
		data	= {'global': res.globalData};

	// Make sure the user have the correct rights
	// This is set in larvitadmingui controllerGlobal
	if ( ! res.adminRights) return cb(new Error('Invalid rights'), req, res, {});

	data.global.menuControllerName	= 'products';
	data.global.messages	= [];
	data.global.errors	= [];

	data.productAttributes = [];

	tasks.push(function (cb) {
		data.product	= new productLib.Product(data.global.urlParsed.query.uuid);
		data.product.loadFromDb(cb);
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

	// Get all available keywords
	tasks.push(function (cb) {
		productLib.helpers.getKeywords(function (err, keywords) {
			if (err) return cb(err);

			keywords.sort(function (a, b) {
				return a.localeCompare(b, 'en', {'sensitivity': 'base'});
			});

			for (let kw of keywords) {
				kw = kw.replace('.keyword', '');
				data.productAttributes.push(kw);
			}

			cb();
		});
	});

	if (data.global.formFields.save !== undefined) {
		let	missingImgSlug;

		// Write product to database
		tasks.push(function (cb) {
			data.product.attributes	= {};

			// Handle product attributes
			for (let i = 0; data.global.formFields.attributeName[i] !== undefined; i ++) {
				const	attributeName	= data.global.formFields.attributeName[i],
					attributeValue	= data.global.formFields.attributeValue[i];

				if (attributeName && attributeValue !== undefined) {
					if (data.product.attributes[attributeName] === undefined) {
						data.product.attributes[attributeName] = [];
					}

					data.product.attributes[attributeName].push(attributeValue);
				}
			}

			// Save product
			data.product.save(function (err) {
				if (err) return cb(err);

				if (data.product.uuid !== undefined && data.global.urlParsed.query.uuid === undefined) {
					log.verbose(logPrefix + 'New product created, redirect to new uuid: "' + data.product.uuid + '"');
					req.session.data.nextCallData	= {'global': {'messages': ['New product created']}};
					res.statusCode	= 302;
					res.setHeader('Location', '/products/edit?uuid=' + data.product.uuid);
				} else {
					data.global.messages = ['Product data saved'];
				}

				cb();
			});
		});

		// Save images - find out last image number
		tasks.push(function (cb) {
			const	takenNumbers	= [];

			let	testNum	= 1;

			if ( ! req.formFiles || ! req.formFiles.newImage) {
				return cb();
			}

			for (let i = 0; data.product.images[i] !== undefined; i ++) {
				const	img	= data.product.images[i],
					curNum	= Number(img.slug.substring(45, img.slug.length - 4));

				takenNumbers.push(curNum);
			}

			while (takenNumbers.indexOf(testNum) !== - 1) {
				testNum ++;
			}

			missingImgSlug = 'product_' + data.product.uuid + '_' + leftPad(testNum, 2, '0');

			cb();
		});

		// Save images - write to disk
		tasks.push(function (cb) {
			const	imgOptions	= {};

			if ( ! req.formFiles || ! req.formFiles.newImage) {
				return cb();
			}

			// Set new image slug etc
			imgOptions.slug	= missingImgSlug;
			imgOptions.metadata	= [{ 'name': 'description', 'data': data.global.formFields.newImageDesc }];
			imgOptions.file	= req.formFiles.newImage;

			if (req.formFiles.newImage.type === 'image/jpeg') {
				imgOptions.slug += '.jpg';
			} else if (req.formFiles.newImage.type === 'image/png') {
				imgOptions.slug += '.png';
			} else if (req.formFiles.newImage.type === 'image/gif') {
				imgOptions.slug += '.gif';
			} else {
				data.global.errors.push('Invalid image type, must be jpeg, png or gif');

				if (res.statusCode === 302) {
					req.session.data.nextCallData.global.errors	= ['Invalid image type, must be jpeg, png or gif'];
				}

				return cb();
			}

			imgLib.saveImage(imgOptions, cb);
		});

		// Reload from database
		tasks.push(function (cb) {
			data.product.loadFromDb(cb);
		});
	}

	if (data.global.formFields.rmProduct !== undefined) {
		tasks.push(function (cb) {
			log.verbose(logPrefix + 'Removing product, uuid: "' + data.product.uuid + '"');
			data.product.rm(function (err) {
				if (err) return cb(err);

				req.session.data.nextCallData	= {'global': {'messages': ['Product removed: ' + data.product.uuid]}};
				res.statusCode	= 302;
				res.setHeader('Location', '/products/list');
				cb();
			});
		});
	}

	if (data.global.formFields.rmImage !== undefined) {
		tasks.push(function (cb) {
			imgLib.rmImage(data.global.formFields.rmImage, cb);
		});
	}

	// Load images
	//	if (data.global.urlParsed.query.uuid) {
	//		tasks.push(function (cb) {
	//			productLib.getImagesForEsResult(data.product)
	//
	//
	//
	//			getImageList(function (err, imageList) {
	//				data.imageList	= imageList;
	//				cb(err);
	//			});
	//		});
	//	}

	async.series(tasks, function (err) {
		cb(err, req, res, data);
	});
};
