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

	tasks.push(function (cb) {
		data.product	= new productLib.Product(data.global.urlParsed.query.uuid);
		data.product.loadFromDb(cb);
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
			const	slugs	= [];

			if ( ! req.formFiles || ! req.formFiles.newImage) {
				return cb();
			}

			missingImgSlug	= 'product_' + data.product.uuid + '_01';

			for (let i = 1; i !== 100; i ++) {
				slugs.push('product_' + data.product.uuid + '_' + leftPad(i, 2, '0') + '.jpg');
				slugs.push('product_' + data.product.uuid + '_' + leftPad(i, 2, '0') + '.png');
				slugs.push('product_' + data.product.uuid + '_' + leftPad(i, 2, '0') + '.gif');
			}

console.log('first slugs');
console.log(slugs[0]);
console.log(slugs[1]);
console.log(slugs[2]);

			imgLib.getImages({'slugs': slugs, 'limit': 100}, function (err, list) {
				if (err) return cb(err);

console.log('found images:');
				console.log(list);

				cb();
			});
		});

		// Save images - write to disk
		tasks.push(function (cb) {
			const	imgOptions	= {};

			if ( ! req.formFiles || ! req.formFiles.newImage) {
				return cb();
			}
console.log('setting slug: ' + missingImgSlug);
			// Set new image slug etc
			imgOptions.slug	= missingImgSlug;
			imgOptions.metadata	= [{ 'name': 'description', 'data': data.global.formFields.newImageDesc }];
			imgOptions.file	= req.formFiles.newImage;

			if (['image/jpeg', 'image/png', 'image/gif'].indexOf(req.formFiles.newImage.type) === - 1) {
				data.global.errors.push('Invalid image type, must be jpeg, png or gif');

				if (res.statusCode === 302) {
					req.session.data.nextCallData.global.errors	= ['Invalid image type, must be jpeg, png or gif'];
				}

				return cb();
			}
console.log('slug just before save: ' + imgOptions.slug);
			imgLib.saveImage(imgOptions, cb);
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

	async.series(tasks, function (err) {
		cb(err, req, res, data);
	});
};
