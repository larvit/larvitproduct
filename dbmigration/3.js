'use strict';

const	async	= require('async'),
	db	= require('larvitdb'),
	lUtils	= require('larvitutils');

exports = module.exports = function (cb) {
	const	tasks	= [];

	// Create the mapping table
	tasks.push(function (cb) {
		let	sql	= '';

		sql += 'CREATE TABLE IF NOT EXISTS `product_image_mapping` (\n';
		sql += '	`productUuid` binary(16) NOT NULL,\n';
		sql += '	`imageUuid` binary(16) NOT NULL,\n';
		sql += '	UNIQUE KEY `product_image` (`productUuid`, `imageUuid`),\n';
		sql += '	FOREIGN KEY (`imageUuid`) REFERENCES `images_images` (`uuid`) ON DELETE NO ACTION\n';
		sql += ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;';

		db.query(sql, cb);
	});

	// Fill the mapping table with data
	tasks.push(function (cb) {
		db.query('SELECT uuid, slug FROM images_images WHERE slug LIKE "product_%" AND LENGTH(slug) >= 44', function (err, rows) {
			const	tasks	= [];

			if (err) return cb(err);

			for (let i = 0; rows[i] !== undefined; i ++) {
				const	row	= rows[i],
					imageUuid = row.uuid,
					productUuid = lUtils.uuidToBuffer(row.slug.substring(8, 44));

				tasks.push(function (cb) {
					db.query('INSERT INTO product_image_mapping (productUuid, imageUuid) VALUES(?,?);',
						[ productUuid, imageUuid ],
						cb);
				});
			}

			async.parallelLimit(tasks, 20, cb);
		});
	});

	async.series(tasks, cb);
};
