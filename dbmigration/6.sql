CREATE TABLE `product_search_index` (
  `productUuid` binary(16) NOT NULL,
  `content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`productUuid`),
  FULLTEXT KEY `psi` (`content`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO product_search_index (productUuid, content) SELECT productUuid, GROUP_CONCAT(data SEPARATOR ' ') FROM product_product_attributes GROUP BY productUuid;

ALTER TABLE `product_product_attributes` DROP INDEX `ppax`;
