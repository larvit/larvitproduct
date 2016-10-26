CREATE TABLE `product_products` (
  `uuid` binary(16) NOT NULL,
  `created` datetime NOT NULL DEFAULT NOW(),
  PRIMARY KEY (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin;

CREATE TABLE `product_attributes` (
  `uuid` binary(16) NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `product_product_attributes` (
  `productUuid` binary(16) NOT NULL,
  `attributeUuid` binary(16) NOT NULL,
  `data` text COLLATE utf8mb4_unicode_ci NOT NULL,
  KEY `productUuid` (`productUuid`),
  KEY `attributeUuid` (`attributeUuid`),
  CONSTRAINT `product_product_attributes_ibfk_1` FOREIGN KEY (`productUuid`) REFERENCES `product_products` (`uuid`) ON DELETE NO ACTION,
  CONSTRAINT `product_product_attributes_ibfk_2` FOREIGN KEY (`attributeUuid`) REFERENCES `product_attributes` (`uuid`) ON DELETE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
