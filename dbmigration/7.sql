ALTER TABLE `product_search_index` ENGINE='InnoDB';

ALTER TABLE `product_search_index`
ADD FOREIGN KEY (`productUuid`) REFERENCES `product_products` (`uuid`) ON DELETE NO ACTION ON UPDATE NO ACTION;
