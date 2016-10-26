[![Build Status](https://travis-ci.org/larvit/larvitproduct.svg?branch=master)](https://travis-ci.org/larvit/larvitproduct) [![Dependencies](https://david-dm.org/larvit/larvitproduct.svg)](https://david-dm.org/larvit/larvitproduct.svg)

# larvitproduct

Generic product module for nodejs.

Product data structure:
```json
{
	"uuid": "string",
	"created": date,
	"attributes": {
		"name": ["Conductor"],
		"price": [200],
		"available color": ["blue", "green"]
	}
}
```

## Installation

```bash
npm i --save larvitproduct
```

## Usage

### Add a new product

```javascript
const	productLib	= require('larvitproduct'),
	product	= new productLib.Product();

product.attributes = {'name': 'Conductor', 'price': 200, 'available color': ['blue', 'green']};

product.save(function(err) {
	if (err) throw err;
});
```

### Get products

```javascript
const	productLib	= require('larvitproduct'),
	products	= new productLib.Products();

products.get(function(err, productList) {
	// productList being an object with productUuid as key
});
```
