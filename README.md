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
### Create new instance of the lib
``` javascript
const {ProductLib} = require('larvitproduct');

const libOptions = {};

libOptions.log = log; // logging instance (see Log in larvitutils library)
libOptions.esIndexName  = 'anEsIndexName';
libOptions.mode = 'noSync'; // see larvitamsync library
libOptions.intercom = new Intercom('loopback interface');
libOptions.amsync = {};
libOptions.amsync.host  = null;
libOptions.amsync.minPort = null;
libOptions.amsync.maxPort = null;
libOptions.elasticsearch = es; // instance of elasticsearch.Client

const prodLib = new ProductLib(libOptions, function (err) {
	if (err) throw err;
	// ProductLib instance created!
});
```

### Add a new product
```javascript
const {ProductLib, Product} = require('larvitproduct');

// Create productLib instance of ProductLib

const product = new Product({'productLib': productLib, 'log': optionalLoggingInstance});
// Or, use the factory function in ProductLib:
const otherProduct = productLib.createProduct(); // will initiate with log instance from productLib

product.attributes = {
	'name': 'Test product #69',
	'price': 99,
	'weight': 14,
	'color': ['blue', 'green']
};

product.save(cb);
```
