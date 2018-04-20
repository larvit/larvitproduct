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
const	elasticsearch	= require('elasticsearch'),
	productLib	= require('larvitproduct'),
	product	= new productLib.Product();

productLib.dataWriter.elasticsearch	= new elasticsearch.Client({'host': '127.0.0.1:9200'});
productLib.dataWriter.esIndexName	= 'theIndexToUse';

product.attributes = {'name': 'Conductor', 'price': 200, 'available color': ['blue', 'green']};

product.save(function (err) {
	if (err) throw err;
});
```

### Get products

```javascript
const	elasticsearch	= require('elasticsearch'),
	productLib	= require('larvitproduct'),
	products	= new productLib.Products();

productLib.dataWriter.elasticsearch	= new elasticsearch.Client({'host': '127.0.0.1:9200'});
productLib.dataWriter.esIndexName	= 'theIndexToUse';

products.get(function (err, productList) {
	// productList being an object with productUuid as key
});
```

### Highjack datawriter to fill up database before getting data from the queue

__THIS MIGHT BE BROKEN__


```javascript
const	EventEmitter	= require('events').EventEmitter,
	eventEmitter	= new EventEmitter(),
	productLib	= require('larvitproduct'),
	oldReady	= productLib.dataWriter.ready,
	async	= require('async'); // npm i --save async

let	readyInProgress	= false,
	isReady	= false;

productLib.dataWriter.ready = function (cb) {
	const	tasks	= [];

	if (isReady === true) { cb(); return; }

	if (readyInProgress === true) {
		eventEmitter.on('ready', cb);
		return;
	}

	readyInProgress = true;

	// Do async stuff here that have to happend before the first message
	// from the queue is written to the database
	tasks.push(function (cb) {
		// do stuff
		cb();
	});

	// Run the original ready function
	tasks.push(oldReady);

	async.series(tasks, function () {
		isReady	= true;
		eventEmitter.emit('ready');
		cb();
	});
}
```
