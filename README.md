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
