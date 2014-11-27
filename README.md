dynamo-sync
===========

Differential data synchronization for Amazon's DynamoDB

```javascript
var aws = require("aws-sdk")
var Table = require("dynamo-sync").Table

var db = new aws.DynamoDB
var table = Table(db, "myTable")

var data = [
  {id: 1, name: "Jed"},
  {id: 2, name: "Michael"},
  {id: 3, name: "Martin"}
]

table.push(data, function(err){ if (err) throw err })
```
