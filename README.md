dynamo-sync
===========

Differential data synchronization for Amazon's DynamoDB

Example
-------

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

API
---

### table = new Table(db, name)

Creates a new table, where `db` is a DynamoDB instance from the `aws-sdk` library, and `name` is the name of the table.

### table#pull(local, cb)

Fills the `local` array with the contents of the remote table.

### table#push(local, cb)

Diffs the `local` array with the remote table, and uses the `BatchWriteItem` API to issue writes only for keys that have changed. Local keys not present remotely will be put, and remote keys not present locally will be deleted.
