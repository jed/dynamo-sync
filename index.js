var deepEqual = require("deep-equal")

function Table(db, name) {
  this.db = db
  this.name = name
}

Table.prototype.push = function(local, cb) {
  var db = this.db
  var name = this.name

  this.diff(local, function(err, diff) {
    if (!diff.length) return cb(null, {changeCount: 0})

    var ops = diff.map(function(pair) {
      return pair[0]
        ? {PutRequest: {Item: serialize(pair[0]).M}}
        : {DeleteRequest: {Key: {path: serialize(pair[1].path)}}}
    })

    var batches = ops.reduce(function(acc, op, i) {
      if (!(i % 25)) {
        var batch = {RequestItems: {}}
        batch.RequestItems[name] = []
        acc.push(batch)
      }

      acc[acc.length - 1].RequestItems[name].push(op)
      return acc
    }, [])

    void function write() {
      var batch = batches.shift()

      if (!batch) return cb(null, {changeCount: ops.length})

      db.batchWriteItem(batch, function(err, data) {
        if (err) return cb(err)

        if (Object.keys(data.UnprocessedItems).length) {
          batches.unshift(data.UnprocessedItems)
        }

        write()
      })
    }()
  })
}

Table.prototype.load = function(cb) {
  this.db.describeTable({TableName: this.name}, function(err, data) {
    if (err) return cb(err)

    var schema = data.Table.KeySchema.map(function(item) {
      return item.AttributeName
    })

    this.compareFunction = function(a, b) {
      a = a && a[schema[0]]
      b = b && b[schema[0]]

      return a > b ? 1 : a < b ? -1 : 0
    }

    this.load = function(cb){ cb() }

    cb()
  }.bind(this))
}

Table.prototype.diff = function(local, cb) {
  this.load(function(err) {
    if (err) return cb(err)

    this.pull([], function(err, remote) {
      if (err) return cb(err)

      var zipped = zip(local, remote, this.compareFunction)
      var diff = zipped.filter(function(item) {
        return !deepEqual(item[0], item[1])
      })

      cb(null, diff)
    }.bind(this))
  }.bind(this))
}

Table.prototype.pull = function(local, cb) {
  var db = this.db

  local.length = 0

  var opts = {TableName: this.name}

  void function scan() {
    db.scan(opts, function(err, data) {
      if (err) return cb(err, items)

      for (var item, i = 0; item = data.Items[i]; i++) {
        try { local.push(parse({M: item})) }
        catch (err) { return cb(err) }
      }

      if (!data.LastEvalutatedKey) return cb(null, local)

      opts.ExclusiveStartKey = data.LastEvalutatedKey
      scan()
    })
  }()
}

function parse(value) {
  var type = Object.keys(value)[0]

  value = value[type]

  switch (type) {
    case "NULL" : return null
    case "S"    : return value
    case "B"    : return Buffer(value, "base64")
    case "BOOL" : return value
    case "N"    : return parseFloat(value, 10)
    case "L"    : return value.map(parse)
    case "M"    : return reduce(value)
  }

  throw new Error("Cannot parse " + type + ".")

  function reduce(value) {
    return Object.keys(value).reduce(function(acc, key) {
      acc[key] = parse(value[key])
      return acc
    }, {})
  }
}

function serialize(value) {
  if (value === "") throw new Error("Cannot serialize empty string.")

  var type = toString.call(value).slice(8, -1)

  switch (type) {
    case "Null"    : return {NULL: true}
    case "String"  : return {S: value}
    case "Buffer"  : return {B: value.toString("base64")}
    case "Boolean" : return {BOOL: value}
    case "Number"  : return {N: String(value)}
    case "Array"   : return {L: value.map(serialize)}
    case "Object"  : return {M: reduce(value)}
  }

  throw new Error("Cannot serialize " + type + ".")

  function reduce(value) {
    return Object.keys(value).reduce(function(acc, key) {
      acc[key] = serialize(value[key])
      return acc
    }, {})
  }
}

function zip(a, b, compareFunction) {
  if (!compareFunction) compareFunction = function(a, b) {
    return a > b ? 1 : a < b ? -1 : 0
  }

  a = a.slice(0).sort(compareFunction)
  b = b.slice(0).sort(compareFunction)

  var zipped = []

  void function zip() {
    if (!a.length && !b.length) return zipped

    var comparison = compareFunction(a[0], b[0])

    zipped.push(
      !b.length || comparison < 0 ? [a.shift(), undefined] :
      !a.length || comparison > 0 ? [undefined, b.shift()] :
                                    [a.shift(), b.shift()]
    )

    zip()
  }()

  return zipped
}

module.exports = Table
