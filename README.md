elixir-mongo
============

A [MongoDB](http://www.mongodb.org) driver in Elixir.

### Connecting:

- `mongo = Mongo.connect`
- `db = mongo.db("test")`
- `anycoll = db.collection("anycoll")`

### Wrappers for CRUD operations:

- `anycoll.find.toArray`
- `anycoll.find.stream |> Enum.to_list`
- `anycoll.find.skip(1).toArray`
- `[[a: 23], [a: 24, b: 1]] |> anycoll.insert`
- `anycoll.update([a: 456], [a: 123, b: 789])`
- `anycoll.delete([b: 789])`

### Wrappers for Aggregate operations:

- `anycoll.count([value: ['$gt': 0]])`
- `anycoll.distinct("value", [value: ["$gt": 1]])`
- `anycoll.mr("function(d){emit(this._id, this.value*2)}", "function(k, vs){return Array.sum(vs)}")`
- `anycoll.group(a: true)`
- `anycoll.aggregate(skip: 1, limit: 5, project: ['_id': false, value: true])`

### Other commands

- `db.auth("testuser", "123")`
- `db.getLastError`
- `db.collection("anycoll").drop`

elixir-mongo on GitHub [source repo](https://github.com/checkiz/elixir-mongo) - 
[documentation](https://checkiz.github.io/elixir-mongo)

MongoDB needs a Bson encoder/decoder, this project uses elixir-bson see [elixir-bson source repo](https://github.com/checkiz/elixir-bson) and its 
[documentation](http://checkiz.github.io/elixir-bson)
