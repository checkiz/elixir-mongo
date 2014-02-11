elixir-mongo
============

A [MongoDB](http://www.mongodb.org) driver in Elixir.

### Wrappers for CRUD and Aggregate operations:

- `Mongo.find(db, "anycoll", ['$maxScan': 2, '$skip': 0]) |> Enum.to_list`
- `[[a: 23], [a: 24, b: 1]] |> Mongo.insert(db, "anycoll")`
- `Mongo.update(db, "anycoll", [a: 456], [a: 123, b: 789])`
- `Mongo.remove(db, "anycoll", [b: 789])`
- `Mongo.count(db, "anycoll", [value: ['$gt': 0]])`
- `Mongo.distinct(db, "anycoll", "value", [value: ["$gt": 3]])`
- `Mongo.mr(db, "anycoll", "function(d){emit(this._id, this.value*2)}", "function(k, vs){return Array.sum(vs)}")`
- `Mongo.group(db, "anycoll", a: true)`
- `Mongo.aggregate(db, "anycoll", skip: 1, limit: 5, project: ['_id': false, value: true])`

### Other commands

- `db = Mongo.connect("anycoll")`
- `Mongo.auth(db, "testuser", "123")`
- `Mongo.getlasterror(db)`
- `Mongo.drop(db, "anycoll")`

elixir-mongo on GitHub [source repo](https://github.com/checkiz/elixir-mongo) - 
[documentation](https://checkiz.github.io/elixir-mongo)

MongoDB needs a Bson encoder/decoder, this project uses elixir-bson see [elixir-bson source repo](https://github.com/checkiz/elixir-bson) and its 
[documentation](https://checkiz.github.io/elixir-bson)