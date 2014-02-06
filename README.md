elixir-mongo
============

A [MongoDB](http://www.mongodb.org) driver in Elixir.

Provides wrappers for CRUD and Aggregate operations:

- `Mongo.find("anycoll", ['$maxScan': 2, '$skip': 0])`
- `[[a: 23], [a: 24, b: 1]] |> Mongo.insert(mongo, "anycoll")`
- `Mongo.update("anycoll", [a: 456], [a: 123, b: 789])`
- `Mongo.remove("anycoll", [b: 789])`
- `Mongo.count("anycoll", [value: ['$gt': 0]])`
- `Mongo.distinct("anycoll", "value", [value: ["$gt": 3]])`
- `Mongo.mr("anycoll", "function(d){emit(this._id, this.value*2)}", "function(k, vs){return Array.sum(vs)}")`
- `Mongo.group("anycoll", a: true)`
- `Mongo.aggregate("anycoll", skip: 1, limit: 5, project: ['_id': false, value: true])`

It is very light on admin commands

- getlasterror
- drop

elixir-mongo on GitHub [source repo](https://github.com/checkiz/elixir-mongo) - 
[documentation](https://checkiz.github.io/elixir-mongo)

MongoDB needs a Bson encoder/decoder, this project uses elixir-bson see [elixir-bson source repo](https://github.com/checkiz/elixir-bson) and its 
[documentation](https://checkiz.github.io/elixir-bson)