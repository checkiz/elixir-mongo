elixir-mongo
============
[![Build Status](https://travis-ci.org/checkiz/elixir-mongo.png?branch=master)](https://travis-ci.org/checkiz/elixir-mongo)
[![Hex Version](https://img.shields.io/hexpm/v/mongo.svg)](https://hex.pm/packages/mongo)

A [MongoDB](http://www.mongodb.org) driver in Elixir.

Document are exchanged using `Maps`.

### Connecting

Example preparing access to the `anycoll` collection in the `test` db :
```elixir
# Connect the mongo server (by default port 27017 at 127.0.0.1)
mongo = Mongo.connect!
# Select the db to access  
db = mongo |> Mongo.db("test")  
# Select the db to access
anycoll = db |> Mongo.Db.collection("anycoll")  
```

### Wrappers for CRUD operations

Examples accessing the `anycoll` collection via CRUD operations see `Mongo.Find`


### Wrappers for Aggregate operations

Example of aggregate operation applied to the `anycoll` collection see `Mongo.Collection`

### Other commands

```elixir
# Authenticate against the db
db |> Mongo.auth("testuser", "123")`
# Retrieve the last error
db |> Mongo.getLastError
```

### Documentation

- [documentation](http://checkiz.github.io/elixir-mongo)

### Dependencies

- MongoDB needs a Bson encoder/decoder, this project uses the elixir-bson encoder/decoder. See [elixir-bson source repo](https://github.com/checkiz/elixir-bson) and its
[documentation](http://checkiz.github.io/elixir-bson)
