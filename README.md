elixir-mongo
============

A [MongoDB](http://www.mongodb.org) driver in Elixir.

Document are exchanged using `Maps`.

### Connecting

Example preparing access to the `anycoll` collection in the `test` db :
```elixir
# Connect the mongo server (by default port 27017 at 127.0.0.1)
mongo = Mongo.connect!
# Select the db to access  
db = mongo.db("test")  
# Select the collection to access
anycoll = db.collection("anycoll")  
```

### Wrappers for CRUD operations

Examples accessing the `anycoll` collection via CRUD operations :

```elixir
# Return the list of all docs in the collection (list of Maps)
anycoll.find.toArray   
anycoll.find.stream |> Enum.to_list   # Same as above
anycoll.find.skip(1).toArray          # Same as above but skip first doc
# Insert a list of two docs into the collection
[%{a: 23}, %{a: 24, b: 1}] |> anycoll.insert  
# Updates the doc matching "a" == 456 with new values
anycoll.update(%{a: 456}, %{a: 123, b: 789})  
# Delete the document matching "b" == 789
anycoll.delete(%{b: 789}) 
```

### Wrappers for Aggregate operations

Example of aggregate operation applied to the `anycoll` collection :

```elixir
# Return docs with "value" > 0
anycoll.count(%{value: %{'$gt': 0}}) 
# Return distinct "value" for docs with "value" > 0
anycoll.distinct("value", %{value: %{"$gt": 1}})  
# Apply a map-reduce to the collection specifying the map function then the apply function
anycoll.mr("function(d){emit(this._id, this.value*2)}", "function(k, vs){return Array.sum(vs)}")
# Groups documents in a collection by the specified keys
anycoll.group(a: true)
# Aggregate operation
anycoll.aggregate([
  %{'$skip': 1},    # skip the first doc
  %{'$limit': 5},   # take five
  %{'$project': %{'_id': false, value: true}} # project : select only "_id" and "value"
])
# Drop a collection
db.collection("anycoll").drop
```

### Other commands

```elixir
# Authenticate against the db
db.auth("testuser", "123")`
# Retrieve the last error
db.getLastError
```

### Documentation

- [documentation](http://checkiz.github.io/elixir-mongo)

### Dependencies

- MongoDB needs a Bson encoder/decoder, this project uses the elixir-bson encoder/decoder. See [elixir-bson source repo](https://github.com/checkiz/elixir-bson) and its 
[documentation](http://checkiz.github.io/elixir-bson)