# v0.5.0
* complete rework of the API
  * drop the Mongodb-like notation in favor of a more Elixir syntax
  * no more use of records
  * implements Enumerable protocol for:
    * %Mongo.Find{}: to retrieve all docs of a query
    * %Mongo.Response{}: to retrieve all docs of a particular batch (specific use)
    * %Mongo.Cursoer{}: to retrive all batches (specific use)

  in this version you write this:

  ```elixir
  coll = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
  ```
  rather than:

  ```elixir
  coll = Mongo.connect!.db("test").collection("anycoll")
  ```

  see https://github.com/checkiz/elixir-mongo/issues/11

# v0.4.0
* compatible with Elixir v1.0.0
* elixir-bson v0.4.0

# v0.3.1
* compatible with Elixir v0.15.1

# v0.3
* compatible with Elixir v0.14.1

# v0.2

* Enhancements
  * Mongo.Cursor: module to interact with MongoDB cursors
  * Authentication
  * Allows to mode to get message back from MongoDB
  	* passive: the drivers controls when to fetch responses (for sync calls)
  	* active: MongoDB sends message back directly (allows assync calls)
  * getLastError, getPrevError

* Bug fixes

* Deprecations

* Backwards incompatible changes
  * A major revamp of the API was necessary
