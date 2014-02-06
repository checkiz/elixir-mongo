elixir-mongo
============

A [MongoDB](http://www.mongodb.org) driver in Elixir.

Provides wrappers for CRUD and Aggregate operations:

- find
- insert
- update
- remove
- count
- distinct
- mr (mapReduce)
- group
- aggregate

It is very light on admin commands

- getlasterror
- drop

elixir-mongo on GitHub [source repo](https://github.com/checkiz/elixir-mongo) - 
[documentation](https://checkiz.github.io/elixir-mongo)

MongoDB needs a Bson encoder/decoder, this project uses elixir-bson see [elixir-bson source repo](https://github.com/checkiz/elixir-bson) and its 
[documentation](https://checkiz.github.io/elixir-bson)