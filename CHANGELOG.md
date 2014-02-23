# v0.2-dev

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

