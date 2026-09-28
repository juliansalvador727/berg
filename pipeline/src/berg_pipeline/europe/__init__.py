"""European datasets layered beside the Swiss archive, never inside it.

Switzerland keeps its schema-v3 objects at the bucket root. Every other country is a
self-contained dataset under datasets/<id>/ with the same wire contract, its own local id
spaces, and its own manifest. catalog.json at the root is the only thing that knows about
more than one of them. See europe.md.
"""
