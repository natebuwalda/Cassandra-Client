This is an in progress implementation of a Cassandra client in Java.
It makes use of annotations to denote object-to-column relationships.
It is intended to be used with Spring, but can be used independently.  

Currently supports Cassandra 0.6.4.

Available functionality:
- CRUD operations for translating object fields into Columns
- Single column operations for modification of distinct values
- Keyspace description
- Basic random host connection pooling

Under development functionality:
- CRUD operations for SuperColumns
- Querying

Future plans:
- Pseudo-transactionality
- Better multiple host failover and connection pooling