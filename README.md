# tux-pgx-scan

Easier method to scan from DB to complicated structs

# Requirements 
this library uses pgx (https://github.com/jackc/pgx) to connect to the database. it makes it a lot easier for me to unpack each row and json data properly.

# the story

I was looking for an easy to scan a query to a struct, a variable, a row , several rows, array of complicated structs.. all in one function that fits all. 

I have a project that I'm working on with gqlgen for GraphQL API (https://github.com/99designs/gqlgen) and pgx (https://github.com/jackc/pgx) to connect to a postgresql database. I still didn't find a proper solution that fits all. that I can take the structs that are being created by gqlgen as-is and scan rows directly to them. so I made this! 

# TODO
pgx is a must, so I'm not gonna change test! 

* I tested only with postgres I would like to add more test cases with other databases to make sure things are working right.
* I would also want to create profiling tests to see how slow/fast it is compared to other methods. 



