# pg-ledger

Postgres DDLs, load scripts and tests for Evently's Postgres ledger store.

### Docker Postgres

First create a docker volume to store data:

`docker volume create postgres_data`

Use this command (I use [Fish](https://fishshell.com) so the `whoami` part may need to be replaced) to pull and run the correct version of Postgres, configured for local development:

`docker run --name evently-postgres -p 5432:5432 -e POSTGRES_USER=(whoami) -e POSTGRES_PASSWORD= -e POSTGRES_HOST_AUTH_METHOD=trust -v postgres_data:/var/lib/postgresql/data -d postgres`

You will need to open a `psql` session and create a role plus two databases.

`docker exec -it evently-postgres psql -U (whoami)`

To operate these databases, create an `evently` role for your application to use to connect.

```sql
CREATE ROLE evently WITH LOGIN PASSWORD 'evently';
```

The test database:

```sql
CREATE DATABASE evently_test WITH OWNER=evently;
ALTER DATABASE evently_test SET log_min_messages TO INFO;
```

The `evently_test` database is used for the database unit tests only. This allows for database development without damaging an existing dev database.

Now create the dev database:

```sql
CREATE DATABASE evently_dev WITH OWNER=evently;
ALTER DATABASE evently_dev SET log_min_messages TO INFO;
```

Creates a dev database for evently development. You can close your Docker session.

#### Install the schema

To install the evently schema in the dev database, run this from the command line:

## this isn't going to work unless you check in the .env file.

`npm run reset-all`

If you want to run the db tests, run `npm test`.
