use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the database.
    let (client, connection) =
      tokio_postgres::connect("host=localhost user=postgres password=password dbname=miniswap", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(())
}



// Steps:
// db/
//  migrations/
//  ephemeral/
//      views/
//      functions/
//      triggers/


/**
When the migration process begins, all current servers should stop accepting connections.

start transaction;
drop schema prod;
alter schema dev rename to prod;
end transaction;

DROP SCHEMA would attempt obtaining exclusive use of the schema first, so it would only actually manage to drop the schema when PostgresSQL is done retrieving data from ongoing queries.
Further queries would likely be locked until the end of the transaction in your question.

We need to clear the type cache of the client server IMMEDIATELY after the schema replacement occurs.

Deploy sequence:
1. Create ephemeral_new with all new objects
2. Deploy new Rust code that references ephemeral_new
3. Gradually shift traffic to new instances
4. Once old instances are drained, drop ephemeral (old)
5. Rename ephemeral_new to ephemeral
6. Deploy again to use ephemeral name

Blue-green database migrations. On startup of new version of rust server:
1. In one transaction: run data table migrations, create `api_{timestamp}` schema with all new objects.
2. The new server has its search path set to `api_{timestamp},api`. The old server continues to operate on the previous schema.
3. The old server is drained of connections.
4. In one transaction: the old `api` schema is dropped and `api_{timestamp}` renamed to `api`.
5. The new server may continue operating, as postgres will skip `api_{timestamp}` in the search path if it doesn't exist.

If I migrate something in the data schema that breaks an old version's function, that's unavoidable.
Requires downtime.

The big problem
**/
fn hello() {

}