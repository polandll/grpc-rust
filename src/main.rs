use std::error::Error;
use std::str::FromStr;
use std::convert::TryInto;
use stargate_grpc::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = StargateClient::builder()

    // Set connect information for STARGATE OSS running in docker container
    //.uri("http://localhost:8090/")?                           // replace with a proper address
    //.auth_token(AuthToken::from_str("0e571c63-a151-42ca-8100-6680409433d0")?)    // replace with a proper token

    // Set connect information for ASTRA DBaaS
    .uri("https://a2b4465c-e7a4-4cb7-a4a4-c829f0ef10d6-us-west1.apps.astra.datastax.com/stargate")?
    .auth_token(AuthToken::from_str("AstraCS:uuwizlOZhGxrUxaOqHPLAGCK:b4296e99a9f801d78043272b0efd79dca115b1fd95765780df36ed3ada87ff9b")?)                                         
    .tls(Some(client::default_tls_config()?))   // optional
    
    .connect()
    .await?;

    println!("{:?}", client);

    // Create a keyspace - only works for Stargate OSS, not Astra
    // let create_keyspace = Query::builder()
    //     .query("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};")
    //     .build();

    // Create a table
    let create_table = Query::builder()
        //.keyspace("test")
        .query(
            "CREATE TABLE IF NOT EXISTS test.users \
                (firstname text, lastname text, PRIMARY KEY (firstname, lastname));",
        )
        .build();

    // Insert some rows/records
    let batch = Batch::builder()
        //.keyspace("test")                   // set the keyspace the query applies to
        //.consistency(Consistency::One)      // set consistency level
        .query("INSERT INTO test.users (firstname, lastname) VALUES ('Lorina', 'Poland');") 
        .query("INSERT INTO test.users (firstname, lastname) VALUES ('Ronnie', 'Miller');")               
        .build();

    // Select/query some data from the keyspace.table
    let query = Query::builder()
        //.keyspace("test") 
        //.consistency(Consistency::One) 
        .query("SELECT firstname, lastname FROM test.users;")               
        .build();  

    // Actually create the keyspace if Stargate OSS with ExecuteQuery
    // client.execute_query(create_keyspace).await?;
    // println!("created keyspace");

    // Create the table with ExecuteQuery
    client.execute_query(create_table).await?;
    println!("created table");

    // Insert data with ExecuteBatch
    client.execute_batch(batch).await?;
    println!("insert data");

    // Query the data and return a response that includes all data selected
    let response = client.execute_query(query).await?;  // send the query and wait for gRPC response
    println!("query sent");
    
    // Process the data response
    let result_set: ResultSet = response.try_into()?;   // convert the response into ResultSet

    for row in result_set.rows {
        let (firstname, lastname): (String, String) = row.try_into()?;
        println!("{} {}", firstname, lastname);
    }

    Ok(())
}