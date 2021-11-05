use std::error::Error;
use std::str::FromStr;
use std::convert::TryInto;
use stargate_grpc::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    // For Stargate OSS and Astra DB, create client:
    let mut client = StargateClient::builder()

    // For Stargate OSS running locally in docker container, set connect information:
    .uri("http://localhost:8090/")?                           // replace with a proper address
    .auth_token(AuthToken::from_str("721e9c04-e121-4bf4-b9a6-887ebeae2bc5")?)    // replace with a proper token

    // For Astra DB, set connect information:
    // .uri("https://a2b4465c-e7a4-4cb7-a4a4-c829f0ef10d6-us-west1.apps.astra.datastax.com/stargate")?
    // .auth_token(AuthToken::from_str("AstraCS:uuwizlOZhGxrUxaOqHPLAGCK:b4296e99a9f801d78043272b0efd79dca115b1fd95765780df36ed3ada87ff9b")?)                                         
    // .tls(Some(client::default_tls_config()?))   // optional
    
    .connect()
    .await?;

    println!("created client {:?}", client);

    // For Stargate OSS only: create a keyspace
    // For Astra DB: delete and create a keyspace in the Astra dashboard
    let create_keyspace = Query::builder()
        .query("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};")
        .build();
    client.execute_query(create_keyspace).await?;

    println!("created keyspace");

    // For Stargate OSS and Astra DB: create a table
    let create_table = Query::builder()
        // .keyspace("test")
        .query(
            "CREATE TABLE IF NOT EXISTS test.users \
                (firstname text, lastname text, PRIMARY KEY (firstname, lastname));",
        )
        .build();
     client.execute_query(create_table).await?;

     println!("created table");

    // For Stargate OSS and Astra DB: INSERT two rows/records
	//  Two queries will be run in a batch statement
    let batch = Batch::builder()
        .keyspace("test")                   // set the keyspace the query applies to
        .consistency(Consistency::One)      // set consistency level
        .query("INSERT INTO test.users (firstname, lastname) VALUES ('Lorina', 'Poland');") 
        .query("INSERT INTO test.users (firstname, lastname) VALUES ('Doug', 'Wettlaufer');")               
        .build();
    client.execute_batch(batch).await?;

    println!("insert data");

    // For Stargate OSS and Astra DB: SELECT the data to read from the table
    // Select/query some data from the keyspace.table
    let query = Query::builder()
        .keyspace("test") 
        .consistency(Consistency::One) 
        .query("SELECT firstname, lastname FROM test.users;")               
        .build();  

     println!("select executed");

    // Get the results from the execute query statement and convert into a ResultSet
    let response = client.execute_query(query).await?;
    let result_set: ResultSet = response.try_into()?;   

    // This for loop to get the results
    for row in result_set.rows {
        let (firstname, lastname): (String, String) = row.try_into()?;
        println!("{} {}", firstname, lastname);
    }
    println!("everything worked!");
    Ok(())
}