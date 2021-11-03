use std::error::Error;
use std::str::FromStr;
use std::convert::TryInto;
use stargate_grpc::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = StargateClient::builder()

    // STARGATE
    //.uri("http://localhost:8090/")?                           // replace with a proper address
    //.auth_token(AuthToken::from_str("0e571c63-a151-42ca-8100-6680409433d0")?)    // replace with a proper token

    // ASTRA
    .uri("https://7e4b21fd-980a-46e0-a908-5e973188209e.us-east-1.apps.astra.datastax.com")?
    .auth_token(AuthToken::from_str("AstraCS:uuwizlOZhGxrUxaOqHPLAGCK:b4296e99a9f801d78043272b0efd79dca115b1fd95765780df36ed3ada87ff9b")?)                                         
    .tls(Some(client::default_tls_config()?))   // optional
    
    .connect()
    .await?;

    println!("{:?}", client);

    // let create_keyspace = Query::builder()
    //     .query("CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};")
    //     .build();

    let create_table = Query::builder()
        .keyspace("test")
        .query(
            "CREATE TABLE IF NOT EXISTS users \
                (firstname text, lastname text, PRIMARY KEY (firstname, lastname))",
        )
        .build();

    let batch = Batch::builder()
        .keyspace("test")                   // set the keyspace the query applies to
        .consistency(Consistency::One)      // set consistency level
        .query("INSERT INTO test.users (firstname, lastname) VALUES ('Lorina', 'Poland');") 
        .query("INSERT INTO test.users (firstname, lastname) VALUES ('Ronnie', 'Miller');")               
        .build();

    let query = Query::builder()
        .keyspace("test") 
        .consistency(Consistency::One) 
        .query("SELECT firstname, lastname FROM test.users")               
        .build();  

    // client.execute_query(create_keyspace).await?;
    // println!("created keyspace");

    client.execute_query(create_table).await?;
    println!("created table");

    client.execute_batch(batch).await?;
    println!("insert data");

    let response = client.execute_query(query).await?;  // send the query and wait for gRPC response
    //let response = client.execute_batch(query).await?;
    let result_set: ResultSet = response.try_into()?;   // convert the response into ResultSet

    for row in result_set.rows {
        let (firstname, lastname): (String, String) = row.try_into()?;
        println!("{} {}", firstname, lastname);
    }

    Ok(())
}