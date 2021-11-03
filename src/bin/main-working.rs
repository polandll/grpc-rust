use std::error::Error;
use std::str::FromStr;
use std::convert::TryInto;
use stargate_grpc::*;

#[tokio::working]
async fn working() -> Result<(), Box<dyn Error>> {
    let mut client = StargateClient::builder()

    // STARGATE
    .uri("http://localhost:8090/")?                           // replace with a proper address
    .auth_token(AuthToken::from_str("0e571c63-a151-42ca-8100-6680409433d0")?)    // replace with a proper token

    // ASTRA
    //.uri("https://28fafa19-84f0-4f36-a42f-b360d3237a94-us-east-1.apps.astra.datastax.com/stargate")?
    //.auth_token(AuthToken::from_str("AstraCS:mwlftanNZvRZTiPAhfeMwzZF:b499d86052cf42cd691fa7590f2adf4ce757d84d5f84174e836e53b0b5b8f3eb")?)                                         
    //.tls(Some(client::default_tls_config()?))   // optional
    
    .connect()
    .await?;

    println!("{:?}", client);

    let create_keyspace = Query::builder()
        .query("CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};")
        .build();

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

    // THIS IS INTERESTING!!
    // use stargate_grpc::{Query, Consistency};
    //
    // let query_defaults = Query::builder()
    //     .keyspace("ks")
    //     .consistency(Consistency::LocalQuorum);
    //
    // let query1 = query_defaults.clone().query("SELECT * FROM table1").build();
    // let query2 = query_defaults.clone().query("SELECT * FROM table2").build();


    client.execute_query(create_keyspace).await?;
    println!("created keyspace");

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