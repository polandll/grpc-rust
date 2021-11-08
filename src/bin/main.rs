use stargate_grpc::*;
use std::str::FromStr;
use std::error::Error;
use std::convert::TryInto;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    // Set the Stargate OSS configuration for a locally running docker container:
    let sg_uri = "http://localhost:8090/";
    let auth_token = "06251024-5aeb-4200-a132-5336e73e5b6e";
    // Set the Astra DB connect information:
    let astra_uri = "https://$ASTRA_CLUSTER_ID-$ASTRA_REGION.apps.astra.datastax.com/stargate";
    let bearer_token = "AstraCS:xxxxx";

    // For Stargate OSS and Astra DB, create client:
    let mut client = StargateClient::builder()

    // For Stargate OSS running locally in docker container, set connect information:
    .uri(sg_uri)?
    .auth_token(AuthToken::from_str(auth_token)?)

    // For Astra DB, set connect information:
    // .uri(astra_uri)?
    // .auth_token(AuthToken::from_str(bearer_token)?)                                         
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