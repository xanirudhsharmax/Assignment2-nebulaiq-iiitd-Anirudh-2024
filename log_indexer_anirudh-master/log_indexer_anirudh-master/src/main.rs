use elasticsearch::{auth::Credentials, http::transport::Transport, BulkParts, Elasticsearch};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server running on port: 8080");

    let cloud_id =
        "cluster_name:Y2xvdWQtZW5kcG9pbnQuZXhhbXBsZSQzZGFkZjgyM2YwNTM4ODQ5N2VhNjg0MjM2ZDkxOGExYQ==";
    let credentials = Credentials::Basic("elasticsearch".into(), "write_your_own_password".into());
    let transport = Transport::cloud(cloud_id, credentials)?;

    let es_client = Elasticsearch::new(transport);

    let log_batch = Arc::new(Mutex::new(Vec::new()));

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("Accepted connection from {:?}", addr);

        let log_batch = Arc::clone(&log_batch);

        tokio::spawn(async move {
            let mut reader = BufReader::new(socket);
            let mut line = String::new();

            loop {
                tokio::select! {
                    result = reader.read_line(&mut line) => {
                        let bytes_read = result.unwrap();

                        if bytes_read == 0 {
                            break;
                        }

                        let mut logs = log_batch.lock().await;
                        logs.push(line.clone());
                        line.clear();

                        if logs.len() >= 100 {
                            println!("Indexing logs to elasticsearch");
                            logs.clear();
                        }
                    }

                    _ = sleep(Duration::from_secs(10)) => {
                        let mut logs = log_batch.lock().await;
                        if !logs.is_empty() {
                            flush_batch(&mut logs).await;
                        }
                    }
                }
            }

            println!("Connection closed");
        });
    }
}

async fn flush_batch(logs: &mut Vec<String>) {
    println!("Flushing batch of {} logs", logs.len());
    logs.clear();
}

// async fn index_logs_to_elasticsearch(
//     es_client: &Elasticsearch,
//     logs: &mut Vec<String>,
// ) -> Result<(), Box<dyn std::error::Error>> {
//     println!("Flushing batch of {} logs", logs.len());

//     let mut bulk_body = Vec::new();
//     for log in logs.iter() {
//         bulk_body.push(json!({ "index": { "_index": "logs" } }));
//         bulk_body.push(json!({ "message": log }));
//     }

//     let bulk_response = es_client
//         .bulk(BulkParts::Index("logs"))
//         .body(Value::Array(bulk_body))
//         .send()
//         .await?;

//     if bulk_response.status_code().is_success() {
//         println!("Successfully indexed batch to Elasticsearch");
//     } else {
//         eprintln!(
//             "Failed to index batch to Elasticsearch: {:?}",
//             bulk_response.status_code()
//         );
//     }

//     logs.clear();
//     Ok(())
// }
