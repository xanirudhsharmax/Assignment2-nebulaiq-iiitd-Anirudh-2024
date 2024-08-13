use std::env;
use std::error::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_addr = "127.0.0.1:8080";
    let message_rate = env::args()
        .nth(1)
        .unwrap_or("10".to_string())
        .parse::<u64>()
        .unwrap(); // messages per second
    let total_messages = env::args()
        .nth(2)
        .unwrap_or("100".to_string())
        .parse::<u64>()
        .unwrap();

    loop {
        match connect_and_send(server_addr, message_rate, total_messages).await {
            Ok(_) => {
                println!("Finished sending all messages. Restarting...");
            }
            Err(e) => {
                println!("Error occurred: {}. Attempting to reconnect...", e);
            }
        }

        sleep(Duration::from_secs(5)).await;
    }
}

async fn connect_and_send(
    server_addr: &str,
    message_rate: u64,
    total_messages: u64,
) -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect(server_addr).await?;
    println!("Connected to server at {}", server_addr);

    let delay = Duration::from_secs_f64(1.0 / message_rate as f64);

    for i in 0..total_messages {
        let message = format!("Log message {}\n", i + 1);
        stream.write_all(message.as_bytes()).await?;
        println!("Sent: {}", message.trim());

        sleep(delay).await;
    }

    println!("All messages sent");
    Ok(())
}
