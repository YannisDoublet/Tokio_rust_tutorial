use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use mini_redis::client;

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;
 
#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>
    },
    Set {
        key: String,
        value: Bytes,
        resp: Responder<()>
    }
}



#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(32);
    let tx2 = tx.clone();
    
    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = rx.recv().await {
            use Command::*;

            match cmd {
                Get { key, resp } => {
                    let res = client.get(&key).await;
                    let _ = resp.send(res);
                }
                Set { key, value, resp } => {
                    let res = client.set(&key, value).await;
                    let _ = resp.send(res);
                }
            }
        }
    });
    
    let t1 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();

        let cmd = Command::Get { key: "hello".to_string(), resp: resp_tx };
        tx.send(cmd).await.unwrap();

        let res = resp_rx.await;
        println!("GOT: {:?}", res);
    });
    
    let t2 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();

        let cmd = Command::Set { key: "hello".to_string(), value: "world".into(), resp: resp_tx };
        tx2.send(cmd).await.unwrap();

        let res = resp_rx.await;
        println!("GOT: {:?}", res);
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
}
