use std::{str::FromStr, time::Duration};

use futures::stream::StreamExt;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_types::config::RpcTransactionLogsConfig;
use thiserror::Error;
use tokio::time::timeout;

use crate::feed::{feed::Feed, logs::log::HasLogsFilter};

pub type Result<T> = core::result::Result<T, Error>;

impl<T> Feed<T>
where
    T: FromStr + HasLogsFilter + Send + Sync + Clone + 'static,
{
    pub async fn subscribe(
        self,
        ws_url: String,
        tx_config: RpcTransactionLogsConfig,
    ) -> Result<()> {
        loop {
            println!("connecting...");

            let res = async {
                let client = PubsubClient::new(&ws_url).await?;
                let (mut log_notification, log_unsubscribe) =
                    PubsubClient::logs_subscribe(&client, T::logs_filter(), tx_config.clone())
                        .await?;

                loop {
                    match timeout(Duration::from_secs(30), log_notification.next()).await {
                        Ok(Some(log_info)) => {
                            if log_info.value.err.is_some() {
                                continue;
                            }

                            for log in log_info.value.logs {
                                if let Ok(event) = T::from_str(&log) {
                                    if self.tx.send((log_info.context.slot, event)).await.is_err() {
                                        println!("receiver dropped");
                                        let _ = log_unsubscribe().await;
                                        return Ok(());
                                    }
                                } else {
                                    // println!("fucking ee");
                                }
                            }
                        }

                        Ok(None) => {
                            println!("stream closed");
                            break;
                        }

                        Err(_) => {
                            println!("stream timeout");
                            break;
                        }
                    }
                }

                let _ = log_unsubscribe().await;
                Ok::<(), Error>(())
            }
            .await;

            match res {
                Ok(_) => println!("reconnecting..."),
                Err(e) => println!("stream error: {e}, reconnecting..."),
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Solana websocket error: {0}")]
    PubSub(#[from] solana_pubsub_client::pubsub_client::PubsubClientError),
}
