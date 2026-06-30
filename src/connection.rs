use std::time::Duration;

use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::monitor::Monitor;
use crate::phoenix::PhxMessage;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;
type WsReader = SplitStream<WsStream>;
type Tx = mpsc::Sender<Message>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

const TOPIC: &str = "agent_smith";
const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const UUID_FILE: &str = "agent_smith_lite.uuid";
const MONITOR_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const MAX_JOIN_ATTEMPTS: u32 = 10;

pub async fn run(
    secret: &str,
    endpoint: &str,
    initial_uuid: Option<&str>,
    accelerator_type: &str,
) -> Result<(), BoxError> {
    let mut uuid = load_uuid(initial_uuid);
    let url = build_url(endpoint);

    let (ws, _) = connect_async(&url).await?;
    info!("WebSocket connected to {}", url);

    let (sink, mut stream) = ws.split();

    let (tx, rx) = mpsc::channel::<Message>(64);
    let writer_handle = tokio::spawn(writer_task(sink, rx));

    let (topic, join_ref) = do_join(&mut stream, &tx, &mut uuid, secret, accelerator_type).await?;
    info!("Joined Phoenix channel: {} (join_ref={})", topic, join_ref);

    let heartbeat_handle = tokio::spawn(heartbeat_loop(tx.clone()));
    let monitor_handle = tokio::spawn(monitor_loop(tx.clone(), topic.clone(), join_ref.clone()));

    loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => {
                handle_incoming(&text, &tx, &topic, &join_ref);
            }
            Some(Ok(Message::Ping(data))) => {
                tx.try_send(Message::Pong(data)).ok();
            }
            Some(Ok(Message::Close(_))) => {
                info!("Server closed the WebSocket connection");
                break;
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                heartbeat_handle.abort();
                monitor_handle.abort();
                writer_handle.abort();
                return Err(Box::new(e));
            }
            None => break,
        }
    }

    heartbeat_handle.abort();
    monitor_handle.abort();
    writer_handle.abort();
    Ok(())
}

async fn writer_task(mut sink: WsSink, mut rx: mpsc::Receiver<Message>) {
    while let Some(msg) = rx.recv().await {
        if let Err(e) = sink.send(msg).await {
            error!("WebSocket write error: {}", e);
            break;
        }
    }
}

async fn do_join(
    stream: &mut WsReader,
    tx: &Tx,
    uuid: &mut Option<String>,
    secret: &str,
    accelerator_type: &str,
) -> Result<(String, String), BoxError> {
    for attempt in 1..=MAX_JOIN_ATTEMPTS {
        let topic = format!("{}:{}", TOPIC, uuid.as_deref().unwrap_or("lobby"));

        let join_ref = format!("join_{}", attempt);
        let join_msg = PhxMessage::new(
            Some(&join_ref),
            Some(&join_ref),
            &topic,
            "phx_join",
            build_join_payload(secret, accelerator_type),
        );
        tx.try_send(Message::Text(join_msg.serialize().into()))
            .map_err(|e| format!("channel send error: {}", e))?;
        info!(
            "phx_join → {} (attempt {}, join_ref={})",
            topic, attempt, join_ref
        );

        loop {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => match PhxMessage::deserialize(&text) {
                    Ok(phx) if phx.event == "phx_reply" && phx.topic == topic => {
                        let status = phx.payload["status"].as_str().unwrap_or("");
                        match status {
                            "ok" => {
                                info!("phx_reply ok on {}", topic);
                                return Ok((topic, join_ref));
                            }
                            "error" => {
                                let reason = phx.payload["response"]["reason"]
                                    .as_str()
                                    .unwrap_or("unknown");
                                if reason == "agent_id_required" {
                                    let new_id = phx.payload["response"]["new_agent_id"]
                                        .as_str()
                                        .unwrap_or("");
                                    if new_id.is_empty() {
                                        return Err("server returned empty agent_id".into());
                                    }
                                    info!("Assigned new agent UUID: {}", new_id);
                                    *uuid = Some(new_id.to_owned());
                                    save_uuid(new_id);
                                    break;
                                } else {
                                    return Err(format!("join rejected: {}", reason).into());
                                }
                            }
                            other => {
                                warn!("Unexpected phx_reply status '{}' — ignoring", other);
                            }
                        }
                    }
                    Ok(phx) => {
                        debug!("Ignoring {} {} during join", phx.topic, phx.event);
                    }
                    Err(e) => {
                        warn!("Unparseable message during join: {}", e);
                    }
                },
                Some(Ok(Message::Ping(data))) => {
                    tx.try_send(Message::Pong(data)).ok();
                }
                Some(Ok(Message::Close(_))) => {
                    return Err("connection closed during join".into());
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(Box::new(e)),
                None => return Err("stream ended during join".into()),
            }
        }
    }

    Err(format!("exceeded {} join attempts", MAX_JOIN_ATTEMPTS).into())
}

async fn heartbeat_loop(tx: Tx) {
    let mut interval = time::interval(HEARTBEAT_INTERVAL);
    let mut ref_num: u64 = 1;

    loop {
        interval.tick().await;
        let msg = PhxMessage::new(
            None,
            Some(&ref_num.to_string()),
            "phoenix",
            "heartbeat",
            json!({}),
        );
        if tx.try_send(Message::Text(msg.serialize().into())).is_err() {
            break;
        }
        debug!("Sent heartbeat (ref {})", ref_num);
        ref_num += 1;
    }
}

async fn monitor_loop(tx: Tx, topic: String, join_ref: String) {
    let mut monitor = tokio::task::spawn_blocking(Monitor::new)
        .await
        .expect("Monitor::new panicked");
    let mut interval = time::interval(MONITOR_INTERVAL);
    let mut ref_num: u64 = 1;

    interval.tick().await;

    loop {
        interval.tick().await;

        let stats = tokio::task::block_in_place(|| monitor.collect());
        let msg = PhxMessage::new(
            Some(&join_ref),
            Some(&ref_num.to_string()),
            &topic,
            "push:hardware",
            stats,
        );
        if tx.try_send(Message::Text(msg.serialize().into())).is_err() {
            break;
        }
        debug!("Pushed hardware stats (ref {})", ref_num);
        ref_num += 1;
    }
}

fn handle_incoming(text: &str, tx: &Tx, topic: &str, join_ref: &str) {
    let phx = match PhxMessage::deserialize(text) {
        Ok(p) => p,
        Err(e) => {
            warn!("Failed to parse incoming message: {}", e);
            return;
        }
    };

    debug!("← {} on {}", phx.event, phx.topic);

    match phx.event.as_str() {
        "phx_reply" => {
            debug!("phx_reply for ref {:?}", phx.msg_ref);
        }

        event if event.starts_with("push:") => {
            let event_name = &event["push:".len()..];
            info!("Received push event: {}", event_name);

            let message_id = phx.payload["message_id"].as_str().unwrap_or("");
            let inner_payload = phx.payload.get("payload").cloned().unwrap_or(json!({}));
            let event_processing_id = inner_payload["event_processing_id"].as_str().unwrap_or("");

            send(
                tx,
                topic,
                join_ref,
                "push:received",
                json!({
                    "message_id": message_id,
                    "response": "ok"
                }),
            );

            let response = dispatch_push(event_name, &inner_payload);
            send(
                tx,
                topic,
                join_ref,
                "push:processed",
                json!({
                    "processed_id": event_processing_id,
                    "response": response
                }),
            );
        }

        event if event.starts_with("cast:") => {
            debug!("Ignoring cast: {}", event);
        }

        other => {
            debug!("Unhandled event: {}", other);
        }
    }
}

fn dispatch_push(event: &str, _payload: &serde_json::Value) -> serde_json::Value {
    match event {
        "reboot_machine" => {
            info!("reboot_machine received — rebooting in 5 s");
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                info!("Executing reboot");
                if let Err(e) = std::process::Command::new("sudo").arg("reboot").spawn() {
                    warn!("reboot spawn failed: {}", e);
                }
            });
            json!({ "result": "rebooting" })
        }
        other => {
            warn!("Unhandled push event: {}", other);
            json!({ "error": format!("unhandled event: {}", other) })
        }
    }
}

fn send(tx: &Tx, topic: &str, join_ref: &str, event: &str, payload: serde_json::Value) {
    let msg = PhxMessage::new(Some(join_ref), None, topic, event, payload);
    tx.try_send(Message::Text(msg.serialize().into())).ok();
}

fn build_join_payload(secret: &str, accelerator_type: &str) -> serde_json::Value {
    json!({
        "header":           format!("Bearer {}", secret),
        "agent_version":    AGENT_VERSION,
        "agent_type":       "agent_smith",
        "accelerator_type": accelerator_type,
        "features":         ["monitor"],
    })
}

fn build_url(endpoint: &str) -> String {
    if endpoint.contains("vsn=") {
        endpoint.to_owned()
    } else if endpoint.contains('?') {
        format!("{}&vsn=2.0.0", endpoint)
    } else {
        format!("{}?vsn=2.0.0", endpoint)
    }
}

pub fn load_uuid(initial_uuid: Option<&str>) -> Option<String> {
    initial_uuid.map(str::to_owned).or_else(|| {
        std::fs::read_to_string(UUID_FILE)
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
    })
}

pub fn save_uuid(uuid: &str) {
    match std::fs::write(UUID_FILE, uuid) {
        Ok(()) => info!("Saved agent UUID to {}", UUID_FILE),
        Err(e) => error!("Failed to save UUID to {}: {}", UUID_FILE, e),
    }
}
