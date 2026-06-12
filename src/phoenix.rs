use serde_json::Value;

/// A Phoenix channel message serialized as a 5-element JSON array:
/// `[join_ref, ref, topic, event, payload]`
///
/// - `join_ref` / `msg_ref` are nullable strings used for message correlation.
/// - The server uses Phoenix Socket protocol v2 (`vsn=2.0.0`).
#[derive(Debug)]
pub struct PhxMessage {
    pub join_ref: Option<String>,
    pub msg_ref: Option<String>,
    pub topic: String,
    pub event: String,
    pub payload: Value,
}

impl PhxMessage {
    pub fn new(
        join_ref: Option<&str>,
        msg_ref: Option<&str>,
        topic: &str,
        event: &str,
        payload: Value,
    ) -> Self {
        Self {
            join_ref: join_ref.map(str::to_owned),
            msg_ref: msg_ref.map(str::to_owned),
            topic: topic.to_owned(),
            event: event.to_owned(),
            payload,
        }
    }

    pub fn serialize(&self) -> String {
        serde_json::to_string(&(
            &self.join_ref,
            &self.msg_ref,
            &self.topic,
            &self.event,
            &self.payload,
        ))
        .expect("serialization is infallible")
    }

    pub fn deserialize(s: &str) -> Result<Self, serde_json::Error> {
        let v: Value = serde_json::from_str(s)?;
        let arr = v
            .as_array()
            .ok_or_else(|| serde::de::Error::custom("expected JSON array"))?;

        if arr.len() != 5 {
            return Err(serde::de::Error::custom(format!(
                "expected 5-element array, got {}",
                arr.len()
            )));
        }

        let join_ref = arr[0].as_str().map(str::to_owned);
        let msg_ref = arr[1].as_str().map(str::to_owned);
        let topic = arr[2]
            .as_str()
            .ok_or_else(|| serde::de::Error::custom("topic must be a string"))?
            .to_owned();
        let event = arr[3]
            .as_str()
            .ok_or_else(|| serde::de::Error::custom("event must be a string"))?
            .to_owned();
        let payload = arr[4].clone();

        Ok(Self {
            join_ref,
            msg_ref,
            topic,
            event,
            payload,
        })
    }
}
