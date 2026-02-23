//! Data representation tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 1.5 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "data".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-1.5.3-1
        make_test(
            "MQTT-1.5.3-1",
            "Server MUST close connection on ill-formed UTF-8",
            test_mqtt_1_5_3_1,
        ),
        // MQTT-1.5.3-2
        make_test(
            "MQTT-1.5.3-2",
            "UTF-8 string MUST NOT include null character U+0000",
            test_mqtt_1_5_3_2,
        ),
        // MQTT-1.5.3-3
        make_test(
            "MQTT-1.5.3-3",
            "BOM (0xEF 0xBB 0xBF) MUST NOT be skipped or stripped",
            test_mqtt_1_5_3_3,
        ),
    ]
}

/// MQTT-1.5.3-1: Server MUST close connection on ill-formed UTF-8
fn test_mqtt_1_5_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with ill-formed UTF-8 in client ID
        // Using invalid UTF-8 sequence: 0xFF is never valid in UTF-8
        let mut packet = Vec::new();

        // Fixed header
        packet.push(0x10); // CONNECT

        // We'll build the variable header and payload first to calculate remaining length
        let mut vheader_payload = Vec::new();

        // Protocol name
        vheader_payload.push(0x00);
        vheader_payload.push(0x04);
        vheader_payload.extend_from_slice(b"MQTT");

        // Protocol level
        vheader_payload.push(4);

        // Connect flags (clean session)
        vheader_payload.push(0x02);

        // Keep alive
        vheader_payload.push(0x00);
        vheader_payload.push(0x1E); // 30 seconds

        // Client ID with invalid UTF-8
        let invalid_client_id: &[u8] = &[0x74, 0x65, 0x73, 0x74, 0xFF, 0x31]; // "test" + 0xFF + "1"
        vheader_payload.push(0x00);
        vheader_payload.push(invalid_client_id.len() as u8);
        vheader_payload.extend_from_slice(invalid_client_id);

        // Remaining length
        packet.push(vheader_payload.len() as u8);
        packet.extend_from_slice(&vheader_payload);

        stream.write_all(&packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let mut buf = [0u8; 4];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Err(_)) => Ok(()), // Connection error
            Ok(Ok(n)) => {
                // Might get a CONNACK with error before close
                if n >= 4 && buf[0] == 0x20 && buf[3] != 0x00 {
                    Ok(()) // CONNACK with error is acceptable
                } else {
                    Err(ConformanceError::ProtocolViolation(
                        "Server accepted ill-formed UTF-8".to_string()
                    ))
                }
            }
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for ill-formed UTF-8".to_string()
            )),
        }
    })
}

/// MQTT-1.5.3-2: UTF-8 string MUST NOT include null character U+0000
fn test_mqtt_1_5_3_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with null character in client ID
        let mut packet = Vec::new();

        // Fixed header
        packet.push(0x10); // CONNECT

        let mut vheader_payload = Vec::new();

        // Protocol name
        vheader_payload.push(0x00);
        vheader_payload.push(0x04);
        vheader_payload.extend_from_slice(b"MQTT");

        // Protocol level
        vheader_payload.push(4);

        // Connect flags
        vheader_payload.push(0x02);

        // Keep alive
        vheader_payload.push(0x00);
        vheader_payload.push(0x1E);

        // Client ID with null character: "test\0null"
        let client_id_with_null: &[u8] = b"test\x00null";
        vheader_payload.push(0x00);
        vheader_payload.push(client_id_with_null.len() as u8);
        vheader_payload.extend_from_slice(client_id_with_null);

        // Remaining length
        packet.push(vheader_payload.len() as u8);
        packet.extend_from_slice(&vheader_payload);

        stream.write_all(&packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let mut buf = [0u8; 4];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Err(_)) => Ok(()), // Connection error
            Ok(Ok(n)) => {
                if n >= 4 && buf[0] == 0x20 && buf[3] != 0x00 {
                    Ok(()) // CONNACK with error
                } else {
                    Err(ConformanceError::ProtocolViolation(
                        "Server accepted UTF-8 string containing null character".to_string()
                    ))
                }
            }
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for null character in UTF-8".to_string()
            )),
        }
    })
}

/// MQTT-1.5.3-3: BOM (0xEF 0xBB 0xBF) MUST be interpreted as U+FEFF, not skipped or stripped
fn test_mqtt_1_5_3_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

        // The BOM is a valid UTF-8 sequence encoding U+FEFF (Zero Width No-Break Space)
        // When used in a topic, it should be treated as part of the topic name
        // A topic with BOM should be different from one without

        let topic_with_bom = format!("\u{FEFF}test/bom/{}", uuid::Uuid::new_v4());
        let topic_without_bom = topic_with_bom.trim_start_matches('\u{FEFF}').to_string();

        let mut opts = MqttOptions::new(
            format!("mqtt-bom-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            &ctx.host,
            ctx.port,
        );
        opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        opts.set_clean_session(true);

        let (client, mut eventloop) = AsyncClient::new(opts, 10);

        // Wait for connection
        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
            }
        }

        // Subscribe to topic WITHOUT BOM
        client.subscribe(&topic_without_bom, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Publish to topic WITH BOM
        client.publish(&topic_with_bom, QoS::AtLeastOnce, false, b"bom test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Wait for PUBACK
        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for PUBACK".to_string())),
            }
        }

        // The message should NOT be received because topic with BOM != topic without BOM
        // If BOM was stripped, they would match and message would be received
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    // Check if the received topic is the one without BOM
                    if publish.topic == topic_without_bom {
                        client.disconnect().await.ok();
                        return Err(ConformanceError::ProtocolViolation(
                            "Server stripped BOM from topic name".to_string()
                        ));
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();
        Ok(())
    })
}
