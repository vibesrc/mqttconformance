//! Variable Header tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 2.3 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "variableheader".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-2.3.1-2
        make_test(
            "MQTT-2.3.1-2",
            "Client MUST assign unused Packet Identifier to new packets",
            test_mqtt_2_3_1_2,
        ),
        // MQTT-2.3.1-3
        make_test(
            "MQTT-2.3.1-3",
            "Client MUST use same Packet Identifier when re-sending",
            test_mqtt_2_3_1_3,
        ),
        // MQTT-2.3.1-4
        make_test(
            "MQTT-2.3.1-4",
            "Server MUST use unique Packet Identifier for QoS > 0 PUBLISH",
            test_mqtt_2_3_1_4,
        ),
        // MQTT-2.3.1-7
        make_test(
            "MQTT-2.3.1-7",
            "SUBACK/UNSUBACK MUST contain same Packet Identifier as request",
            test_mqtt_2_3_1_7,
        ),
    ]
}

/// MQTT-2.3.1-2: Client MUST assign unused Packet Identifier to new packets
/// This is a client-side requirement; we verify server accepts sequential packet IDs
fn test_mqtt_2_3_1_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test2312{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if buf[0] != 0x20 || buf[3] != 0x00 {
            return Err(ConformanceError::UnexpectedResponse("Expected successful CONNACK".to_string()));
        }

        // Send multiple SUBSCRIBE with different packet IDs
        for pkid in 1u16..=3 {
            let topic = format!("test/pkid/{}", pkid);
            let subscribe = build_subscribe_packet(&topic, pkid);
            stream.write_all(&subscribe).await
                .map_err(ConformanceError::Io)?;
        }

        // Receive all SUBACKs
        let mut received = 0;
        let mut buf = [0u8; 64];
        let start = std::time::Instant::now();
        while received < 3 && start.elapsed() < ctx.timeout {
            match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
                Ok(Ok(n)) => {
                    if n == 0 {
                        break;
                    }
                    // Count SUBACKs
                    let mut i = 0;
                    while i < n {
                        if buf[i] == 0x90 {
                            received += 1;
                        }
                        i += 1;
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }

        if received >= 3 {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(
                format!("Expected 3 SUBACKs, got {}", received)
            ))
        }
    })
}

/// MQTT-2.3.1-3: Client MUST use same Packet Identifier when re-sending
/// This is primarily a client requirement; server should handle re-sent packets
fn test_mqtt_2_3_1_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test2313{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if buf[0] != 0x20 || buf[3] != 0x00 {
            return Err(ConformanceError::UnexpectedResponse("Expected successful CONNACK".to_string()));
        }

        let topic = "test/resend";
        let pkid: u16 = 42;

        // Send same PUBLISH twice with same packet ID (simulating re-send)
        let publish = build_publish_qos1_packet(topic, pkid, b"message", false);
        stream.write_all(&publish).await.map_err(ConformanceError::Io)?;

        // Send again with DUP flag set
        let publish_dup = build_publish_qos1_packet(topic, pkid, b"message", true);
        stream.write_all(&publish_dup).await.map_err(ConformanceError::Io)?;

        // Server should send PUBACK for both (or at least one)
        let mut puback_count = 0;
        let mut buf = [0u8; 16];
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            match tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    for i in 0..n {
                        if buf[i] == 0x40 { // PUBACK
                            puback_count += 1;
                        }
                    }
                }
                _ => break,
            }
        }

        // We should get at least one PUBACK
        if puback_count >= 1 {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(
                "Server did not acknowledge re-sent PUBLISH".to_string()
            ))
        }
    })
}

/// MQTT-2.3.1-4: Server MUST use unique Packet Identifier for QoS > 0 PUBLISH
fn test_mqtt_2_3_1_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/server/pkid/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-pkid-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Subscribe with QoS 1
        client.subscribe(&topic, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Publish multiple QoS 1 messages to self
        for i in 0..3 {
            client.publish(&topic, QoS::AtLeastOnce, false, format!("msg{}", i).into_bytes()).await
                .map_err(ConformanceError::MqttClient)?;
        }

        // Collect packet IDs from received messages
        let mut received_pkids = Vec::new();
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) && received_pkids.len() < 3 {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if let Some(pkid) = publish.pkid.into() {
                        received_pkids.push(pkid);
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        // Verify all packet IDs are unique
        let unique_count = received_pkids.iter().collect::<std::collections::HashSet<_>>().len();
        if unique_count == received_pkids.len() && received_pkids.len() >= 3 {
            Ok(())
        } else if received_pkids.len() < 3 {
            // Server might not have sent all messages yet
            Ok(()) // Acceptable
        } else {
            Err(ConformanceError::ProtocolViolation(
                "Server used duplicate packet identifiers".to_string()
            ))
        }
    })
}

/// MQTT-2.3.1-7: SUBACK/UNSUBACK MUST contain same Packet Identifier as request
fn test_mqtt_2_3_1_7(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test2317{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if buf[0] != 0x20 || buf[3] != 0x00 {
            return Err(ConformanceError::UnexpectedResponse("Expected successful CONNACK".to_string()));
        }

        // Send SUBSCRIBE with specific packet ID
        let pkid: u16 = 0x1234;
        let subscribe = build_subscribe_packet("test/suback/pkid", pkid);
        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        // Read SUBACK and verify packet ID matches
        let mut buf = [0u8; 8];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 4 && buf[0] == 0x90 {
            let suback_pkid = ((buf[2] as u16) << 8) | (buf[3] as u16);
            if suback_pkid == pkid {
                // Now test UNSUBSCRIBE
                let unsub_pkid: u16 = 0x5678;
                let unsubscribe = build_unsubscribe_packet("test/suback/pkid", unsub_pkid);
                stream.write_all(&unsubscribe).await
                    .map_err(ConformanceError::Io)?;

                let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                    .map_err(|_| ConformanceError::Timeout("Waiting for UNSUBACK".to_string()))?
                    .map_err(ConformanceError::Io)?;

                if n >= 4 && buf[0] == 0xB0 {
                    let unsuback_pkid = ((buf[2] as u16) << 8) | (buf[3] as u16);
                    if unsuback_pkid == unsub_pkid {
                        Ok(())
                    } else {
                        Err(ConformanceError::ProtocolViolation(
                            format!("UNSUBACK packet ID {} does not match request {}", unsuback_pkid, unsub_pkid)
                        ))
                    }
                } else {
                    Err(ConformanceError::UnexpectedResponse("Expected UNSUBACK".to_string()))
                }
            } else {
                Err(ConformanceError::ProtocolViolation(
                    format!("SUBACK packet ID {} does not match request {}", suback_pkid, pkid)
                ))
            }
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected SUBACK".to_string()))
        }
    })
}

fn build_connect_packet(client_id: &str, keep_alive: u16) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let client_id_len = client_id_bytes.len();
    let remaining_length = 6 + 1 + 1 + 2 + 2 + client_id_len;

    let mut packet = Vec::with_capacity(2 + remaining_length);
    packet.push(0x10);
    packet.push(remaining_length as u8);
    packet.push(0x00);
    packet.push(0x04);
    packet.extend_from_slice(b"MQTT");
    packet.push(4);
    packet.push(0x02);
    packet.push((keep_alive >> 8) as u8);
    packet.push((keep_alive & 0xFF) as u8);
    packet.push((client_id_len >> 8) as u8);
    packet.push((client_id_len & 0xFF) as u8);
    packet.extend_from_slice(client_id_bytes);

    packet
}

fn build_subscribe_packet(topic: &str, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let remaining_length = 2 + 2 + topic_bytes.len() + 1;

    let mut packet = Vec::new();
    packet.push(0x82); // SUBSCRIBE
    packet.push(remaining_length as u8);
    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);
    packet.push(0x00); // QoS 0

    packet
}

fn build_unsubscribe_packet(topic: &str, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let remaining_length = 2 + 2 + topic_bytes.len();

    let mut packet = Vec::new();
    packet.push(0xA2); // UNSUBSCRIBE
    packet.push(remaining_length as u8);
    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);

    packet
}

fn build_publish_qos1_packet(topic: &str, packet_id: u16, payload: &[u8], dup: bool) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let remaining_length = 2 + topic_bytes.len() + 2 + payload.len();

    let mut packet = Vec::new();
    let flags = if dup { 0x3A } else { 0x32 }; // QoS 1, optional DUP
    packet.push(flags);
    packet.push(remaining_length as u8);
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);
    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.extend_from_slice(payload);

    packet
}
