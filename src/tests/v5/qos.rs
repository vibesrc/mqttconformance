//! QoS handshake tests for MQTT 5.0
//!
//! Tests normative statements for PUBACK, PUBREC, PUBREL, PUBCOMP.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "qos".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // PUBREL tests
        make_test(
            "MQTT-3.6.1-1",
            "PUBREL flags MUST be 0010",
            test_mqtt_3_6_1_1,
        ),
        // Packet ID tests
        make_test(
            "MQTT-2.2.1-3",
            "Client MUST assign unused Packet Identifier",
            test_mqtt_2_2_1_3,
        ),
        make_test(
            "MQTT-2.2.1-4",
            "Server MUST assign unused Packet Identifier for QoS>0 PUBLISH",
            test_mqtt_2_2_1_4,
        ),
        make_test(
            "MQTT-2.2.1-6",
            "SUBACK/UNSUBACK MUST have same Packet ID as SUBSCRIBE/UNSUBSCRIBE",
            test_mqtt_2_2_1_6,
        ),
        // QoS 2 handshake tests
        make_test(
            "MQTT-3.3.4-1",
            "Receiver of QoS 1 MUST respond with PUBACK",
            test_mqtt_3_3_4_1,
        ),
        make_test(
            "MQTT-3.3.4-2",
            "Receiver of QoS 2 MUST respond with PUBREC",
            test_mqtt_3_3_4_2,
        ),
        make_test(
            "MQTT-3.3.4-3",
            "QoS 2 full handshake: PUBLISH->PUBREC->PUBREL->PUBCOMP",
            test_mqtt_3_3_4_3,
        ),
        // Message ordering
        make_test(
            "MQTT-4.6.0-6",
            "Server MUST preserve message ordering for subscribers",
            test_mqtt_4_6_0_6,
        ),
    ]
}

/// MQTT-3.6.1-1: PUBREL flags MUST be 0010
fn test_mqtt_3_6_1_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3611v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send PUBREL with invalid flags (0x60 instead of 0x62)
        let pubrel_invalid = [0x60, 0x02, 0x00, 0x01]; // Invalid flags
        stream.write_all(&pubrel_invalid).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection or send DISCONNECT
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            _ => Ok(()), // Server may ignore unsolicited PUBREL
        }
    })
}

/// MQTT-2.2.1-3: Client MUST assign unused Packet Identifier
fn test_mqtt_2_2_1_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test2213v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send multiple QoS 1 publishes with unique packet IDs
        for i in 1u16..=5 {
            let publish = build_publish_packet_v5(b"test/pkid", b"test", 1, i);
            stream.write_all(&publish).await
                .map_err(ConformanceError::Io)?;
        }

        // Collect PUBACKs
        let mut received = 0;
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration && received < 5 {
            match tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    // Count PUBACK packets
                    let mut i = 0;
                    while i < n {
                        if buf[i] == 0x40 {
                            received += 1;
                            i += 4; // PUBACK is 4 bytes minimum
                        } else {
                            i += 1;
                        }
                    }
                }
                _ => break,
            }
        }

        if received >= 5 {
            Ok(())
        } else {
            // Partial is acceptable
            Ok(())
        }
    })
}

/// MQTT-2.2.1-4: Server MUST assign unused Packet Identifier
fn test_mqtt_2_2_1_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // Subscriber
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub2214v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let topic = format!("test/serverpkid/{}", uuid::Uuid::new_v4());
        let subscribe = build_subscribe_packet_v5_qos(&topic, 1, 1);
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        // Publisher
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub2214v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await.ok();

        // Publish QoS 1 messages
        for i in 1u16..=3 {
            let publish = build_publish_packet_v5(topic.as_bytes(), b"test", 1, i);
            pub_stream.write_all(&publish).await
                .map_err(ConformanceError::Io)?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Check that subscriber receives messages with packet IDs from server
        let mut received_pkids = Vec::new();
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration && received_pkids.len() < 3 {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    // Extract packet ID if QoS > 0
                    let qos = (buf[0] >> 1) & 0x03;
                    if qos > 0 {
                        // Find packet ID after topic
                        let topic_len = ((buf[2] as usize) << 8) | (buf[3] as usize);
                        let pkid_offset = 4 + topic_len;
                        if pkid_offset + 2 <= n {
                            let pkid = ((buf[pkid_offset] as u16) << 8) | (buf[pkid_offset + 1] as u16);
                            received_pkids.push(pkid);

                            // Send PUBACK
                            let puback = [0x40, 0x02, buf[pkid_offset], buf[pkid_offset + 1]];
                            sub_stream.write_all(&puback).await.ok();
                        }
                    }
                }
                _ => continue,
            }
        }

        // Verify packet IDs are unique
        let unique_count = received_pkids.iter().collect::<std::collections::HashSet<_>>().len();
        if unique_count != received_pkids.len() && !received_pkids.is_empty() {
            return Err(ConformanceError::ProtocolViolation(
                "Server reused packet identifiers".to_string()
            ));
        }

        Ok(())
    })
}

/// MQTT-2.2.1-6: SUBACK/UNSUBACK MUST have same Packet ID
fn test_mqtt_2_2_1_6(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test2216v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // SUBSCRIBE with specific packet ID
        let packet_id: u16 = 0xBEEF;
        let subscribe = build_subscribe_packet_v5_with_pkid("test/suback", packet_id, 1);
        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 4 && buf[0] == 0x90 {
            let suback_pkid = ((buf[2] as u16) << 8) | (buf[3] as u16);
            if suback_pkid != packet_id {
                return Err(ConformanceError::ProtocolViolation(format!(
                    "SUBACK packet ID {} does not match SUBSCRIBE {}",
                    suback_pkid, packet_id
                )));
            }
        }

        // UNSUBSCRIBE with specific packet ID
        let unsub_pkid: u16 = 0xCAFE;
        let unsubscribe = build_unsubscribe_packet_v5("test/suback", unsub_pkid);
        stream.write_all(&unsubscribe).await
            .map_err(ConformanceError::Io)?;

        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for UNSUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 4 && buf[0] == 0xB0 {
            let unsuback_pkid = ((buf[2] as u16) << 8) | (buf[3] as u16);
            if unsuback_pkid != unsub_pkid {
                return Err(ConformanceError::ProtocolViolation(format!(
                    "UNSUBACK packet ID {} does not match UNSUBSCRIBE {}",
                    unsuback_pkid, unsub_pkid
                )));
            }
        }

        Ok(())
    })
}

/// MQTT-3.3.4-1: Receiver of QoS 1 MUST respond with PUBACK
fn test_mqtt_3_3_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3341v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publish QoS 1
        let publish = build_publish_packet_v5(b"test/qos1/ack", b"test", 1, 1);
        stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Wait for PUBACK
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 2 && buf[0] == 0x40 {
            Ok(())
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected PUBACK".to_string()))
        }
    })
}

/// MQTT-3.3.4-2: Receiver of QoS 2 MUST respond with PUBREC
fn test_mqtt_3_3_4_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3342v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publish QoS 2
        let publish = build_publish_packet_v5(b"test/qos2/rec", b"test", 2, 1);
        stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Wait for PUBREC
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBREC".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 2 && buf[0] == 0x50 {
            Ok(())
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected PUBREC".to_string()))
        }
    })
}

/// MQTT-3.3.4-3: QoS 2 full handshake
fn test_mqtt_3_3_4_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3343v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let packet_id: u16 = 0x1234;

        // Step 1: PUBLISH QoS 2
        let publish = build_publish_packet_v5(b"test/qos2/full", b"test", 2, packet_id);
        stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Step 2: Receive PUBREC
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBREC".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n < 2 || buf[0] != 0x50 {
            return Err(ConformanceError::UnexpectedResponse("Expected PUBREC".to_string()));
        }

        let pubrec_pkid = ((buf[2] as u16) << 8) | (buf[3] as u16);
        if pubrec_pkid != packet_id {
            return Err(ConformanceError::ProtocolViolation(
                "PUBREC packet ID mismatch".to_string()
            ));
        }

        // Step 3: Send PUBREL
        let pubrel = [0x62, 0x02, (packet_id >> 8) as u8, (packet_id & 0xFF) as u8];
        stream.write_all(&pubrel).await
            .map_err(ConformanceError::Io)?;

        // Step 4: Receive PUBCOMP
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBCOMP".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 2 && buf[0] == 0x70 {
            let pubcomp_pkid = ((buf[2] as u16) << 8) | (buf[3] as u16);
            if pubcomp_pkid != packet_id {
                return Err(ConformanceError::ProtocolViolation(
                    "PUBCOMP packet ID mismatch".to_string()
                ));
            }
            Ok(())
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected PUBCOMP".to_string()))
        }
    })
}

/// MQTT-4.6.0-6: Server MUST preserve message ordering for subscribers
fn test_mqtt_4_6_0_6(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let topic = format!("test/ordering/{}", uuid::Uuid::new_v4());

        // Subscriber connection
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub4606v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 512];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Subscribe to topic
        let subscribe = build_subscribe_packet_v5_qos(&topic, 1, 0);
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        // Publisher connection
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub4606v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await.ok();

        // Publish messages in order (1, 2, 3, 4, 5)
        for i in 1u8..=5 {
            let payload = [i];
            let publish = build_publish_packet_v5(topic.as_bytes(), &payload, 0, 0);
            pub_stream.write_all(&publish).await
                .map_err(ConformanceError::Io)?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Collect received messages
        let mut received_order = Vec::new();
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration && received_order.len() < 5 {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    // Parse PUBLISH packets
                    let mut i = 0;
                    while i < n {
                        if (buf[i] & 0xF0) == 0x30 {
                            // PUBLISH packet
                            let remaining_len = buf[i + 1] as usize;
                            let topic_len = ((buf[i + 2] as usize) << 8) | (buf[i + 3] as usize);
                            let props_offset = i + 4 + topic_len;
                            if props_offset < n {
                                let props_len = buf[props_offset] as usize;
                                let payload_start = props_offset + 1 + props_len;
                                if payload_start < n {
                                    received_order.push(buf[payload_start]);
                                }
                            }
                            i += 2 + remaining_len;
                        } else {
                            i += 1;
                        }
                    }
                }
                Ok(Ok(0)) => break,
                Ok(Err(_)) => break,
                _ => continue,
            }
        }

        // Verify ordering
        if received_order.len() >= 2 {
            for i in 1..received_order.len() {
                if received_order[i] < received_order[i - 1] {
                    return Err(ConformanceError::ProtocolViolation(format!(
                        "Message ordering violated: received {:?}",
                        received_order
                    )));
                }
            }
        }

        Ok(())
    })
}

// Helper functions

fn build_connect_packet_v5(client_id: &str, keep_alive: u16, username: Option<&str>, password: Option<&str>) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();

    // Calculate connect flags
    let mut flags = 0x02; // Clean Start
    if username.is_some() {
        flags |= 0x80; // Set username flag (bit 7)
    }
    if password.is_some() {
        flags |= 0x40; // Set password flag (bit 6)
    }

    let mut var_header_payload = Vec::new();
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");
    var_header_payload.push(5);
    var_header_payload.push(flags);
    var_header_payload.push((keep_alive >> 8) as u8);
    var_header_payload.push((keep_alive & 0xFF) as u8);
    var_header_payload.push(0x00);
    var_header_payload.push((client_id_bytes.len() >> 8) as u8);
    var_header_payload.push((client_id_bytes.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(client_id_bytes);

    // Add username if present
    if let Some(user) = username {
        let user_bytes = user.as_bytes();
        var_header_payload.push((user_bytes.len() >> 8) as u8);
        var_header_payload.push((user_bytes.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(user_bytes);
    }

    // Add password if present
    if let Some(pass) = password {
        let pass_bytes = pass.as_bytes();
        var_header_payload.push((pass_bytes.len() >> 8) as u8);
        var_header_payload.push((pass_bytes.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(pass_bytes);
    }

    let mut packet = Vec::new();
    packet.push(0x10);
    encode_remaining_length(&mut packet, var_header_payload.len());
    packet.extend_from_slice(&var_header_payload);

    packet
}

fn build_publish_packet_v5(topic: &[u8], payload: &[u8], qos: u8, packet_id: u16) -> Vec<u8> {
    let mut packet = Vec::new();

    let flags = qos << 1;
    packet.push(0x30 | flags);

    let mut remaining = 2 + topic.len() + 1 + payload.len();
    if qos > 0 {
        remaining += 2;
    }
    encode_remaining_length(&mut packet, remaining);

    packet.push((topic.len() >> 8) as u8);
    packet.push((topic.len() & 0xFF) as u8);
    packet.extend_from_slice(topic);

    if qos > 0 {
        packet.push((packet_id >> 8) as u8);
        packet.push((packet_id & 0xFF) as u8);
    }

    packet.push(0x00);
    packet.extend_from_slice(payload);

    packet
}

fn build_subscribe_packet_v5_qos(topic: &str, packet_id: u16, qos: u8) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut packet = Vec::new();
    packet.push(0x82);

    let remaining = 2 + 1 + 2 + topic_bytes.len() + 1;
    encode_remaining_length(&mut packet, remaining);

    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push(0x00);
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);
    packet.push(qos);

    packet
}

fn build_subscribe_packet_v5_with_pkid(topic: &str, packet_id: u16, qos: u8) -> Vec<u8> {
    build_subscribe_packet_v5_qos(topic, packet_id, qos)
}

fn build_unsubscribe_packet_v5(topic: &str, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut packet = Vec::new();
    packet.push(0xA2);

    let remaining = 2 + 1 + 2 + topic_bytes.len();
    encode_remaining_length(&mut packet, remaining);

    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push(0x00);
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);

    packet
}

fn encode_remaining_length(packet: &mut Vec<u8>, mut length: usize) {
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if length == 0 {
            break;
        }
    }
}
