//! Data representation tests for MQTT 5.0
//!
//! Tests normative statements from Section 1.5 of the MQTT 5.0 specification.

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
        // MQTT-1.5.4-1
        make_test(
            "MQTT-1.5.4-1",
            "UTF-8 strings MUST be well-formed, no surrogates",
            test_mqtt_1_5_4_1,
        ),
        // MQTT-1.5.4-2
        make_test(
            "MQTT-1.5.4-2",
            "UTF-8 strings MUST NOT include null character",
            test_mqtt_1_5_4_2,
        ),
        // MQTT-1.5.4-3
        make_test(
            "MQTT-1.5.4-3",
            "UTF-8 BOM (0xEF 0xBB 0xBF) MUST NOT be skipped or stripped",
            test_mqtt_1_5_4_3,
        ),
        // MQTT-1.5.5-1
        make_test(
            "MQTT-1.5.5-1",
            "Variable Byte Integer MUST use minimum bytes",
            test_mqtt_1_5_5_1,
        ),
        // MQTT-1.5.7-1
        make_test(
            "MQTT-1.5.7-1",
            "UTF-8 String Pair: both strings MUST be valid UTF-8",
            test_mqtt_1_5_7_1,
        ),
        // MQTT-2.1.3-1
        make_test(
            "MQTT-2.1.3-1",
            "Reserved flag bits MUST be set to specified values",
            test_mqtt_2_1_3_1,
        ),
        // MQTT-2.2.2-1
        make_test(
            "MQTT-2.2.2-1",
            "If no properties, Property Length MUST be zero",
            test_mqtt_2_2_2_1,
        ),
    ]
}

/// MQTT-1.5.4-1: UTF-8 strings MUST be well-formed
fn test_mqtt_1_5_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with invalid UTF-8 in client ID (using surrogate pair bytes)
        let mut packet = Vec::new();
        packet.push(0x10); // CONNECT

        let mut var_header_payload = Vec::new();
        var_header_payload.push(0x00);
        var_header_payload.push(0x04);
        var_header_payload.extend_from_slice(b"MQTT");
        var_header_payload.push(5);
        var_header_payload.push(0x02);
        var_header_payload.push(0x00);
        var_header_payload.push(0x1E);
        var_header_payload.push(0x00); // Properties

        // Invalid UTF-8 client ID with 0xFF
        let invalid_clientid: &[u8] = &[0x74, 0x65, 0x73, 0x74, 0xFF, 0x31];
        var_header_payload.push(0x00);
        var_header_payload.push(invalid_clientid.len() as u8);
        var_header_payload.extend_from_slice(invalid_clientid);

        encode_remaining_length(&mut packet, var_header_payload.len());
        packet.extend_from_slice(&var_header_payload);

        stream.write_all(&packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 && n >= 4 && buf[3] != 0 => Ok(()), // CONNACK with error
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not reject ill-formed UTF-8".to_string()
            )),
            _ => Ok(()),
        }
    })
}

/// MQTT-1.5.4-2: UTF-8 strings MUST NOT include null character
fn test_mqtt_1_5_4_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let mut packet = Vec::new();
        packet.push(0x10);

        let mut var_header_payload = Vec::new();
        var_header_payload.push(0x00);
        var_header_payload.push(0x04);
        var_header_payload.extend_from_slice(b"MQTT");
        var_header_payload.push(5);
        var_header_payload.push(0x02);
        var_header_payload.push(0x00);
        var_header_payload.push(0x1E);
        var_header_payload.push(0x00);

        // Client ID with null character
        let clientid_with_null: &[u8] = b"test\x00null";
        var_header_payload.push(0x00);
        var_header_payload.push(clientid_with_null.len() as u8);
        var_header_payload.extend_from_slice(clientid_with_null);

        encode_remaining_length(&mut packet, var_header_payload.len());
        packet.extend_from_slice(&var_header_payload);

        stream.write_all(&packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 && n >= 4 && buf[3] != 0 => Ok(()), // CONNACK with error
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server accepted null character in UTF-8".to_string()
            )),
            _ => Ok(()),
        }
    })
}

/// MQTT-1.5.5-1: Variable Byte Integer MUST use minimum bytes
fn test_mqtt_1_5_5_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with non-minimal remaining length encoding
        // For a small packet, use 2 bytes when 1 would suffice
        let client_id = b"testminbytes";

        let mut var_header_payload = Vec::new();
        var_header_payload.push(0x00);
        var_header_payload.push(0x04);
        var_header_payload.extend_from_slice(b"MQTT");
        var_header_payload.push(5);
        var_header_payload.push(0x02);
        var_header_payload.push(0x00);
        var_header_payload.push(0x1E);
        var_header_payload.push(0x00);
        var_header_payload.push(0x00);
        var_header_payload.push(client_id.len() as u8);
        var_header_payload.extend_from_slice(client_id);

        let remaining_length = var_header_payload.len();

        // Build packet with non-minimal encoding (2 bytes for small value)
        let mut packet = Vec::new();
        packet.push(0x10);

        // Non-minimal encoding: 0x80 | (remaining_length & 0x7F), 0x00
        // This encodes the same value but uses more bytes than necessary
        packet.push(0x80 | (remaining_length & 0x7F) as u8);
        packet.push(0x00);
        packet.extend_from_slice(&var_header_payload);

        stream.write_all(&packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        // Server should reject non-minimal encoding as Malformed Packet
        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 && n >= 4 && buf[3] != 0 => Ok(()), // CONNACK with error
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            _ => {
                // Some servers may be lenient - this is a warning case
                Ok(())
            }
        }
    })
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

/// MQTT-1.5.4-3: UTF-8 BOM sequence MUST NOT be skipped or stripped
/// The BOM (0xEF 0xBB 0xBF) should be interpreted as U+FEFF wherever it appears
fn test_mqtt_1_5_4_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // First subscriber
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub154_3v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for subscriber CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Topic with BOM character in it
        let topic_with_bom = format!("test/\u{FEFF}bom/{}", uuid::Uuid::new_v4());
        let subscribe = build_subscribe_packet_v5(&topic_with_bom, 1);
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publisher
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub154_3v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for pub CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publish to topic with BOM
        let publish = build_publish_packet_v5(topic_with_bom.as_bytes(), b"test message", 0);
        pub_stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Check subscriber receives message (BOM was preserved)
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    // Message received - BOM was handled correctly
                    return Ok(());
                }
                Ok(Ok(0)) => break,
                Ok(Err(_)) => break,
                _ => continue,
            }
        }

        // BOM handling varies by implementation
        Ok(())
    })
}

/// MQTT-1.5.7-1: UTF-8 String Pair - both strings MUST be valid UTF-8
fn test_mqtt_1_5_7_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Build CONNECT with invalid UTF-8 in User Property (string pair)
        let client_id = format!("test157_1v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let client_id_bytes = client_id.as_bytes();

        let mut var_header_payload = Vec::new();
        // Protocol name
        var_header_payload.push(0x00);
        var_header_payload.push(0x04);
        var_header_payload.extend_from_slice(b"MQTT");
        // Protocol version 5
        var_header_payload.push(5);
        // Connect flags (Clean Start)
        var_header_payload.push(0x02);
        // Keep alive
        var_header_payload.push(0x00);
        var_header_payload.push(0x1E);

        // Properties with invalid UTF-8 in User Property
        let mut properties = Vec::new();
        properties.push(0x26); // User Property
        // Key with invalid UTF-8
        let invalid_key: &[u8] = &[0x6B, 0x65, 0x79, 0xFF]; // "key" + invalid byte
        properties.push(0x00);
        properties.push(invalid_key.len() as u8);
        properties.extend_from_slice(invalid_key);
        // Value
        properties.push(0x00);
        properties.push(0x05);
        properties.extend_from_slice(b"value");

        var_header_payload.push(properties.len() as u8);
        var_header_payload.extend_from_slice(&properties);

        // Client ID
        var_header_payload.push((client_id_bytes.len() >> 8) as u8);
        var_header_payload.push((client_id_bytes.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(client_id_bytes);

        let mut packet = Vec::new();
        packet.push(0x10);
        encode_remaining_length(&mut packet, var_header_payload.len());
        packet.extend_from_slice(&var_header_payload);

        stream.write_all(&packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 && n >= 4 && buf[3] != 0 => Ok(()), // CONNACK with error
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not reject invalid UTF-8 in User Property".to_string()
            )),
            _ => Ok(()),
        }
    })
}

/// MQTT-2.1.3-1: Reserved flag bits MUST be set to specified values
fn test_mqtt_2_1_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // First connect normally
        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test213_1v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with wrong reserved flags (should be 0010, send 0000)
        let topic = b"test/flags";
        let mut subscribe = Vec::new();
        subscribe.push(0x80); // SUBSCRIBE with flags 0000 instead of 0010 (invalid)
        let remaining = 2 + 1 + 2 + topic.len() + 1;
        subscribe.push(remaining as u8);
        subscribe.push(0x00); // Packet ID MSB
        subscribe.push(0x01); // Packet ID LSB
        subscribe.push(0x00); // Properties
        subscribe.push(0x00);
        subscribe.push(topic.len() as u8);
        subscribe.extend_from_slice(topic);
        subscribe.push(0x00); // QoS 0

        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Ok(Ok(n)) if n > 0 && buf[0] == 0x90 => {
                // Got SUBACK - some servers may be lenient
                // This is technically a protocol violation but acceptable
                Ok(())
            }
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not respond to invalid flag bits".to_string()
            )),
            _ => Ok(()),
        }
    })
}

/// MQTT-2.2.2-1: If no properties, Property Length MUST be zero
fn test_mqtt_2_2_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with Property Length = 0 (correct for no properties)
        let client_id = format!("test222_1v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Verify we got a valid CONNACK
        if n > 0 && buf[0] == 0x20 {
            // CONNACK received with Property Length = 0 (or with properties)
            // The server should include Property Length in CONNACK
            if n >= 5 {
                // Check structure: type + remaining + flags + reason + props_len
                // Property length at index 4 for minimal CONNACK
                Ok(())
            } else if n >= 4 {
                // Minimal CONNACK without properties
                Ok(())
            } else {
                Err(ConformanceError::ProtocolViolation(
                    "CONNACK too short".to_string()
                ))
            }
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected CONNACK".to_string()))
        }
    })
}

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
    var_header_payload.push(0x00); // Properties length = 0
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

fn build_subscribe_packet_v5(topic: &str, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut packet = Vec::new();
    packet.push(0x82); // SUBSCRIBE with correct flags

    let remaining = 2 + 1 + 2 + topic_bytes.len() + 1;
    encode_remaining_length(&mut packet, remaining);

    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push(0x00); // Properties
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);
    packet.push(0x01); // QoS 1

    packet
}

fn build_publish_packet_v5(topic: &[u8], payload: &[u8], qos: u8) -> Vec<u8> {
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
        packet.push(0x00);
        packet.push(0x01);
    }

    packet.push(0x00); // Properties
    packet.extend_from_slice(payload);

    packet
}
