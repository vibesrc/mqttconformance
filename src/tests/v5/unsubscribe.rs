//! UNSUBSCRIBE packet tests for MQTT 5.0
//!
//! Tests normative statements from Section 3.10 of the MQTT 5.0 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "unsubscribe".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.10.3-1
        make_test(
            "MQTT-3.10.3-1",
            "Payload MUST contain at least one Topic Filter",
            test_mqtt_3_10_3_1,
        ),
        // MQTT-3.10.4-1
        make_test(
            "MQTT-3.10.4-1",
            "Server MUST respond with UNSUBACK even if no Topic Filters matched",
            test_mqtt_3_10_4_1,
        ),
    ]
}

/// MQTT-3.10.3-1: Payload MUST contain at least one Topic Filter
fn test_mqtt_3_10_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test31031v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send UNSUBSCRIBE with empty payload
        let mut unsubscribe = Vec::new();
        unsubscribe.push(0xA2); // UNSUBSCRIBE
        unsubscribe.push(0x03); // Remaining: packet ID + props only
        unsubscribe.push(0x00);
        unsubscribe.push(0x01);
        unsubscribe.push(0x00); // Properties

        stream.write_all(&unsubscribe).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            _ => Err(ConformanceError::ProtocolViolation(
                "Server accepted UNSUBSCRIBE with no topic filters".to_string()
            )),
        }
    })
}

/// MQTT-3.10.4-1: Server MUST respond with UNSUBACK even if no Topic Filters matched
fn test_mqtt_3_10_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test31041v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Unsubscribe from a topic we never subscribed to
        let topic = format!("test/unsubscribe/nonexistent/{}", uuid::Uuid::new_v4());
        let packet_id: u16 = 0x1234;
        let unsubscribe = build_unsubscribe_packet(&topic, packet_id);
        stream.write_all(&unsubscribe).await
            .map_err(ConformanceError::Io)?;

        // Server MUST respond with UNSUBACK
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for UNSUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n < 4 || buf[0] != 0xB0 {
            return Err(ConformanceError::UnexpectedResponse(
                "Expected UNSUBACK even for non-existent subscription".to_string()
            ));
        }

        // Verify packet ID matches
        let unsuback_pkid = ((buf[2] as u16) << 8) | (buf[3] as u16);
        if unsuback_pkid != packet_id {
            return Err(ConformanceError::ProtocolViolation(format!(
                "UNSUBACK packet ID {} does not match UNSUBSCRIBE {}",
                unsuback_pkid, packet_id
            )));
        }

        // Verify reason code is present (should be 0x11 = No subscription existed, or 0x00 = Success)
        let props_len = buf[4] as usize;
        let reason_code_offset = 5 + props_len;
        if reason_code_offset < n {
            let reason_code = buf[reason_code_offset];
            // 0x00 = Success, 0x11 = No subscription existed - both are valid
            if reason_code != 0x00 && reason_code != 0x11 {
                // Other error codes are also acceptable
                if reason_code < 0x80 {
                    return Err(ConformanceError::ProtocolViolation(format!(
                        "Unexpected UNSUBACK reason code: 0x{:02X}",
                        reason_code
                    )));
                }
            }
        }

        Ok(())
    })
}

fn build_unsubscribe_packet(topic: &str, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut packet = Vec::new();
    packet.push(0xA2); // UNSUBSCRIBE

    let remaining = 2 + 1 + 2 + topic_bytes.len();
    encode_remaining_length(&mut packet, remaining);

    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push(0x00); // Properties
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);

    packet
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
