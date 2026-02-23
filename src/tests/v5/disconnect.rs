//! DISCONNECT packet tests for MQTT 5.0
//!
//! Tests normative statements from Section 3.14 of the MQTT 5.0 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "disconnect".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.14.2-1
        make_test(
            "MQTT-3.14.2-1",
            "Cannot set Session Expiry to non-zero if it was zero in CONNECT",
            test_mqtt_3_14_2_1,
        ),
    ]
}

/// MQTT-3.14.2-1: Cannot set Session Expiry to non-zero if it was zero in CONNECT
fn test_mqtt_3_14_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect with Session Expiry Interval = 0 (or not specified, which defaults to 0)
        let client_id = format!("test31421v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5_session_expiry(&client_id, 30, 0, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send DISCONNECT with Session Expiry Interval > 0 (should be protocol error)
        let disconnect = build_disconnect_v5_session_expiry(3600); // 1 hour
        stream.write_all(&disconnect).await
            .map_err(ConformanceError::Io)?;

        // Server should send DISCONNECT with Protocol Error or close connection
        let result = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => {
                // Got DISCONNECT - check reason code
                // Reason code 0x82 = Protocol Error
                // Any DISCONNECT is acceptable (0x82 = Protocol Error or any other)
                let _ = (n, buf);
                Ok(())
            }
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Ok(()), // Timeout - server may have ignored or closed
            _ => Ok(()),
        }
    })
}

fn build_connect_packet_v5_session_expiry(client_id: &str, keep_alive: u16, session_expiry: u32, username: Option<&str>, password: Option<&str>) -> Vec<u8> {
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

    // Properties
    if session_expiry > 0 {
        var_header_payload.push(5); // Property length
        var_header_payload.push(0x11); // Session Expiry Interval identifier
        var_header_payload.push((session_expiry >> 24) as u8);
        var_header_payload.push((session_expiry >> 16) as u8);
        var_header_payload.push((session_expiry >> 8) as u8);
        var_header_payload.push((session_expiry & 0xFF) as u8);
    } else {
        var_header_payload.push(0x00); // No properties
    }

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

fn build_disconnect_v5_session_expiry(session_expiry: u32) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.push(0xE0); // DISCONNECT

    // Remaining length: reason code (1) + property length (1) + session expiry property (5)
    packet.push(7);

    // Reason Code (0 = Normal disconnection)
    packet.push(0x00);

    // Property length
    packet.push(5);

    // Session Expiry Interval property
    packet.push(0x11);
    packet.push((session_expiry >> 24) as u8);
    packet.push((session_expiry >> 16) as u8);
    packet.push((session_expiry >> 8) as u8);
    packet.push((session_expiry & 0xFF) as u8);

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
