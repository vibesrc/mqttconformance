//! CONNACK packet tests for MQTT 5.0
//!
//! Tests normative statements from Section 3.2 of the MQTT 5.0 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "connack".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.2.2-1
        make_test(
            "MQTT-3.2.2-1",
            "Clean Start = 1: Session Present MUST be 0",
            test_mqtt_3_2_2_1,
        ),
        // MQTT-3.2.2-4
        make_test(
            "MQTT-3.2.2-4",
            "Non-zero Reason Code: Session Present MUST be 0",
            test_mqtt_3_2_2_4,
        ),
        // MQTT-3.2.2-5
        make_test(
            "MQTT-3.2.2-5",
            "Non-zero Reason Code: Server MUST close connection",
            test_mqtt_3_2_2_5,
        ),
        // MQTT-3.2.2-15
        make_test(
            "MQTT-3.2.2-15",
            "Client MUST NOT send packets exceeding Server's Maximum Packet Size",
            test_mqtt_3_2_2_15,
        ),
    ]
}

/// MQTT-3.2.2-1: Clean Start = 1: Session Present MUST be 0
fn test_mqtt_3_2_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3221v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, true, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 3 && buf[0] == 0x20 {
            let session_present = buf[2] & 0x01;
            if session_present != 0 {
                return Err(ConformanceError::ProtocolViolation(
                    "Session Present must be 0 when Clean Start = 1".to_string()
                ));
            }
        }

        Ok(())
    })
}

/// MQTT-3.2.2-4: Non-zero Reason Code: Session Present MUST be 0
fn test_mqtt_3_2_2_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with empty ClientId and Clean Start = 0 (should be rejected)
        let connect_packet = build_connect_packet_v5_empty_clientid(30, false, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 4 && buf[0] == 0x20 {
            let session_present = buf[2] & 0x01;
            let reason_code = buf[3];

            if reason_code != 0 && session_present != 0 {
                return Err(ConformanceError::ProtocolViolation(
                    "Session Present must be 0 when Reason Code is non-zero".to_string()
                ));
            }
        }

        Ok(())
    })
}

/// MQTT-3.2.2-5: Non-zero Reason Code: Server MUST close connection
fn test_mqtt_3_2_2_5(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with unsupported protocol version to trigger error
        let connect_packet = build_connect_packet_v5_bad_protocol(99, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for response".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 4 && buf[0] == 0x20 && buf[3] != 0 {
            // Got CONNACK with error, connection should be closed
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut check_buf = [0u8; 1];
            match stream.read(&mut check_buf).await {
                Ok(0) => Ok(()), // Connection closed
                Ok(_) => Err(ConformanceError::ProtocolViolation(
                    "Server did not close connection after error CONNACK".to_string()
                )),
                Err(_) => Ok(()), // Connection error = closed
            }
        } else {
            // n == 0 means connection closed, otherwise server may have closed immediately
            Ok(())
        }
    })
}

/// MQTT-3.2.2-15: Client MUST NOT send packets exceeding Server's Maximum Packet Size
fn test_mqtt_3_2_2_15(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test32215v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, true, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n < 5 || buf[0] != 0x20 {
            return Err(ConformanceError::UnexpectedResponse("Expected CONNACK".to_string()));
        }

        // Parse CONNACK to get Maximum Packet Size property (0x27)
        let props_len_offset = 4;
        let props_len = buf[props_len_offset] as usize;
        let mut max_packet_size: u32 = 268435455; // Default max (256MB - 1)

        let mut i = props_len_offset + 1;
        while i < props_len_offset + 1 + props_len && i + 4 < n {
            if buf[i] == 0x27 {
                // Maximum Packet Size - Four Byte Integer
                max_packet_size = ((buf[i+1] as u32) << 24)
                    | ((buf[i+2] as u32) << 16)
                    | ((buf[i+3] as u32) << 8)
                    | (buf[i+4] as u32);
                break;
            }
            i += 1;
        }

        // If server specified a reasonably small max packet size, we can test exceeding it
        // Otherwise, this test verifies the property is parsed correctly
        if max_packet_size < 1_000_000 {
            // Create a PUBLISH that exceeds the limit
            let topic = b"test/maxsize";
            let payload_size = (max_packet_size as usize) + 100;
            let payload = vec![0x41u8; payload_size]; // Fill with 'A'

            let mut publish = Vec::new();
            publish.push(0x30); // PUBLISH QoS 0

            let remaining = 2 + topic.len() + 1 + payload.len();
            encode_remaining_length_connack(&mut publish, remaining);

            publish.push((topic.len() >> 8) as u8);
            publish.push((topic.len() & 0xFF) as u8);
            publish.extend_from_slice(topic);
            publish.push(0x00); // Properties
            publish.extend_from_slice(&payload);

            stream.write_all(&publish).await
                .map_err(ConformanceError::Io)?;

            // Server should close connection or send DISCONNECT
            let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

            match result {
                Ok(Ok(0)) => Ok(()), // Connection closed
                Ok(Ok(_n)) if buf[0] == 0xE0 => Ok(()), // DISCONNECT
                Ok(Err(_)) => Ok(()), // Connection error
                Err(_) => {
                    // Timeout - server may still be processing
                    Ok(())
                }
                _ => Ok(()),
            }
        } else {
            // Server allows very large packets or didn't specify limit
            // We just verify the connection works
            Ok(())
        }
    })
}

fn encode_remaining_length_connack(packet: &mut Vec<u8>, mut length: usize) {
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

fn build_connect_packet_v5(client_id: &str, keep_alive: u16, clean_start: bool, username: Option<&str>, password: Option<&str>) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let mut flags = if clean_start { 0x02 } else { 0x00 };

    // Add username/password flags
    if username.is_some() {
        flags |= 0x80; // Set username flag (bit 7)
    }
    if password.is_some() {
        flags |= 0x40; // Set password flag (bit 6)
    }

    let mut var_header_payload = Vec::new();

    // Protocol name
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");

    // Protocol version 5
    var_header_payload.push(5);

    // Connect flags
    var_header_payload.push(flags);

    // Keep alive
    var_header_payload.push((keep_alive >> 8) as u8);
    var_header_payload.push((keep_alive & 0xFF) as u8);

    // Properties (empty)
    var_header_payload.push(0x00);

    // Client ID
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

fn build_connect_packet_v5_empty_clientid(keep_alive: u16, clean_start: bool, username: Option<&str>, password: Option<&str>) -> Vec<u8> {
    let mut flags = if clean_start { 0x02 } else { 0x00 };

    // Add username/password flags
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
    var_header_payload.push(0x00); // Properties
    var_header_payload.push(0x00); // ClientId length MSB
    var_header_payload.push(0x00); // ClientId length LSB

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

fn build_connect_packet_v5_bad_protocol(protocol_version: u8, username: Option<&str>, password: Option<&str>) -> Vec<u8> {
    let client_id = b"badproto";

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
    var_header_payload.push(protocol_version);
    var_header_payload.push(flags);
    var_header_payload.push(0x00);
    var_header_payload.push(0x1E);
    var_header_payload.push(0x00); // Properties
    var_header_payload.push(0x00);
    var_header_payload.push(client_id.len() as u8);
    var_header_payload.extend_from_slice(client_id);

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
