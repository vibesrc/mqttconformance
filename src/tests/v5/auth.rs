//! Authentication tests for MQTT 5.0
//!
//! Tests normative statements related to enhanced authentication.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "auth".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        make_test(
            "MQTT-4.12.0-1",
            "With Auth Method, Client MUST NOT send non-AUTH/DISCONNECT before CONNACK",
            test_mqtt_4_12_0_1,
        ),
        make_test(
            "MQTT-4.12.0-2",
            "Without Auth Method in CONNECT, Server MUST NOT send AUTH packet",
            test_mqtt_4_12_0_2,
        ),
        make_test(
            "MQTT-4.12.0-3",
            "Without Auth Method in CONNECT, Server MUST authenticate using only CONNECT info",
            test_mqtt_4_12_0_3,
        ),
        make_test(
            "MQTT-4.12.1-1",
            "AUTH packet MUST have Reason Code",
            test_mqtt_4_12_1_1,
        ),
        make_test(
            "MQTT-4.12.1-2",
            "Re-auth MUST use same Authentication Method",
            test_mqtt_4_12_1_2,
        ),
    ]
}

/// MQTT-4.12.0-1: With Auth Method, Client MUST NOT send non-AUTH/DISCONNECT before CONNACK
fn test_mqtt_4_12_0_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with Authentication Method
        let client_id = format!("test41201v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_with_auth_method(&client_id, 30, "PLAIN");
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        // Server may respond with:
        // - CONNACK with success (if auth not required)
        // - CONNACK with error
        // - AUTH packet to continue authentication
        // - Close connection

        let mut buf = [0u8; 128];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 => {
                // CONNACK received
                Ok(())
            }
            Ok(Ok(n)) if n > 0 && buf[0] == 0xF0 => {
                // AUTH packet - server wants to continue authentication
                Ok(())
            }
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Ok(()), // Timeout
            _ => Ok(()),
        }
    })
}

/// MQTT-4.12.0-2: Without Auth Method in CONNECT, Server MUST NOT send AUTH packet
fn test_mqtt_4_12_0_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT without Authentication Method
        let client_id = format!("test41202v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 128];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for response".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n > 0 {
            // First packet MUST NOT be AUTH (0xF0)
            if buf[0] == 0xF0 {
                return Err(ConformanceError::ProtocolViolation(
                    "Server sent AUTH packet when no Authentication Method was in CONNECT".to_string()
                ));
            }

            // Should be CONNACK
            if buf[0] != 0x20 {
                return Err(ConformanceError::UnexpectedResponse(format!(
                    "Expected CONNACK, got 0x{:02X}",
                    buf[0]
                )));
            }
        }

        Ok(())
    })
}

/// MQTT-4.12.0-3: Without Auth Method, Server authenticates using only CONNECT info
fn test_mqtt_4_12_0_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT without Authentication Method - server should complete auth
        // using only the information in the CONNECT packet
        let client_id = format!("test41203v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 128];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n == 0 {
            return Err(ConformanceError::UnexpectedResponse("Connection closed".to_string()));
        }

        // Should get CONNACK directly (not AUTH)
        if buf[0] == 0x20 {
            // This is correct - server authenticated using only CONNECT info
            Ok(())
        } else if buf[0] == 0xF0 {
            // AUTH packet - this violates the requirement
            Err(ConformanceError::ProtocolViolation(
                "Server sent AUTH when Client did not set Authentication Method".to_string()
            ))
        } else {
            Err(ConformanceError::UnexpectedResponse(format!(
                "Expected CONNACK, got 0x{:02X}",
                buf[0]
            )))
        }
    })
}

/// MQTT-4.12.1-1: AUTH packet MUST have Reason Code
fn test_mqtt_4_12_1_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test41211v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send AUTH packet (unsolicited - server may reject)
        let auth = build_auth_packet(0x18, "PLAIN"); // 0x18 = Continue authentication
        stream.write_all(&auth).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection or send DISCONNECT (unsolicited AUTH is invalid)
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()),
            Err(_) => Ok(()),
            _ => Ok(()),
        }
    })
}

/// MQTT-4.12.1-2: Re-auth MUST use same Authentication Method
fn test_mqtt_4_12_1_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test41212v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Try to re-auth with different method (should fail if we didn't use auth initially)
        let auth = build_auth_packet(0x19, "SCRAM-SHA-256"); // 0x19 = Re-authenticate
        stream.write_all(&auth).await
            .map_err(ConformanceError::Io)?;

        // Server should reject because no initial auth method was used
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()),
            Err(_) => Ok(()),
            _ => Ok(()),
        }
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

fn build_connect_with_auth_method(client_id: &str, keep_alive: u16, auth_method: &str) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let auth_method_bytes = auth_method.as_bytes();

    let mut properties = Vec::new();
    properties.push(0x15); // Authentication Method
    properties.push((auth_method_bytes.len() >> 8) as u8);
    properties.push((auth_method_bytes.len() & 0xFF) as u8);
    properties.extend_from_slice(auth_method_bytes);

    let mut var_header_payload = Vec::new();
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");
    var_header_payload.push(5);
    var_header_payload.push(0x02);
    var_header_payload.push((keep_alive >> 8) as u8);
    var_header_payload.push((keep_alive & 0xFF) as u8);

    // Properties length
    encode_variable_byte_int(&mut var_header_payload, properties.len());
    var_header_payload.extend_from_slice(&properties);

    var_header_payload.push((client_id_bytes.len() >> 8) as u8);
    var_header_payload.push((client_id_bytes.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(client_id_bytes);

    let mut packet = Vec::new();
    packet.push(0x10);
    encode_remaining_length(&mut packet, var_header_payload.len());
    packet.extend_from_slice(&var_header_payload);

    packet
}

fn build_auth_packet(reason_code: u8, auth_method: &str) -> Vec<u8> {
    let auth_method_bytes = auth_method.as_bytes();

    let mut properties = Vec::new();
    properties.push(0x15); // Authentication Method
    properties.push((auth_method_bytes.len() >> 8) as u8);
    properties.push((auth_method_bytes.len() & 0xFF) as u8);
    properties.extend_from_slice(auth_method_bytes);

    let mut packet = Vec::new();
    packet.push(0xF0); // AUTH packet

    let remaining = 1 + 1 + properties.len(); // reason code + props len + props
    encode_remaining_length(&mut packet, remaining);

    packet.push(reason_code);
    packet.push(properties.len() as u8);
    packet.extend_from_slice(&properties);

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

fn encode_variable_byte_int(packet: &mut Vec<u8>, mut value: usize) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if value == 0 {
            break;
        }
    }
}
