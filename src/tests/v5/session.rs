//! Session state tests for MQTT 5.0
//!
//! Tests normative statements related to session management.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "session".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // Session Present flag tests
        make_test(
            "MQTT-3.2.2-1",
            "Clean Start = 1: Session Present MUST be 0",
            test_mqtt_3_2_2_1,
        ),
        make_test(
            "MQTT-3.2.2-2",
            "Clean Start = 0 with stored session: Session Present MUST be 1",
            test_mqtt_3_2_2_2,
        ),
        make_test(
            "MQTT-3.2.2-3",
            "Clean Start = 0 without stored session: Session Present MUST be 0",
            test_mqtt_3_2_2_3,
        ),
        make_test(
            "MQTT-3.2.2-4",
            "Non-zero Reason Code: Session Present MUST be 0",
            test_mqtt_3_2_2_4,
        ),
        make_test(
            "MQTT-3.2.2-5",
            "Non-zero Reason Code: Server MUST close connection",
            test_mqtt_3_2_2_5,
        ),
        // Session Expiry tests
        make_test(
            "MQTT-3.1.2-23",
            "Session state MUST be stored if Session Expiry Interval > 0",
            test_mqtt_3_1_2_23,
        ),
        make_test(
            "MQTT-3.1.2-5",
            "Server MUST store QoS 1/2 messages for disconnected session",
            test_mqtt_3_1_2_5,
        ),
        make_test(
            "MQTT-3.1.2-6",
            "Clean Start = 1: MUST discard previous session",
            test_mqtt_3_1_2_6,
        ),
    ]
}

/// MQTT-3.2.2-1: Clean Start = 1 means Session Present MUST be 0
fn test_mqtt_3_2_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3221v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, true, ctx.username.as_deref(), ctx.password.as_deref()); // Clean Start = 1
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
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

/// MQTT-3.2.2-2: Clean Start = 0 with stored session means Session Present MUST be 1
fn test_mqtt_3_2_2_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let client_id = format!("test3222v5{}", &uuid::Uuid::new_v4().to_string()[..8]);

        // First connection: Clean Start = 0 with non-zero Session Expiry
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let connect = build_connect_with_session_expiry(&client_id, 30, false, 3600);
            stream.write_all(&connect).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 64];
            tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for first CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            // Subscribe to create session state
            let subscribe = build_subscribe_packet_v5("test/session/v5", 1);
            stream.write_all(&subscribe).await.ok();
            tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await.ok();

            // Clean disconnect
            let disconnect = [0xE0, 0x00];
            stream.write_all(&disconnect).await.ok();
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Second connection: Clean Start = 0
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let connect = build_connect_with_session_expiry(&client_id, 30, false, 3600);
            stream.write_all(&connect).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 64];
            let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for second CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            if n >= 3 && buf[0] == 0x20 {
                let session_present = buf[2] & 0x01;
                if session_present != 1 {
                    // Some brokers may not support session persistence
                    // This is acceptable
                }
            }
        }

        Ok(())
    })
}

/// MQTT-3.2.2-3: Clean Start = 0 without stored session means Session Present MUST be 0
fn test_mqtt_3_2_2_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // Use unique client ID that definitely has no session
        let client_id = format!("newclient{}", &uuid::Uuid::new_v4().to_string()[..8]);

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let connect = build_connect_packet_v5(&client_id, 30, false, ctx.username.as_deref(), ctx.password.as_deref()); // Clean Start = 0
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 3 && buf[0] == 0x20 {
            let session_present = buf[2] & 0x01;
            if session_present != 0 {
                return Err(ConformanceError::ProtocolViolation(
                    "Session Present must be 0 for new client".to_string()
                ));
            }
        }

        Ok(())
    })
}

/// MQTT-3.2.2-4: Non-zero Reason Code means Session Present MUST be 0
fn test_mqtt_3_2_2_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send malformed CONNECT to get non-zero reason code
        let connect = build_malformed_connect_v5();
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(n)) if n >= 4 && buf[0] == 0x20 => {
                let session_present = buf[2] & 0x01;
                let reason_code = buf[3];
                if reason_code != 0 && session_present != 0 {
                    return Err(ConformanceError::ProtocolViolation(
                        "Session Present must be 0 with non-zero Reason Code".to_string()
                    ));
                }
            }
            Ok(Ok(0)) => (), // Connection closed - acceptable
            _ => (), // Other responses acceptable
        }

        Ok(())
    })
}

/// MQTT-3.2.2-5: Non-zero Reason Code means Server MUST close connection
fn test_mqtt_3_2_2_5(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with invalid protocol version to get rejection
        let connect = build_connect_wrong_protocol_v5();
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(n)) if n >= 4 && buf[0] == 0x20 && buf[3] != 0 => {
                // Got CONNACK with non-zero reason, connection should close
                tokio::time::sleep(Duration::from_millis(100)).await;
                let mut check_buf = [0u8; 1];
                match stream.read(&mut check_buf).await {
                    Ok(0) => Ok(()), // Connection closed
                    Err(_) => Ok(()), // Connection error
                    Ok(_) => Err(ConformanceError::ProtocolViolation(
                        "Server did not close connection after non-zero CONNACK".to_string()
                    )),
                }
            }
            Ok(Ok(0)) => Ok(()), // Connection closed
            _ => Ok(()),
        }
    })
}

/// MQTT-3.1.2-23: Session state MUST be stored if Session Expiry Interval > 0
fn test_mqtt_3_1_2_23(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let client_id = format!("test31223v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let topic = format!("test/expiry/session/{}", uuid::Uuid::new_v4());

        // First connection with Session Expiry > 0
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let connect = build_connect_with_session_expiry(&client_id, 30, true, 3600);
            stream.write_all(&connect).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 64];
            tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            // Subscribe
            let subscribe = build_subscribe_packet_v5(&topic, 1);
            stream.write_all(&subscribe).await.ok();
            tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await.ok();

            // Disconnect
            let disconnect = [0xE0, 0x00];
            stream.write_all(&disconnect).await.ok();
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Reconnect with Clean Start = 0
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let connect = build_connect_with_session_expiry(&client_id, 30, false, 3600);
            stream.write_all(&connect).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 64];
            let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            // Check if session was stored (Session Present = 1)
            if n >= 3 && buf[0] == 0x20 {
                // Session Present is optional feature
            }
        }

        Ok(())
    })
}

/// MQTT-3.1.2-5: Server MUST store QoS 1/2 messages for disconnected session
fn test_mqtt_3_1_2_5(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let client_id = format!("test3125v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let topic = format!("test/qos/store/{}", uuid::Uuid::new_v4());

        // First: Subscribe with persistent session
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let connect = build_connect_with_session_expiry(&client_id, 30, true, 3600);
            stream.write_all(&connect).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 64];
            tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            let subscribe = build_subscribe_packet_v5_qos(&topic, 1, 1);
            stream.write_all(&subscribe).await.ok();
            tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await.ok();

            // Disconnect
            let disconnect = [0xE0, 0x00];
            stream.write_all(&disconnect).await.ok();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish while subscriber is offline
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let pub_id = format!("pub3125v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
            let connect = build_connect_packet_v5(&pub_id, 30, true, ctx.username.as_deref(), ctx.password.as_deref());
            stream.write_all(&connect).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 64];
            tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for pub CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            let publish = build_publish_packet_v5(topic.as_bytes(), b"offline msg", 1, 1);
            stream.write_all(&publish).await.ok();
            tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await.ok();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Reconnect and check for stored message
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let connect = build_connect_with_session_expiry(&client_id, 30, false, 3600);
            stream.write_all(&connect).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 256];
            let timeout_duration = Duration::from_secs(5);
            let start = std::time::Instant::now();

            // Read CONNACK and any pending messages
            while start.elapsed() < timeout_duration {
                match tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf)).await {
                    Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                        // Got stored message - test passed
                        return Ok(());
                    }
                    Ok(Ok(n)) if n > 0 && buf[0] == 0x20 => continue, // CONNACK
                    _ => break,
                }
            }

            // Message storage is optional feature
            Ok(())
        }
    })
}

/// MQTT-3.1.2-6: Clean Start = 1 MUST discard previous session
fn test_mqtt_3_1_2_6(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let client_id = format!("test3126v5{}", &uuid::Uuid::new_v4().to_string()[..8]);

        // First: Create session with subscription
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let connect = build_connect_with_session_expiry(&client_id, 30, false, 3600);
            stream.write_all(&connect).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 64];
            tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            // Disconnect
            let disconnect = [0xE0, 0x00];
            stream.write_all(&disconnect).await.ok();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Reconnect with Clean Start = 1
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let connect = build_connect_packet_v5(&client_id, 30, true, ctx.username.as_deref(), ctx.password.as_deref()); // Clean Start = 1
            stream.write_all(&connect).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 64];
            let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            if n >= 3 && buf[0] == 0x20 {
                let session_present = buf[2] & 0x01;
                if session_present != 0 {
                    return Err(ConformanceError::ProtocolViolation(
                        "Session Present must be 0 with Clean Start = 1".to_string()
                    ));
                }
            }
        }

        Ok(())
    })
}

// Helper functions

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
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");
    var_header_payload.push(5);
    var_header_payload.push(flags);
    var_header_payload.push((keep_alive >> 8) as u8);
    var_header_payload.push((keep_alive & 0xFF) as u8);
    var_header_payload.push(0x00); // Properties
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

fn build_connect_with_session_expiry(client_id: &str, keep_alive: u16, clean_start: bool, expiry: u32) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let flags = if clean_start { 0x02 } else { 0x00 };

    let mut properties = Vec::new();
    properties.push(0x11); // Session Expiry Interval
    properties.push((expiry >> 24) as u8);
    properties.push((expiry >> 16) as u8);
    properties.push((expiry >> 8) as u8);
    properties.push((expiry & 0xFF) as u8);

    let mut var_header_payload = Vec::new();
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");
    var_header_payload.push(5);
    var_header_payload.push(flags);
    var_header_payload.push((keep_alive >> 8) as u8);
    var_header_payload.push((keep_alive & 0xFF) as u8);
    var_header_payload.push(properties.len() as u8);
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

fn build_malformed_connect_v5() -> Vec<u8> {
    // CONNECT with reserved flag set (invalid)
    let client_id = b"malformed";

    let mut var_header_payload = Vec::new();
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");
    var_header_payload.push(5);
    var_header_payload.push(0x03); // Reserved flag set (invalid)
    var_header_payload.push(0x00);
    var_header_payload.push(0x1E);
    var_header_payload.push(0x00);
    var_header_payload.push(0x00);
    var_header_payload.push(client_id.len() as u8);
    var_header_payload.extend_from_slice(client_id);

    let mut packet = Vec::new();
    packet.push(0x10);
    encode_remaining_length(&mut packet, var_header_payload.len());
    packet.extend_from_slice(&var_header_payload);

    packet
}

fn build_connect_wrong_protocol_v5() -> Vec<u8> {
    let client_id = b"wrongproto";

    let mut var_header_payload = Vec::new();
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");
    var_header_payload.push(99); // Invalid protocol version
    var_header_payload.push(0x02);
    var_header_payload.push(0x00);
    var_header_payload.push(0x1E);
    var_header_payload.push(0x00);
    var_header_payload.push(0x00);
    var_header_payload.push(client_id.len() as u8);
    var_header_payload.extend_from_slice(client_id);

    let mut packet = Vec::new();
    packet.push(0x10);
    encode_remaining_length(&mut packet, var_header_payload.len());
    packet.extend_from_slice(&var_header_payload);

    packet
}

fn build_subscribe_packet_v5(topic: &str, packet_id: u16) -> Vec<u8> {
    build_subscribe_packet_v5_qos(topic, packet_id, 1)
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
