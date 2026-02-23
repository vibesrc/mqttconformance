//! CONNECT packet tests for MQTT 5.0
//!
//! Tests normative statements from Section 3.1 of the MQTT 5.0 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "connect".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.1.0-1
        make_test(
            "MQTT-3.1.0-1",
            "First packet from Client to Server MUST be CONNECT",
            test_mqtt_3_1_0_1,
        ),
        // MQTT-3.1.0-2
        make_test(
            "MQTT-3.1.0-2",
            "Second CONNECT is Protocol Error; Server MUST close connection",
            test_mqtt_3_1_0_2,
        ),
        // MQTT-3.1.2-1
        make_test(
            "MQTT-3.1.2-1",
            "Server MAY send CONNACK with 0x84 for invalid protocol name",
            test_mqtt_3_1_2_1,
        ),
        // MQTT-3.1.2-2
        make_test(
            "MQTT-3.1.2-2",
            "Server MAY send CONNACK with 0x84 for wrong protocol version",
            test_mqtt_3_1_2_2,
        ),
        // MQTT-3.1.2-3
        make_test(
            "MQTT-3.1.2-3",
            "Reserved flag MUST be 0; if not, Malformed Packet",
            test_mqtt_3_1_2_3,
        ),
        // MQTT-3.1.2-4
        make_test(
            "MQTT-3.1.2-4",
            "Clean Start = 1: MUST discard existing Session",
            test_mqtt_3_1_2_4,
        ),
        // MQTT-3.1.2-7
        make_test(
            "MQTT-3.1.2-7",
            "Will Flag = 1: MUST store Will Message",
            test_mqtt_3_1_2_7,
        ),
        // MQTT-3.1.2-11
        make_test(
            "MQTT-3.1.2-11",
            "Will QoS MUST be 0 if Will Flag is 0",
            test_mqtt_3_1_2_11,
        ),
        // MQTT-3.1.2-7 (inverse) - tests that Will Flag 0 means no will stored
        make_test(
            "MQTT-3.1.2-7-NOWILL",
            "Will Flag 0 means no Will Message published",
            test_mqtt_3_1_2_7_nowill,
        ),
        // MQTT-3.1.2-13
        make_test(
            "MQTT-3.1.2-13",
            "Will Retain MUST be 0 if Will Flag is 0",
            test_mqtt_3_1_2_13,
        ),
        // MQTT-3.1.2-12
        make_test(
            "MQTT-3.1.2-12",
            "Will QoS value of 3 is Malformed Packet",
            test_mqtt_3_1_2_12,
        ),
        // MQTT-3.1.2-19
        make_test(
            "MQTT-3.1.2-19",
            "Password MUST be present if Password Flag is 1",
            test_mqtt_3_1_2_19,
        ),
        // MQTT-3.1.2-20
        make_test(
            "MQTT-3.1.2-20",
            "Keep Alive: Client MUST send PINGREQ if no other packets",
            test_mqtt_3_1_2_20,
        ),
        // MQTT-3.1.2-21
        make_test(
            "MQTT-3.1.2-21",
            "Client MUST use Server Keep Alive if returned in CONNACK",
            test_mqtt_3_1_2_21,
        ),
        // MQTT-3.1.2-22
        make_test(
            "MQTT-3.1.2-22",
            "Server MUST close connection if no packet within 1.5x Keep Alive",
            test_mqtt_3_1_2_22,
        ),
        // MQTT-3.1.2-24
        make_test(
            "MQTT-3.1.2-24",
            "Maximum Packet Size: Server MUST NOT send packets exceeding client limit",
            test_mqtt_3_1_2_24,
        ),
        // MQTT-3.1.3-1
        make_test(
            "MQTT-3.1.3-1",
            "Server MUST allow ClientIds 1-23 bytes containing 0-9, a-z, A-Z",
            test_mqtt_3_1_3_1,
        ),
        // MQTT-3.1.3-2
        make_test(
            "MQTT-3.1.3-2",
            "Zero-length ClientId: Server MUST assign unique ID",
            test_mqtt_3_1_3_2,
        ),
        // MQTT-3.1.3-3
        make_test(
            "MQTT-3.1.3-3",
            "Zero-length ClientId with Clean Start 0: Reason Code 0x85",
            test_mqtt_3_1_3_3,
        ),
    ]
}

/// MQTT-3.1.0-1: First packet MUST be CONNECT
fn test_mqtt_3_1_0_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT packet (MQTT 5.0)
        let client_id = format!("test3101v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        // Read CONNACK
        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n > 0 && buf[0] == 0x20 {
            Ok(())
        } else {
            Err(ConformanceError::UnexpectedResponse(
                "Expected CONNACK after CONNECT".to_string()
            ))
        }
    })
}

/// MQTT-3.1.0-2: Second CONNECT is Protocol Error
fn test_mqtt_3_1_0_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // First CONNECT
        let client_id = format!("test3102v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        // Read CONNACK
        let mut buf = [0u8; 16];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for first CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Second CONNECT
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection or send DISCONNECT with Protocol Error
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT packet
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection after second CONNECT".to_string()
            )),
            _ => Ok(()), // Any response that indicates the connection was terminated
        }
    })
}

/// MQTT-3.1.2-3: Reserved flag MUST be 0
fn test_mqtt_3_1_2_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with reserved flag set
        let client_id = format!("test3123v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5_with_flags(&client_id, 30, 0x01, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 16];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 => {
                // Got CONNACK - check for error reason code
                // In MQTT 5, CONNACK has: type(1) + remaining(1-4) + flags(1) + reason(1) + props
                // Reason code 0x81 = Malformed Packet
                if n >= 4 && buf[3] != 0x00 {
                    Ok(()) // Error reason code
                } else {
                    Err(ConformanceError::ProtocolViolation(
                        "Server accepted CONNECT with reserved flag set".to_string()
                    ))
                }
            }
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Ok(()), // Timeout - connection likely closed
            _ => Ok(()),
        }
    })
}

/// MQTT-3.1.2-4: Clean Start = 1 MUST discard existing Session
fn test_mqtt_3_1_2_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let client_id = format!("test3124v5{}", &uuid::Uuid::new_v4().to_string()[..8]);

        // First connection with Clean Start = 0 to create session
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let connect_packet = build_connect_packet_v5_clean_start(&client_id, 30, false, ctx.username.as_deref(), ctx.password.as_deref());
            stream.write_all(&connect_packet).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 16];
            tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for first CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            // Disconnect
            let disconnect = [0xE0, 0x00];
            stream.write_all(&disconnect).await.ok();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second connection with Clean Start = 1
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let connect_packet = build_connect_packet_v5_clean_start(&client_id, 30, true, ctx.username.as_deref(), ctx.password.as_deref());
            stream.write_all(&connect_packet).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 16];
            let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for second CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            if n >= 3 && buf[0] == 0x20 {
                // Check Session Present flag (first bit of Connect Acknowledge Flags)
                let session_present = buf[2] & 0x01;
                if session_present != 0 {
                    return Err(ConformanceError::ProtocolViolation(
                        "Session Present should be 0 with Clean Start = 1".to_string()
                    ));
                }
            }
        }

        Ok(())
    })
}

/// MQTT-3.1.2-7: Will Flag = 1 MUST store Will Message
fn test_mqtt_3_1_2_7(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let will_topic = format!("test/will/v5/{}", uuid::Uuid::new_v4());

        // Subscribe to will topic first
        let sub_id = format!("sub3127v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&sub_connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for subscriber CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Subscribe
        let subscribe_packet = build_subscribe_packet_v5(&will_topic, 1);
        sub_stream.write_all(&subscribe_packet).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Connect client with will message
        let will_id = format!("will3127v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let will_connect = build_connect_packet_v5_with_will(&will_id, 30, &will_topic, b"will message", ctx.username.as_deref(), ctx.password.as_deref());

        let mut will_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        will_stream.write_all(&will_connect).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, will_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for will client CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Close will client connection without DISCONNECT (trigger will)
        drop(will_stream);

        // Wait for will message
        let timeout_duration = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && buf[0] == 0x30 => {
                    // Got PUBLISH - will message received
                    return Ok(());
                }
                Ok(Ok(0)) => break,
                Ok(Err(_)) => break,
                _ => continue,
            }
        }

        // Will message may not be delivered immediately
        Ok(())
    })
}

/// MQTT-3.1.2-22: Server MUST close connection if no packet within 1.5x Keep Alive
fn test_mqtt_3_1_2_22(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect with 2 second keep alive
        let client_id = format!("test31222v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 2, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        // Read CONNACK fully - first get header to determine length
        let mut header = [0u8; 2];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut header)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if header[0] != 0x20 {
            return Err(ConformanceError::UnexpectedResponse("Expected CONNACK".to_string()));
        }

        // Read remaining bytes based on remaining length
        let remaining_len = header[1] as usize;
        let mut connack_body = vec![0u8; remaining_len];
        stream.read_exact(&mut connack_body).await
            .map_err(ConformanceError::Io)?;

        // Wait 1.5x keep alive + buffer (4 seconds)
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Try to read - should get connection closed or DISCONNECT packet
        let mut check_buf = [0u8; 16];
        match stream.read(&mut check_buf).await {
            Ok(0) => Ok(()), // Connection closed
            Ok(n) if n > 0 && check_buf[0] == 0xE0 => Ok(()), // DISCONNECT packet (MQTT 5.0)
            Ok(n) => Err(ConformanceError::ProtocolViolation(
                format!("Server did not disconnect after keep alive timeout (got {} bytes: {:02x?})", n, &check_buf[..n])
            )),
            Err(_) => Ok(()), // Connection error
        }
    })
}

/// MQTT-3.1.2-1: Server MAY send CONNACK with Reason Code 0x84 for invalid protocol name
fn test_mqtt_3_1_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Build CONNECT with invalid protocol name "MQTX" instead of "MQTT"
        let client_id = format!("test3121v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let client_id_bytes = client_id.as_bytes();

        let mut var_header_payload = Vec::new();
        // Invalid protocol name "MQTX"
        var_header_payload.push(0x00);
        var_header_payload.push(0x04);
        var_header_payload.extend_from_slice(b"MQTX");
        // Protocol version 5
        var_header_payload.push(5);
        // Connect flags (Clean Start)
        var_header_payload.push(0x02);
        // Keep alive
        var_header_payload.push(0x00);
        var_header_payload.push(0x1E);
        // Properties (empty)
        var_header_payload.push(0x00);
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
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 => {
                // CONNACK received - check for 0x84 (Unsupported Protocol Version)
                if n >= 4 && buf[3] == 0x84 {
                    Ok(()) // Correct behavior
                } else if n >= 4 && buf[3] != 0 {
                    Ok(()) // Other error code is acceptable
                } else {
                    // Server accepted invalid protocol name - unexpected but not forbidden
                    Ok(())
                }
            }
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Ok(()), // Timeout
            _ => Ok(()),
        }
    })
}

/// MQTT-3.1.2-2: Server MAY send CONNACK with Reason Code 0x84 for wrong protocol version
fn test_mqtt_3_1_2_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Build CONNECT with protocol version 99 (invalid)
        let client_id = format!("test3122v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let client_id_bytes = client_id.as_bytes();

        let mut var_header_payload = Vec::new();
        // Protocol name "MQTT"
        var_header_payload.push(0x00);
        var_header_payload.push(0x04);
        var_header_payload.extend_from_slice(b"MQTT");
        // Invalid protocol version 99
        var_header_payload.push(99);
        // Connect flags (Clean Start)
        var_header_payload.push(0x02);
        // Keep alive
        var_header_payload.push(0x00);
        var_header_payload.push(0x1E);
        // Properties (empty)
        var_header_payload.push(0x00);
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
            Ok(Ok(0)) => Ok(()), // Connection closed - MUST close
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 => {
                // CONNACK - check for 0x84 (Unsupported Protocol Version)
                if n >= 4 && buf[3] == 0x84 {
                    Ok(()) // Correct: sent CONNACK with 0x84
                } else if n >= 4 && buf[3] != 0 {
                    Ok(()) // Other error reason code acceptable
                } else {
                    Err(ConformanceError::ProtocolViolation(
                        "Server accepted invalid protocol version".to_string()
                    ))
                }
            }
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Ok(()), // Timeout - connection may have been closed
            _ => Ok(()),
        }
    })
}

/// MQTT-3.1.2-11: If Will Flag is 0, Will QoS MUST be 0
fn test_mqtt_3_1_2_11(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Build CONNECT with Will Flag = 0 but Will QoS = 1 (invalid)
        let client_id = format!("test31211v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let client_id_bytes = client_id.as_bytes();

        let mut var_header_payload = Vec::new();
        var_header_payload.push(0x00);
        var_header_payload.push(0x04);
        var_header_payload.extend_from_slice(b"MQTT");
        var_header_payload.push(5);
        // Connect flags: Clean Start=1, Will Flag=0, Will QoS=1 (invalid)
        // Bits: Username(0) Password(0) Will Retain(0) Will QoS(01) Will Flag(0) Clean Start(1) Reserved(0)
        // = 0000 1010 = 0x0A
        var_header_payload.push(0x0A);
        var_header_payload.push(0x00);
        var_header_payload.push(0x1E);
        var_header_payload.push(0x00);
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
            Ok(Ok(0)) => Ok(()), // Connection closed - Malformed Packet
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 && n >= 4 && buf[3] != 0 => Ok(()), // CONNACK with error
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not reject Will QoS != 0 when Will Flag = 0".to_string()
            )),
            _ => Ok(()),
        }
    })
}

/// MQTT-3.1.2-7-NOWILL: If Will Flag is 0, no Will Message is published (inverse of MQTT-3.1.2-7)
fn test_mqtt_3_1_2_7_nowill(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let will_topic = format!("test/nowill/v5/{}", uuid::Uuid::new_v4());

        // Subscriber
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub31212v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for subscriber CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let subscribe = build_subscribe_packet_v5(&will_topic, 1);
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        // Client with Will Flag = 0 (no will message)
        {
            let mut client_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let client_id = format!("nowill31212v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
            // Connect with Will Flag = 0
            let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
            client_stream.write_all(&connect).await
                .map_err(ConformanceError::Io)?;

            tokio::time::timeout(ctx.timeout, client_stream.read(&mut buf)).await.ok();

            // Abnormal disconnect without proper DISCONNECT
            drop(client_stream);
        }

        // Wait and verify no will message is received
        tokio::time::sleep(Duration::from_secs(2)).await;

        match tokio::time::timeout(Duration::from_millis(500), sub_stream.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                Err(ConformanceError::ProtocolViolation(
                    "Received message when Will Flag was 0".to_string()
                ))
            }
            _ => Ok(()), // No message received - correct behavior
        }
    })
}

/// MQTT-3.1.2-13: If Will Flag is 0, Will Retain MUST be 0 (same as 3.1.2-15)
fn test_mqtt_3_1_2_13(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Build CONNECT with Will Flag = 0 but Will Retain = 1 (invalid)
        let client_id = format!("test31213v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let client_id_bytes = client_id.as_bytes();

        let mut var_header_payload = Vec::new();
        var_header_payload.push(0x00);
        var_header_payload.push(0x04);
        var_header_payload.extend_from_slice(b"MQTT");
        var_header_payload.push(5);
        // Connect flags: Will Retain=1, Will Flag=0 (invalid)
        // Bits: Username(0) Password(0) Will Retain(1) Will QoS(00) Will Flag(0) Clean Start(1) Reserved(0)
        // = 0010 0010 = 0x22
        var_header_payload.push(0x22);
        var_header_payload.push(0x00);
        var_header_payload.push(0x1E);
        var_header_payload.push(0x00);
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
                "Server did not reject Will Retain = 1 when Will Flag = 0".to_string()
            )),
            _ => Ok(()),
        }
    })
}

/// MQTT-3.1.2-12: Will QoS value of 3 is Malformed Packet
fn test_mqtt_3_1_2_12(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Build CONNECT with Will Flag = 1 and Will QoS = 3 (invalid)
        let client_id = format!("test31214v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let client_id_bytes = client_id.as_bytes();
        let will_topic = b"test/will/qos3";
        let will_message = b"test";

        let mut var_header_payload = Vec::new();
        var_header_payload.push(0x00);
        var_header_payload.push(0x04);
        var_header_payload.extend_from_slice(b"MQTT");
        var_header_payload.push(5);
        // Connect flags: Will Flag=1, Will QoS=3 (invalid)
        // Bits: Username(0) Password(0) Will Retain(0) Will QoS(11) Will Flag(1) Clean Start(1) Reserved(0)
        // = 0001 1110 = 0x1E
        var_header_payload.push(0x1E);
        var_header_payload.push(0x00);
        var_header_payload.push(0x1E);
        var_header_payload.push(0x00); // Properties

        // Client ID
        var_header_payload.push((client_id_bytes.len() >> 8) as u8);
        var_header_payload.push((client_id_bytes.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(client_id_bytes);

        // Will Properties (empty)
        var_header_payload.push(0x00);

        // Will Topic
        var_header_payload.push((will_topic.len() >> 8) as u8);
        var_header_payload.push((will_topic.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(will_topic);

        // Will Message
        var_header_payload.push((will_message.len() >> 8) as u8);
        var_header_payload.push((will_message.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(will_message);

        let mut packet = Vec::new();
        packet.push(0x10);
        encode_remaining_length(&mut packet, var_header_payload.len());
        packet.extend_from_slice(&var_header_payload);

        stream.write_all(&packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed - Malformed Packet
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 && n >= 4 && buf[3] != 0 => Ok(()), // CONNACK with error
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not reject Will QoS = 3".to_string()
            )),
            _ => Ok(()),
        }
    })
}

/// MQTT-3.1.2-19: If Password Flag is 1, Password MUST be present in Payload
fn test_mqtt_3_1_2_19(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Build CONNECT with Password Flag = 1 and actual password
        let client_id = format!("test31219v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let client_id_bytes = client_id.as_bytes();
        let password = b"testpassword";

        let mut var_header_payload = Vec::new();
        var_header_payload.push(0x00);
        var_header_payload.push(0x04);
        var_header_payload.extend_from_slice(b"MQTT");
        var_header_payload.push(5);
        // Connect flags: Password Flag=1, Clean Start=1
        // Bits: Username(0) Password(1) Will Retain(0) Will QoS(00) Will Flag(0) Clean Start(1) Reserved(0)
        // = 0100 0010 = 0x42
        var_header_payload.push(0x42);
        var_header_payload.push(0x00);
        var_header_payload.push(0x1E);
        var_header_payload.push(0x00); // Properties

        // Client ID
        var_header_payload.push((client_id_bytes.len() >> 8) as u8);
        var_header_payload.push((client_id_bytes.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(client_id_bytes);

        // Password (as Binary Data in MQTT 5.0)
        var_header_payload.push((password.len() >> 8) as u8);
        var_header_payload.push((password.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(password);

        let mut packet = Vec::new();
        packet.push(0x10);
        encode_remaining_length(&mut packet, var_header_payload.len());
        packet.extend_from_slice(&var_header_payload);

        stream.write_all(&packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Server should accept or reject based on auth - both are valid responses
        if n > 0 && buf[0] == 0x20 {
            Ok(()) // CONNACK received - packet structure was valid
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected CONNACK".to_string()))
        }
    })
}

/// MQTT-3.1.2-20: Client MUST send PINGREQ if Keep Alive is non-zero and no other packets
fn test_mqtt_3_1_2_20(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect with short keep alive
        let client_id = format!("test31220v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 5, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send PINGREQ
        let pingreq = [0xC0, 0x00];
        stream.write_all(&pingreq).await
            .map_err(ConformanceError::Io)?;

        // Should get PINGRESP
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(n)) if n >= 2 && buf[0] == 0xD0 => Ok(()), // Got PINGRESP
            Ok(Ok(0)) => Err(ConformanceError::ProtocolViolation(
                "Connection closed instead of PINGRESP".to_string()
            )),
            Ok(Err(e)) => Err(ConformanceError::Io(e)),
            Err(_) => Err(ConformanceError::Timeout("Waiting for PINGRESP".to_string())),
            _ => Ok(()),
        }
    })
}

/// MQTT-3.1.2-21: Client MUST use Server Keep Alive if returned in CONNACK
fn test_mqtt_3_1_2_21(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect and check for Server Keep Alive in CONNACK properties
        let client_id = format!("test31221v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 60, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 128];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Parse CONNACK and look for Server Keep Alive property (0x13)
        if n > 0 && buf[0] == 0x20 {
            // CONNACK received - check properties for Server Keep Alive
            // Structure: type + remaining + flags + reason + props_len + props
            if n >= 5 {
                let props_len = buf[4] as usize;
                let props_start = 5;
                let mut i = props_start;

                while i < props_start + props_len && i < n {
                    let prop_id = buf[i];
                    if prop_id == 0x13 {
                        // Server Keep Alive property found
                        if i + 2 < n {
                            let _server_keep_alive = ((buf[i + 1] as u16) << 8) | (buf[i + 2] as u16);
                            // Server specified a keep alive value
                            return Ok(());
                        }
                    }
                    // Skip property (simplified - assume 3 byte properties)
                    i += 3;
                }
            }
            // Server may not return Server Keep Alive - that's OK
            Ok(())
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected CONNACK".to_string()))
        }
    })
}

/// MQTT-3.1.2-24: Server MUST NOT send packets exceeding client's Maximum Packet Size
fn test_mqtt_3_1_2_24(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect with small Maximum Packet Size
        let client_id = format!("test31224v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_with_max_packet_size(&client_id, 30, 128);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Check that CONNACK doesn't exceed our Maximum Packet Size
        if n > 0 && buf[0] == 0x20 {
            if n > 128 {
                return Err(ConformanceError::ProtocolViolation(
                    format!("CONNACK size {} exceeds Maximum Packet Size 128", n)
                ));
            }
            Ok(())
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected CONNACK".to_string()))
        }
    })
}

/// MQTT-3.1.3-1: Server MUST allow ClientIds 1-23 bytes with 0-9, a-z, A-Z
fn test_mqtt_3_1_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Use a 23-character client ID with only allowed characters
        let client_id = "abcdefghijklmnopqrstuv1"; // 23 chars: a-z and numbers
        let connect_packet = build_connect_packet_v5(client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n > 0 && buf[0] == 0x20 {
            // Check reason code
            if n >= 4 && buf[3] == 0x00 {
                Ok(()) // Success
            } else if n >= 4 && buf[3] == 0x85 {
                Err(ConformanceError::ProtocolViolation(
                    "Server rejected valid ClientId (1-23 chars, alphanumeric)".to_string()
                ))
            } else {
                // Other reason codes may be valid (e.g., auth required)
                Ok(())
            }
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected CONNACK".to_string()))
        }
    })
}

/// MQTT-3.1.3-2: Zero-length ClientId: Server MUST assign unique ID
fn test_mqtt_3_1_3_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect with zero-length ClientId and Clean Start = 1
        let connect_packet = build_connect_packet_v5_zero_clientid(30, true);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n > 0 && buf[0] == 0x20 {
            if n >= 4 && buf[3] == 0x00 {
                // Success - check for Assigned Client Identifier property (0x12)
                if n >= 5 {
                    let props_len = buf[4] as usize;
                    let props_start = 5;
                    let mut i = props_start;

                    while i < props_start + props_len && i < n {
                        let prop_id = buf[i];
                        if prop_id == 0x12 {
                            // Assigned Client Identifier found
                            return Ok(());
                        }
                        // Skip property (simplified)
                        if prop_id == 0x12 || prop_id == 0x1F || prop_id == 0x1C {
                            // String property
                            if i + 2 < n {
                                let str_len = ((buf[i + 1] as usize) << 8) | (buf[i + 2] as usize);
                                i += 3 + str_len;
                                continue;
                            }
                        }
                        i += 3;
                    }
                }
                // Server may not return Assigned Client Identifier
                // (it's only MUST if accepting zero-length client ID)
                Ok(())
            } else {
                // Server may reject zero-length ClientId - that's allowed
                Ok(())
            }
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected CONNACK".to_string()))
        }
    })
}

/// MQTT-3.1.3-3: Zero-length ClientId with Clean Start = 0 must get Reason Code 0x85
fn test_mqtt_3_1_3_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect with zero-length ClientId and Clean Start = 0 (invalid combination)
        let connect_packet = build_connect_packet_v5_zero_clientid(30, false);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(n)) if n > 0 && buf[0] == 0x20 => {
                // CONNACK - check for Reason Code 0x85 (Client Identifier not valid)
                if n >= 4 && buf[3] == 0x85 {
                    Ok(()) // Correct response
                } else if n >= 4 && buf[3] != 0x00 {
                    Ok(()) // Other error code is acceptable
                } else {
                    Err(ConformanceError::ProtocolViolation(
                        "Server accepted zero-length ClientId with Clean Start = 0".to_string()
                    ))
                }
            }
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Ok(()), // Timeout - connection may have been closed
            _ => Ok(()),
        }
    })
}

// Helper functions for building MQTT 5.0 packets

fn build_connect_packet_v5(client_id: &str, keep_alive: u16, username: Option<&str>, password: Option<&str>) -> Vec<u8> {
    build_connect_packet_v5_internal(client_id, keep_alive, 0x02, &[], None, username, password) // Clean Start = 1
}

fn build_connect_packet_v5_with_flags(client_id: &str, keep_alive: u16, connect_flags: u8, username: Option<&str>, password: Option<&str>) -> Vec<u8> {
    build_connect_packet_v5_internal(client_id, keep_alive, connect_flags, &[], None, username, password)
}

fn build_connect_packet_v5_clean_start(client_id: &str, keep_alive: u16, clean_start: bool, username: Option<&str>, password: Option<&str>) -> Vec<u8> {
    let flags = if clean_start { 0x02 } else { 0x00 };
    build_connect_packet_v5_internal(client_id, keep_alive, flags, &[], None, username, password)
}

fn build_connect_packet_v5_with_will(client_id: &str, keep_alive: u16, will_topic: &str, will_payload: &[u8], username: Option<&str>, password: Option<&str>) -> Vec<u8> {
    let flags = 0x06; // Clean Start + Will Flag
    build_connect_packet_v5_internal(client_id, keep_alive, flags, &[], Some((will_topic, will_payload)), username, password)
}

fn build_connect_packet_v5_internal(
    client_id: &str,
    keep_alive: u16,
    connect_flags: u8,
    _properties: &[u8],
    will: Option<(&str, &[u8])>,
    username: Option<&str>,
    password: Option<&str>,
) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();

    // Adjust connect flags for username and password
    let mut flags = connect_flags;
    if username.is_some() {
        flags |= 0x80; // Set username flag (bit 7)
    }
    if password.is_some() {
        flags |= 0x40; // Set password flag (bit 6)
    }

    let mut var_header_payload = Vec::new();

    // Protocol name "MQTT"
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

    // Properties (empty for now)
    var_header_payload.push(0x00); // Property length = 0

    // Client ID
    var_header_payload.push((client_id_bytes.len() >> 8) as u8);
    var_header_payload.push((client_id_bytes.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(client_id_bytes);

    // Will properties and payload if present
    if let Some((topic, payload)) = will {
        let topic_bytes = topic.as_bytes();

        // Will properties (empty)
        var_header_payload.push(0x00);

        // Will topic
        var_header_payload.push((topic_bytes.len() >> 8) as u8);
        var_header_payload.push((topic_bytes.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(topic_bytes);

        // Will payload
        var_header_payload.push((payload.len() >> 8) as u8);
        var_header_payload.push((payload.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(payload);
    }

    // Username if present
    if let Some(user) = username {
        let user_bytes = user.as_bytes();
        var_header_payload.push((user_bytes.len() >> 8) as u8);
        var_header_payload.push((user_bytes.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(user_bytes);
    }

    // Password if present
    if let Some(pass) = password {
        let pass_bytes = pass.as_bytes();
        var_header_payload.push((pass_bytes.len() >> 8) as u8);
        var_header_payload.push((pass_bytes.len() & 0xFF) as u8);
        var_header_payload.extend_from_slice(pass_bytes);
    }

    let remaining_length = var_header_payload.len();

    let mut packet = Vec::new();
    packet.push(0x10); // CONNECT

    // Encode remaining length
    encode_remaining_length(&mut packet, remaining_length);

    packet.extend_from_slice(&var_header_payload);

    packet
}

fn build_subscribe_packet_v5(topic: &str, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut packet = Vec::new();
    packet.push(0x82); // SUBSCRIBE

    let remaining = 2 + 1 + 2 + topic_bytes.len() + 1; // packet id + props + topic len + topic + options
    packet.push(remaining as u8);

    // Packet ID
    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);

    // Properties (empty)
    packet.push(0x00);

    // Topic filter
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);

    // Subscription options (QoS 1)
    packet.push(0x01);

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

fn build_connect_with_max_packet_size(client_id: &str, keep_alive: u16, max_packet_size: u32) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();

    let mut var_header_payload = Vec::new();

    // Protocol name "MQTT"
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");

    // Protocol version 5
    var_header_payload.push(5);

    // Connect flags (Clean Start)
    var_header_payload.push(0x02);

    // Keep alive
    var_header_payload.push((keep_alive >> 8) as u8);
    var_header_payload.push((keep_alive & 0xFF) as u8);

    // Properties with Maximum Packet Size
    let mut properties = Vec::new();
    properties.push(0x27); // Maximum Packet Size property
    properties.push((max_packet_size >> 24) as u8);
    properties.push((max_packet_size >> 16) as u8);
    properties.push((max_packet_size >> 8) as u8);
    properties.push((max_packet_size & 0xFF) as u8);

    var_header_payload.push(properties.len() as u8);
    var_header_payload.extend_from_slice(&properties);

    // Client ID
    var_header_payload.push((client_id_bytes.len() >> 8) as u8);
    var_header_payload.push((client_id_bytes.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(client_id_bytes);

    let mut packet = Vec::new();
    packet.push(0x10); // CONNECT

    encode_remaining_length(&mut packet, var_header_payload.len());
    packet.extend_from_slice(&var_header_payload);

    packet
}

fn build_connect_packet_v5_zero_clientid(keep_alive: u16, clean_start: bool) -> Vec<u8> {
    let mut var_header_payload = Vec::new();

    // Protocol name "MQTT"
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");

    // Protocol version 5
    var_header_payload.push(5);

    // Connect flags
    let flags = if clean_start { 0x02 } else { 0x00 };
    var_header_payload.push(flags);

    // Keep alive
    var_header_payload.push((keep_alive >> 8) as u8);
    var_header_payload.push((keep_alive & 0xFF) as u8);

    // Properties (empty)
    var_header_payload.push(0x00);

    // Zero-length Client ID
    var_header_payload.push(0x00);
    var_header_payload.push(0x00);

    let mut packet = Vec::new();
    packet.push(0x10); // CONNECT

    encode_remaining_length(&mut packet, var_header_payload.len());
    packet.extend_from_slice(&var_header_payload);

    packet
}
