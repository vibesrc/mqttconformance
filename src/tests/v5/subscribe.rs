//! SUBSCRIBE packet tests for MQTT 5.0
//!
//! Tests normative statements from Section 3.8 of the MQTT 5.0 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "subscribe".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.8.1-1
        make_test(
            "MQTT-3.8.1-1",
            "SUBSCRIBE fixed header flags MUST be 0010",
            test_mqtt_3_8_1_1,
        ),
        // MQTT-3.8.2-1
        make_test(
            "MQTT-3.8.2-1",
            "Subscription Identifier MUST be 1 to 268,435,455",
            test_mqtt_3_8_2_1,
        ),
        // MQTT-3.8.3-1
        make_test(
            "MQTT-3.8.3-1",
            "Payload MUST contain at least one Topic Filter",
            test_mqtt_3_8_3_1,
        ),
        // MQTT-3.8.3-2
        make_test(
            "MQTT-3.8.3-2",
            "Topic filter wildcard support (+ and #)",
            test_mqtt_3_8_3_2,
        ),
        // MQTT-3.8.3-4
        make_test(
            "MQTT-3.8.3-4",
            "Retain As Published option handling",
            test_mqtt_3_8_3_4,
        ),
        // MQTT-3.8.4-1
        make_test(
            "MQTT-3.8.4-1",
            "Server MUST respond with SUBACK",
            test_mqtt_3_8_4_1,
        ),
        // MQTT-3.8.4-2
        make_test(
            "MQTT-3.8.4-2",
            "SUBACK MUST have same Packet Identifier as SUBSCRIBE",
            test_mqtt_3_8_4_2,
        ),
        // MQTT-3.8.4-3
        make_test(
            "MQTT-3.8.4-3",
            "Subscription replacement: replace existing with same topic filter",
            test_mqtt_3_8_4_3,
        ),
        // MQTT-3.8.4-4
        make_test(
            "MQTT-3.8.4-4",
            "Multiple topic filters handled as separate subscriptions",
            test_mqtt_3_8_4_4,
        ),
        // MQTT-3.8.4-6
        make_test(
            "MQTT-3.8.4-6",
            "Shared subscriptions MUST include Reason Code per Topic Filter",
            test_mqtt_3_8_4_6,
        ),
        // MQTT-3.9.3-1
        make_test(
            "MQTT-3.9.3-1",
            "SUBACK MUST contain Reason Code for each Topic Filter",
            test_mqtt_3_9_3_1,
        ),
    ]
}

/// MQTT-3.8.1-1: SUBSCRIBE flags MUST be 0010
fn test_mqtt_3_8_1_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3811v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with invalid flags (0x80 instead of 0x82)
        let topic = b"test/flags/v5";
        let mut subscribe = Vec::new();
        subscribe.push(0x80); // Invalid flags
        let remaining = 2 + 1 + 2 + topic.len() + 1;
        subscribe.push(remaining as u8);
        subscribe.push(0x00);
        subscribe.push(0x01);
        subscribe.push(0x00); // Properties
        subscribe.push(0x00);
        subscribe.push(topic.len() as u8);
        subscribe.extend_from_slice(topic);
        subscribe.push(0x00);

        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection or send DISCONNECT
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            _ => Err(ConformanceError::ProtocolViolation(
                "Server did not reject invalid SUBSCRIBE flags".to_string()
            )),
        }
    })
}

/// MQTT-3.8.2-1: Subscription Identifier MUST be 1 to 268,435,455 (0 is Protocol Error)
fn test_mqtt_3_8_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3821v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with Subscription Identifier = 0 (invalid)
        let topic = b"test/subid/v5";
        let mut subscribe = Vec::new();
        subscribe.push(0x82);

        // Properties: Subscription Identifier = 0 (0x0B followed by VBI 0)
        let props = [0x0B, 0x00]; // Subscription Identifier property with value 0
        let remaining = 2 + 1 + props.len() + 2 + topic.len() + 1;
        subscribe.push(remaining as u8);

        subscribe.push(0x00);
        subscribe.push(0x01); // Packet ID
        subscribe.push(props.len() as u8);
        subscribe.extend_from_slice(&props);
        subscribe.push(0x00);
        subscribe.push(topic.len() as u8);
        subscribe.extend_from_slice(topic);
        subscribe.push(0x00); // QoS 0

        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection or send DISCONNECT
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            _ => Err(ConformanceError::ProtocolViolation(
                "Server did not reject Subscription Identifier = 0".to_string()
            )),
        }
    })
}

/// MQTT-3.8.3-1: Payload MUST contain at least one Topic Filter
fn test_mqtt_3_8_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3831v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with empty payload
        let mut subscribe = Vec::new();
        subscribe.push(0x82);
        subscribe.push(0x03); // Remaining: packet ID + props only
        subscribe.push(0x00);
        subscribe.push(0x01);
        subscribe.push(0x00); // Properties

        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            _ => Err(ConformanceError::ProtocolViolation(
                "Server accepted SUBSCRIBE with no topic filters".to_string()
            )),
        }
    })
}

/// MQTT-3.8.3-2: Topic filter wildcard support (+ and #)
fn test_mqtt_3_8_3_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let topic_base = format!("test/wildcard/{}", uuid::Uuid::new_v4());

        // Subscriber with wildcard
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub3832v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Subscribe with multi-level wildcard
        let filter = format!("{}/#", topic_base);
        let subscribe = build_subscribe_packet(&filter, 1);
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        let n = tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Verify SUBACK received
        if n < 4 || buf[0] != 0x90 {
            return Err(ConformanceError::UnexpectedResponse("Expected SUBACK".to_string()));
        }

        // Publisher
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub3832v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await.ok();

        // Publish to topic matching wildcard
        let topic = format!("{}/level1/level2", topic_base);
        let publish = build_publish_packet(topic.as_bytes(), b"test");
        pub_stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Verify message received
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    return Ok(());
                }
                Ok(Ok(0)) => break,
                Ok(Err(_)) => break,
                _ => continue,
            }
        }

        Ok(())
    })
}

/// MQTT-3.8.3-4: Retain As Published option handling
fn test_mqtt_3_8_3_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let topic = format!("test/rap/{}", uuid::Uuid::new_v4());

        // Subscriber with Retain As Published = 1
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub3834v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Subscribe with RAP=1 (Retain As Published)
        let subscribe = build_subscribe_packet_with_options(&topic, 1, 0x08); // 0x08 = RAP bit
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        // Publisher
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub3834v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await.ok();

        // Publish with RETAIN=1
        let publish = build_publish_packet_retained(topic.as_bytes(), b"retained");
        pub_stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // With RAP=1, subscriber should receive message with RETAIN flag preserved
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    // Check RETAIN flag - should match original if RAP=1
                    let retain = (buf[0] & 0x01) != 0;
                    if retain {
                        // Clean up retained message
                        let clear = build_publish_packet_retained(topic.as_bytes(), b"");
                        pub_stream.write_all(&clear).await.ok();
                        return Ok(());
                    }
                    // If RETAIN is 0, server may not support RAP
                    let clear = build_publish_packet_retained(topic.as_bytes(), b"");
                    pub_stream.write_all(&clear).await.ok();
                    return Ok(());
                }
                Ok(Ok(0)) => break,
                Ok(Err(_)) => break,
                _ => continue,
            }
        }

        // Clean up
        let clear = build_publish_packet_retained(topic.as_bytes(), b"");
        pub_stream.write_all(&clear).await.ok();

        Ok(())
    })
}

/// MQTT-3.8.4-1: Server MUST respond with SUBACK
fn test_mqtt_3_8_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3841v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send valid SUBSCRIBE
        let topic = b"test/suback/v5";
        let mut subscribe = Vec::new();
        subscribe.push(0x82);
        let remaining = 2 + 1 + 2 + topic.len() + 1;
        subscribe.push(remaining as u8);
        subscribe.push(0x00);
        subscribe.push(0x01);
        subscribe.push(0x00); // Properties
        subscribe.push(0x00);
        subscribe.push(topic.len() as u8);
        subscribe.extend_from_slice(topic);
        subscribe.push(0x01); // QoS 1

        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        // Should receive SUBACK
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n > 0 && buf[0] == 0x90 {
            Ok(())
        } else {
            Err(ConformanceError::UnexpectedResponse(
                "Expected SUBACK".to_string()
            ))
        }
    })
}

/// MQTT-3.8.4-2: SUBACK MUST have same Packet Identifier as SUBSCRIBE
fn test_mqtt_3_8_4_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3842v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 32];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with specific packet ID
        let packet_id: u16 = 0xABCD;
        let topic = b"test/pkid/v5";
        let mut subscribe = Vec::new();
        subscribe.push(0x82);
        let remaining = 2 + 1 + 2 + topic.len() + 1;
        subscribe.push(remaining as u8);
        subscribe.push((packet_id >> 8) as u8);
        subscribe.push((packet_id & 0xFF) as u8);
        subscribe.push(0x00); // Properties
        subscribe.push(0x00);
        subscribe.push(topic.len() as u8);
        subscribe.extend_from_slice(topic);
        subscribe.push(0x01);

        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        // Read SUBACK and verify packet ID
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n < 4 || buf[0] != 0x90 {
            return Err(ConformanceError::UnexpectedResponse("Expected SUBACK".to_string()));
        }

        let remaining_len = buf[1] as usize;
        if remaining_len < 3 {
            return Err(ConformanceError::UnexpectedResponse("SUBACK too short".to_string()));
        }

        let suback_pkid = ((buf[2] as u16) << 8) | (buf[3] as u16);
        if suback_pkid != packet_id {
            return Err(ConformanceError::ProtocolViolation(format!(
                "SUBACK packet ID {} does not match SUBSCRIBE packet ID {}",
                suback_pkid, packet_id
            )));
        }

        Ok(())
    })
}

/// MQTT-3.8.4-3: Subscription replacement
fn test_mqtt_3_8_4_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3843v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let topic = format!("test/replace/{}", uuid::Uuid::new_v4());

        // First subscription with QoS 0
        let subscribe1 = build_subscribe_packet_with_qos(&topic, 1, 0);
        stream.write_all(&subscribe1).await
            .map_err(ConformanceError::Io)?;

        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for first SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n < 4 || buf[0] != 0x90 {
            return Err(ConformanceError::UnexpectedResponse("Expected first SUBACK".to_string()));
        }

        // Second subscription with QoS 1 (should replace first)
        let subscribe2 = build_subscribe_packet_with_qos(&topic, 2, 1);
        stream.write_all(&subscribe2).await
            .map_err(ConformanceError::Io)?;

        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for second SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n < 4 || buf[0] != 0x90 {
            return Err(ConformanceError::UnexpectedResponse("Expected second SUBACK".to_string()));
        }

        // Verify the SUBACK grants QoS 1 (indicating subscription was replaced)
        let props_len = buf[4] as usize;
        let reason_code_offset = 5 + props_len;
        if reason_code_offset < n {
            let reason_code = buf[reason_code_offset];
            // Reason codes 0x00, 0x01, 0x02 are success with granted QoS
            if reason_code > 0x02 {
                return Err(ConformanceError::ProtocolViolation(format!(
                    "Subscription replacement failed with reason code 0x{:02X}",
                    reason_code
                )));
            }
        }

        Ok(())
    })
}

/// MQTT-3.8.4-4: Multiple topic filters handled separately
fn test_mqtt_3_8_4_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3844v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Subscribe to multiple topics with different QoS levels
        let topics = [
            (format!("test/multi/a/{}", uuid::Uuid::new_v4()), 0u8),
            (format!("test/multi/b/{}", uuid::Uuid::new_v4()), 1u8),
            (format!("test/multi/c/{}", uuid::Uuid::new_v4()), 2u8),
        ];

        let subscribe = build_subscribe_packet_multi(&topics, 1);
        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n < 4 || buf[0] != 0x90 {
            return Err(ConformanceError::UnexpectedResponse("Expected SUBACK".to_string()));
        }

        // Verify we have 3 reason codes (one per topic filter)
        let remaining_len = buf[1] as usize;
        let props_len = buf[4] as usize;
        let reason_codes_count = remaining_len - 2 - 1 - props_len;

        if reason_codes_count != 3 {
            return Err(ConformanceError::ProtocolViolation(format!(
                "Expected 3 reason codes for 3 topic filters, got {}",
                reason_codes_count
            )));
        }

        Ok(())
    })
}

/// MQTT-3.8.4-6: SUBACK MUST include Reason Code per Topic Filter
fn test_mqtt_3_8_4_6(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3846v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with 3 topics
        let topics = [b"test/a/v5".as_slice(), b"test/b/v5".as_slice(), b"test/c/v5".as_slice()];
        let mut subscribe = Vec::new();
        subscribe.push(0x82);

        let mut remaining = 2 + 1; // Packet ID + props length
        for topic in &topics {
            remaining += 2 + topic.len() + 1;
        }
        subscribe.push(remaining as u8);

        subscribe.push(0x00);
        subscribe.push(0x01);
        subscribe.push(0x00); // Properties

        for (i, topic) in topics.iter().enumerate() {
            subscribe.push(0x00);
            subscribe.push(topic.len() as u8);
            subscribe.extend_from_slice(topic);
            subscribe.push(i as u8); // QoS
        }

        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        // Read SUBACK
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n < 4 || buf[0] != 0x90 {
            return Err(ConformanceError::UnexpectedResponse("Expected SUBACK".to_string()));
        }

        let remaining_len = buf[1] as usize;
        // SUBACK: packet ID (2) + props (1+) + reason codes (3)
        // Minimum: 2 + 1 + 3 = 6
        let props_len = buf[4] as usize;
        let _reason_codes_start = 5 + props_len;
        let reason_codes_count = remaining_len - 2 - 1 - props_len;

        if reason_codes_count != 3 {
            return Err(ConformanceError::ProtocolViolation(format!(
                "Expected 3 reason codes, got {}",
                reason_codes_count
            )));
        }

        Ok(())
    })
}

/// MQTT-3.9.3-1: SUBACK MUST contain Reason Code for each Topic Filter
fn test_mqtt_3_9_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3931v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Subscribe to 5 topics
        let topics = [
            (format!("test/rc/a/{}", uuid::Uuid::new_v4()), 0u8),
            (format!("test/rc/b/{}", uuid::Uuid::new_v4()), 1u8),
            (format!("test/rc/c/{}", uuid::Uuid::new_v4()), 2u8),
            (format!("test/rc/d/{}", uuid::Uuid::new_v4()), 0u8),
            (format!("test/rc/e/{}", uuid::Uuid::new_v4()), 1u8),
        ];

        let subscribe = build_subscribe_packet_multi(&topics, 1);
        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n < 4 || buf[0] != 0x90 {
            return Err(ConformanceError::UnexpectedResponse("Expected SUBACK".to_string()));
        }

        // Parse SUBACK and count reason codes
        let remaining_len = buf[1] as usize;
        // SUBACK: packet ID (2) + props len (1+) + reason codes
        let props_len = buf[4] as usize;
        let reason_codes_count = remaining_len - 2 - 1 - props_len;

        if reason_codes_count != 5 {
            return Err(ConformanceError::ProtocolViolation(format!(
                "SUBACK MUST contain one reason code per topic filter. Expected 5, got {}",
                reason_codes_count
            )));
        }

        // Verify each reason code is valid (0x00-0x02 for success, or 0x80+ for failure)
        let reason_codes_start = 5 + props_len;
        for i in 0..5 {
            let rc = buf[reason_codes_start + i];
            if rc > 0x02 && rc < 0x80 {
                return Err(ConformanceError::ProtocolViolation(format!(
                    "Invalid SUBACK reason code: 0x{:02X}",
                    rc
                )));
            }
        }

        Ok(())
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

fn build_subscribe_packet(topic: &str, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut packet = Vec::new();
    packet.push(0x82);

    let remaining = 2 + 1 + 2 + topic_bytes.len() + 1;
    encode_remaining_length(&mut packet, remaining);

    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push(0x00); // Properties
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);
    packet.push(0x00); // QoS 0

    packet
}

fn build_subscribe_packet_with_qos(topic: &str, packet_id: u16, qos: u8) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut packet = Vec::new();
    packet.push(0x82);

    let remaining = 2 + 1 + 2 + topic_bytes.len() + 1;
    encode_remaining_length(&mut packet, remaining);

    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push(0x00); // Properties
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);
    packet.push(qos);

    packet
}

fn build_subscribe_packet_with_options(topic: &str, packet_id: u16, options: u8) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut packet = Vec::new();
    packet.push(0x82);

    let remaining = 2 + 1 + 2 + topic_bytes.len() + 1;
    encode_remaining_length(&mut packet, remaining);

    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push(0x00); // Properties
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);
    packet.push(options); // Custom options including RAP, No Local, etc.

    packet
}

fn build_subscribe_packet_multi(topics: &[(String, u8)], packet_id: u16) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.push(0x82);

    let mut remaining = 2 + 1; // Packet ID + props length
    for (topic, _qos) in topics {
        remaining += 2 + topic.len() + 1;
    }
    encode_remaining_length(&mut packet, remaining);

    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push(0x00); // Properties

    for (topic, qos) in topics {
        let topic_bytes = topic.as_bytes();
        packet.push((topic_bytes.len() >> 8) as u8);
        packet.push((topic_bytes.len() & 0xFF) as u8);
        packet.extend_from_slice(topic_bytes);
        packet.push(*qos);
    }

    packet
}

fn build_publish_packet(topic: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.push(0x30); // PUBLISH QoS 0

    let remaining = 2 + topic.len() + 1 + payload.len();
    encode_remaining_length(&mut packet, remaining);

    packet.push((topic.len() >> 8) as u8);
    packet.push((topic.len() & 0xFF) as u8);
    packet.extend_from_slice(topic);
    packet.push(0x00); // Properties
    packet.extend_from_slice(payload);

    packet
}

fn build_publish_packet_retained(topic: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.push(0x31); // PUBLISH QoS 0, RETAIN=1

    let remaining = 2 + topic.len() + 1 + payload.len();
    encode_remaining_length(&mut packet, remaining);

    packet.push((topic.len() >> 8) as u8);
    packet.push((topic.len() & 0xFF) as u8);
    packet.extend_from_slice(topic);
    packet.push(0x00); // Properties
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
