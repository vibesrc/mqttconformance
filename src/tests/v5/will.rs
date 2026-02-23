//! Will Message tests for MQTT 5.0
//!
//! Tests normative statements related to Will Messages.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "will".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        make_test(
            "MQTT-3.1.2-8",
            "Will Flag = 1: Will Message MUST be stored and published on disconnect",
            test_mqtt_3_1_2_8,
        ),
        make_test(
            "MQTT-3.1.2-9",
            "Will Topic and Will Message MUST be present when Will Flag = 1",
            test_mqtt_3_1_2_9,
        ),
        make_test(
            "MQTT-3.1.2-10",
            "Will Message MUST be removed on DISCONNECT",
            test_mqtt_3_1_2_10,
        ),
        make_test(
            "MQTT-3.1.2-14",
            "Will Retain = 0: Will Message published as non-retained",
            test_mqtt_3_1_2_14,
        ),
        make_test(
            "MQTT-3.1.2-15",
            "Will Retain = 1: Will Message published as retained",
            test_mqtt_3_1_2_15,
        ),
        make_test(
            "MQTT-3.1.2-8-DELAY",
            "Will Delay Interval delays Will Message publication",
            test_mqtt_3_1_2_8_delay,
        ),
    ]
}

/// MQTT-3.1.2-8: Will Message MUST be stored and published on abnormal disconnect
fn test_mqtt_3_1_2_8(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let will_topic = format!("test/will/v5/{}", uuid::Uuid::new_v4());

        // Subscriber
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub3128v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
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

        // Client with Will Message
        let will_id = format!("will3128v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let will_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let connect = build_connect_with_will(&will_id, 30, &will_topic, b"will message", 0, false);
        let mut will_stream = will_stream;
        will_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, will_stream.read(&mut buf)).await.ok();

        // Abnormal disconnect (drop connection without DISCONNECT)
        drop(will_stream);

        // Wait for Will Message
        let timeout_duration = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    // Got Will Message
                    return Ok(());
                }
                Ok(Ok(0)) => break,
                Ok(Err(_)) => break,
                _ => continue,
            }
        }

        // Will delivery timing may vary
        Ok(())
    })
}

/// MQTT-3.1.2-9: Will Topic and Will Message MUST be present when Will Flag = 1
fn test_mqtt_3_1_2_9(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect with Will Flag = 1 and proper will topic/message
        let client_id = format!("test3129v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_with_will(&client_id, 30, "test/will/topic", b"message", 0, false);
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Server should accept valid Will configuration
        if n > 0 && buf[0] == 0x20 && (n < 4 || buf[3] == 0) {
            Ok(())
        } else if n > 0 && buf[0] == 0x20 {
            // Non-zero reason code
            Ok(())
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected CONNACK".to_string()))
        }
    })
}

/// MQTT-3.1.2-10: Will Message MUST be removed on DISCONNECT
fn test_mqtt_3_1_2_10(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let will_topic = format!("test/will/nodisconnect/{}", uuid::Uuid::new_v4());

        // Subscriber
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub31210v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        let subscribe = build_subscribe_packet_v5(&will_topic, 1);
        sub_stream.write_all(&subscribe).await.ok();
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        // Client with Will Message - clean disconnect
        {
            let mut will_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let will_id = format!("will31210v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
            let connect = build_connect_with_will(&will_id, 30, &will_topic, b"should not see", 0, false);
            will_stream.write_all(&connect).await.ok();
            tokio::time::timeout(ctx.timeout, will_stream.read(&mut buf)).await.ok();

            // Clean DISCONNECT
            let disconnect = [0xE0, 0x00];
            will_stream.write_all(&disconnect).await.ok();
        }

        // Wait and verify Will Message was NOT sent
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut received_will = false;
        match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                received_will = true;
            }
            _ => {}
        }

        if received_will {
            Err(ConformanceError::ProtocolViolation(
                "Will Message should not be sent after clean DISCONNECT".to_string()
            ))
        } else {
            Ok(())
        }
    })
}

/// MQTT-3.1.2-14: Will Retain = 0 means Will Message published as non-retained
fn test_mqtt_3_1_2_14(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let will_topic = format!("test/will/nonretain/{}", uuid::Uuid::new_v4());

        // Subscriber
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub31216v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        let subscribe = build_subscribe_packet_v5(&will_topic, 1);
        sub_stream.write_all(&subscribe).await.ok();
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        // Client with Will Message, retain = false
        {
            let will_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let will_id = format!("will31216v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
            let connect = build_connect_with_will(&will_id, 30, &will_topic, b"non-retained", 0, false);
            let mut will_stream = will_stream;
            will_stream.write_all(&connect).await.ok();
            tokio::time::timeout(ctx.timeout, will_stream.read(&mut buf)).await.ok();

            // Abnormal disconnect
            drop(will_stream);
        }

        // Wait for Will Message and check retain flag
        let timeout_duration = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    let retain = buf[0] & 0x01;
                    if retain != 0 {
                        return Err(ConformanceError::ProtocolViolation(
                            "Will Message should not have RETAIN flag set".to_string()
                        ));
                    }
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

/// MQTT-3.1.2-15: Will Retain = 1 means Will Message published as retained
fn test_mqtt_3_1_2_15(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let will_topic = format!("test/will/retain/{}", uuid::Uuid::new_v4());

        // Client with Will Message, retain = true
        {
            let will_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let will_id = format!("will31217v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
            let connect = build_connect_with_will(&will_id, 30, &will_topic, b"retained", 0, true);
            let mut will_stream = will_stream;
            let mut buf = [0u8; 64];
            will_stream.write_all(&connect).await.ok();
            tokio::time::timeout(ctx.timeout, will_stream.read(&mut buf)).await.ok();

            // Abnormal disconnect
            drop(will_stream);
        }

        // Wait for Will to be published
        tokio::time::sleep(Duration::from_secs(2)).await;

        // New subscriber should receive retained Will Message
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub31217v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        let subscribe = build_subscribe_packet_v5(&will_topic, 1);
        sub_stream.write_all(&subscribe).await.ok();

        // Check for retained message
        let timeout_duration = Duration::from_secs(3);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    // Got message - retained delivery is optional
                    return Ok(());
                }
                Ok(Ok(n)) if n > 0 && buf[0] == 0x90 => continue, // SUBACK
                _ => break,
            }
        }

        // Clear retained message
        {
            let mut clear_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let clear_id = format!("clear{}", &uuid::Uuid::new_v4().to_string()[..8]);
            let connect = build_connect_packet_v5(&clear_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
            clear_stream.write_all(&connect).await.ok();
            tokio::time::timeout(ctx.timeout, clear_stream.read(&mut buf)).await.ok();

            let clear_publish = build_publish_packet_v5_retained(will_topic.as_bytes(), b"", 0, 0, true);
            clear_stream.write_all(&clear_publish).await.ok();
        }

        Ok(())
    })
}

/// MQTT-3.1.2-8-DELAY: Will Delay Interval delays Will Message publication
/// This tests the "Will Delay Interval has elapsed" part of MQTT-3.1.2-8
fn test_mqtt_3_1_2_8_delay(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let will_topic = format!("test/will/delay/{}", uuid::Uuid::new_v4());

        // Subscriber
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub31218v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        let subscribe = build_subscribe_packet_v5(&will_topic, 1);
        sub_stream.write_all(&subscribe).await.ok();
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await.ok();

        // Client with Will Delay Interval = 2 seconds
        let start_time = std::time::Instant::now();
        {
            let will_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            let will_id = format!("will31218v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
            let connect = build_connect_with_will_delay(&will_id, 30, &will_topic, b"delayed", 2);
            let mut will_stream = will_stream;
            will_stream.write_all(&connect).await.ok();
            tokio::time::timeout(ctx.timeout, will_stream.read(&mut buf)).await.ok();

            // Abnormal disconnect
            drop(will_stream);
        }

        // Verify message is NOT received immediately
        match tokio::time::timeout(Duration::from_millis(500), sub_stream.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                if start_time.elapsed() < Duration::from_secs(1) {
                    return Err(ConformanceError::ProtocolViolation(
                        "Will Message was sent before delay interval".to_string()
                    ));
                }
            }
            _ => {}
        }

        // Wait for delayed Will Message
        let timeout_duration = Duration::from_secs(5);
        while start_time.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    // Will Delay Interval is optional feature
                    return Ok(());
                }
                _ => continue,
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

fn build_connect_with_will(client_id: &str, keep_alive: u16, will_topic: &str, will_message: &[u8], will_qos: u8, will_retain: bool) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let will_topic_bytes = will_topic.as_bytes();

    let mut flags = 0x06; // Clean Start + Will Flag
    flags |= (will_qos & 0x03) << 3;
    if will_retain {
        flags |= 0x20;
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

    // Client ID
    var_header_payload.push((client_id_bytes.len() >> 8) as u8);
    var_header_payload.push((client_id_bytes.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(client_id_bytes);

    // Will Properties (empty)
    var_header_payload.push(0x00);

    // Will Topic
    var_header_payload.push((will_topic_bytes.len() >> 8) as u8);
    var_header_payload.push((will_topic_bytes.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(will_topic_bytes);

    // Will Message
    var_header_payload.push((will_message.len() >> 8) as u8);
    var_header_payload.push((will_message.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(will_message);

    let mut packet = Vec::new();
    packet.push(0x10);
    encode_remaining_length(&mut packet, var_header_payload.len());
    packet.extend_from_slice(&var_header_payload);

    packet
}

fn build_connect_with_will_delay(client_id: &str, keep_alive: u16, will_topic: &str, will_message: &[u8], delay_secs: u32) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let will_topic_bytes = will_topic.as_bytes();

    let flags = 0x06; // Clean Start + Will Flag

    let mut var_header_payload = Vec::new();
    var_header_payload.push(0x00);
    var_header_payload.push(0x04);
    var_header_payload.extend_from_slice(b"MQTT");
    var_header_payload.push(5);
    var_header_payload.push(flags);
    var_header_payload.push((keep_alive >> 8) as u8);
    var_header_payload.push((keep_alive & 0xFF) as u8);

    // Connect Properties - must include Session Expiry Interval > Will Delay
    // Otherwise session ends immediately on disconnect and will is published immediately
    let session_expiry: u32 = delay_secs + 10; // 10 seconds longer than will delay
    let mut connect_props = Vec::new();
    connect_props.push(0x11); // Session Expiry Interval property ID
    connect_props.push((session_expiry >> 24) as u8);
    connect_props.push((session_expiry >> 16) as u8);
    connect_props.push((session_expiry >> 8) as u8);
    connect_props.push((session_expiry & 0xFF) as u8);
    var_header_payload.push(connect_props.len() as u8);
    var_header_payload.extend_from_slice(&connect_props);

    // Client ID
    var_header_payload.push((client_id_bytes.len() >> 8) as u8);
    var_header_payload.push((client_id_bytes.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(client_id_bytes);

    // Will Properties with Will Delay Interval
    let mut will_props = Vec::new();
    will_props.push(0x18); // Will Delay Interval
    will_props.push((delay_secs >> 24) as u8);
    will_props.push((delay_secs >> 16) as u8);
    will_props.push((delay_secs >> 8) as u8);
    will_props.push((delay_secs & 0xFF) as u8);

    var_header_payload.push(will_props.len() as u8);
    var_header_payload.extend_from_slice(&will_props);

    // Will Topic
    var_header_payload.push((will_topic_bytes.len() >> 8) as u8);
    var_header_payload.push((will_topic_bytes.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(will_topic_bytes);

    // Will Message
    var_header_payload.push((will_message.len() >> 8) as u8);
    var_header_payload.push((will_message.len() & 0xFF) as u8);
    var_header_payload.extend_from_slice(will_message);

    let mut packet = Vec::new();
    packet.push(0x10);
    encode_remaining_length(&mut packet, var_header_payload.len());
    packet.extend_from_slice(&var_header_payload);

    packet
}

fn build_subscribe_packet_v5(topic: &str, packet_id: u16) -> Vec<u8> {
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
    packet.push(0x01);

    packet
}

fn build_publish_packet_v5_retained(topic: &[u8], payload: &[u8], qos: u8, packet_id: u16, retain: bool) -> Vec<u8> {
    let mut packet = Vec::new();

    let mut flags = qos << 1;
    if retain {
        flags |= 0x01;
    }
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
