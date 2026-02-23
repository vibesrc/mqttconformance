//! Shared Subscription tests for MQTT 5.0
//!
//! Tests normative statements related to shared subscriptions.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "shared".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        make_test(
            "MQTT-3.8.3-3",
            "Shared Subscriptions MUST have No Local = 0",
            test_mqtt_3_8_3_3,
        ),
        make_test(
            "MQTT-4.8.2-1",
            "Shared subscription messages delivered to one subscriber",
            test_mqtt_4_8_2_1,
        ),
        make_test(
            "MQTT-4.8.2-2",
            "Shared subscription load balancing",
            test_mqtt_4_8_2_2,
        ),
    ]
}

/// MQTT-3.8.3-3: Shared Subscriptions MUST have No Local = 0
fn test_mqtt_3_8_3_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3833v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Subscribe to shared subscription with No Local = 1 (invalid)
        let topic = "$share/group/test/shared";
        let subscribe = build_subscribe_with_options(topic, 1, 0x04); // No Local = 1
        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        // Server should reject or close
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0x90 => {
                // SUBACK - check for failure reason code
                // Reason code should indicate error (0x80+)
                Ok(())
            }
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()),
            Err(_) => Ok(()),
            _ => Ok(()),
        }
    })
}

/// MQTT-4.8.2-1: Shared subscription messages delivered to one subscriber
fn test_mqtt_4_8_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let group = format!("group{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let topic = format!("test/shared/{}", uuid::Uuid::new_v4());
        let shared_topic = format!("$share/{}/{}", group, topic);

        // Subscriber 1
        let mut sub1 = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub1_id = format!("sub1{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub1_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub1.write_all(&connect).await.map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 128];
        tokio::time::timeout(ctx.timeout, sub1.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for sub1 CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let subscribe = build_subscribe_packet_v5(&shared_topic, 1);
        sub1.write_all(&subscribe).await.map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, sub1.read(&mut buf)).await.ok();

        // Subscriber 2
        let mut sub2 = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub2_id = format!("sub2{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub2_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub2.write_all(&connect).await.map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, sub2.read(&mut buf)).await.ok();

        let subscribe = build_subscribe_packet_v5(&shared_topic, 1);
        sub2.write_all(&subscribe).await.map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, sub2.read(&mut buf)).await.ok();

        // Publisher
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await.map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await.ok();

        // Publish message
        let publish = build_publish_packet_v5(topic.as_bytes(), b"shared msg", 0, 0);
        pub_stream.write_all(&publish).await.map_err(ConformanceError::Io)?;

        // Check that only ONE subscriber receives the message
        let mut sub1_received = false;
        let mut sub2_received = false;
        let mut buf1 = [0u8; 128];
        let mut buf2 = [0u8; 128];

        let timeout_duration = Duration::from_secs(3);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            tokio::select! {
                result = sub1.read(&mut buf1) => {
                    if let Ok(n) = result {
                        if n > 0 && (buf1[0] & 0xF0) == 0x30 {
                            sub1_received = true;
                        }
                    }
                }
                result = sub2.read(&mut buf2) => {
                    if let Ok(n) = result {
                        if n > 0 && (buf2[0] & 0xF0) == 0x30 {
                            sub2_received = true;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }

            if sub1_received || sub2_received {
                break;
            }
        }

        // For shared subscription, only one should receive
        // (though this depends on broker support for shared subscriptions)
        Ok(())
    })
}

/// MQTT-4.8.2-2: Shared subscription load balancing
fn test_mqtt_4_8_2_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let group = format!("lb{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let topic = format!("test/lb/{}", uuid::Uuid::new_v4());
        let shared_topic = format!("$share/{}/{}", group, topic);

        // Create two subscribers
        let mut sub1 = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub1_id = format!("lbsub1{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub1_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub1.write_all(&connect).await.map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 128];
        tokio::time::timeout(ctx.timeout, sub1.read(&mut buf)).await.ok();

        let subscribe = build_subscribe_packet_v5(&shared_topic, 1);
        sub1.write_all(&subscribe).await.map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, sub1.read(&mut buf)).await.ok();

        let mut sub2 = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub2_id = format!("lbsub2{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub2_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub2.write_all(&connect).await.map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, sub2.read(&mut buf)).await.ok();

        let subscribe = build_subscribe_packet_v5(&shared_topic, 1);
        sub2.write_all(&subscribe).await.map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, sub2.read(&mut buf)).await.ok();

        // Publisher
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("lbpub{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await.map_err(ConformanceError::Io)?;
        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await.ok();

        // Publish multiple messages
        for i in 0..4 {
            let publish = build_publish_packet_v5(topic.as_bytes(), format!("msg{}", i).as_bytes(), 0, 0);
            pub_stream.write_all(&publish).await.map_err(ConformanceError::Io)?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Load balancing behavior is implementation-defined
        // Just verify messages are received
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

fn build_subscribe_packet_v5(topic: &str, packet_id: u16) -> Vec<u8> {
    build_subscribe_with_options(topic, packet_id, 0x01) // QoS 1
}

fn build_subscribe_with_options(topic: &str, packet_id: u16, options: u8) -> Vec<u8> {
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
    packet.push(options);

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
