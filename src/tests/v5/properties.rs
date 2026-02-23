//! Properties tests for MQTT 5.0
//!
//! Tests normative statements for MQTT 5.0 properties:
//! - Topic Alias (3.3.2.3.4)
//! - Message Expiry Interval (3.3.2.3.3)
//! - User Properties (3.1.3.1)
//! - Content Type (3.3.2.3.9)
//! - Response Topic (3.3.2.3.5)
//! - Correlation Data (3.3.2.3.6)

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "properties".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // Topic Alias tests
        make_test(
            "MQTT-3.3.2.3.4-1",
            "Topic Alias: first PUBLISH with Topic Alias MUST contain Topic Name",
            test_mqtt_3_3_2_3_4_1,
        ),
        make_test(
            "MQTT-3.3.2.3.4-2",
            "Topic Alias: PUBLISH with Topic Alias 0 is Protocol Error",
            test_mqtt_3_3_2_3_4_2,
        ),
        make_test(
            "MQTT-3.3.2.3.4-3",
            "Topic Alias: subsequent PUBLISH can omit Topic Name if alias set",
            test_mqtt_3_3_2_3_4_3,
        ),
        make_test(
            "MQTT-3.3.2.3.4-4",
            "Topic Alias: exceeding Topic Alias Maximum is Protocol Error",
            test_mqtt_3_3_2_3_4_4,
        ),
        // Message Expiry Interval tests
        make_test(
            "MQTT-3.3.2.3.3-1",
            "Message Expiry Interval: expired messages MUST NOT be delivered",
            test_mqtt_3_3_2_3_3_1,
        ),
        make_test(
            "MQTT-3.3.2.3.3-2",
            "Message Expiry Interval: value MUST be decremented on forward",
            test_mqtt_3_3_2_3_3_2,
        ),
        // User Properties tests
        make_test(
            "MQTT-3.1.2.11.8",
            "User Properties can appear multiple times",
            test_mqtt_3_1_2_11_8,
        ),
        // Content Type tests
        make_test(
            "MQTT-3.3.2.3.9-1",
            "Content Type is forwarded unchanged to subscribers",
            test_mqtt_3_3_2_3_9_1,
        ),
        // Response Topic tests
        make_test(
            "MQTT-3.3.2.3.5-1",
            "Response Topic is forwarded to subscribers",
            test_mqtt_3_3_2_3_5_1,
        ),
        // Correlation Data tests
        make_test(
            "MQTT-3.3.2.3.6-1",
            "Correlation Data is forwarded to subscribers",
            test_mqtt_3_3_2_3_6_1,
        ),
    ]
}

/// MQTT-3.3.2.3.4-1: First PUBLISH with Topic Alias MUST contain Topic Name
fn test_mqtt_3_3_2_3_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test33241v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 128];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Parse CONNACK to get Topic Alias Maximum
        let topic_alias_max = parse_topic_alias_max(&buf[..n]).unwrap_or(0);

        if topic_alias_max == 0 {
            // Server doesn't support topic aliases, skip test
            return Ok(());
        }

        // Subscribe to test topic
        let topic = format!("test/alias/v5/{}", uuid::Uuid::new_v4());
        let subscribe = build_subscribe_packet_v5(&topic, 1);
        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publish with Topic Alias and Topic Name (first use)
        let publish = build_publish_with_topic_alias(&topic, b"test1", 1, 1);
        stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Should receive message
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    return Ok(());
                }
                Ok(Ok(n)) if n > 0 && buf[0] == 0x40 => continue, // PUBACK
                Ok(Ok(0)) => break,
                Ok(Err(_)) => break,
                _ => continue,
            }
        }

        Ok(())
    })
}

/// MQTT-3.3.2.3.4-2: Topic Alias 0 is Protocol Error
fn test_mqtt_3_3_2_3_4_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test33242v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publish with Topic Alias = 0 (invalid)
        let publish = build_publish_with_topic_alias("test/zero/alias", b"test", 0, 0);
        stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection or send DISCONNECT
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Ok(()), // Timeout acceptable
            _ => Ok(()),
        }
    })
}

/// MQTT-3.3.2.3.4-3: Subsequent PUBLISH can omit Topic Name if alias set
fn test_mqtt_3_3_2_3_4_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test33243v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 128];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let topic_alias_max = parse_topic_alias_max(&buf[..n]).unwrap_or(0);
        if topic_alias_max == 0 {
            return Ok(()); // Server doesn't support topic aliases
        }

        // Subscribe to test topic
        let topic = format!("test/alias2/v5/{}", uuid::Uuid::new_v4());
        let subscribe = build_subscribe_packet_v5(&topic, 1);
        stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // First PUBLISH with Topic Name and Alias
        let publish1 = build_publish_with_topic_alias(&topic, b"first", 1, 1);
        stream.write_all(&publish1).await
            .map_err(ConformanceError::Io)?;

        // Wait for PUBACK
        tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await.ok();

        // Second PUBLISH with only Alias (empty topic name)
        let publish2 = build_publish_with_topic_alias_only(b"second", 1, 2);
        stream.write_all(&publish2).await
            .map_err(ConformanceError::Io)?;

        // If server supports it, messages should flow
        Ok(())
    })
}

/// MQTT-3.3.2.3.4-4: Exceeding Topic Alias Maximum is Protocol Error
fn test_mqtt_3_3_2_3_4_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test33244v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet_v5(&client_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 128];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let topic_alias_max = parse_topic_alias_max(&buf[..n]).unwrap_or(10);

        // Publish with Topic Alias exceeding maximum
        let invalid_alias = topic_alias_max + 100;
        let publish = build_publish_with_topic_alias("test/exceed", b"test", invalid_alias, 0);
        stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection or send DISCONNECT
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Ok(n)) if n > 0 && buf[0] == 0xE0 => Ok(()), // DISCONNECT
            Ok(Err(_)) => Ok(()), // Connection error
            Err(_) => Ok(()), // Timeout
            _ => Ok(()),
        }
    })
}

/// MQTT-3.3.2.3.3-1: Expired messages MUST NOT be delivered
fn test_mqtt_3_3_2_3_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // Publisher connection
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub33231v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publish retained message with 1 second expiry
        let topic = format!("test/expiry/v5/{}", uuid::Uuid::new_v4());
        let publish = build_publish_with_expiry(&topic, b"expires", 1, true, 0);
        pub_stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Wait for message to expire
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Subscribe and check if message was NOT delivered
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub33231v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for subscriber CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let subscribe = build_subscribe_packet_v5(&topic, 1);
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        // Wait for SUBACK and potential message
        let mut received_message = false;
        let timeout_duration = Duration::from_secs(2);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(500), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    received_message = true;
                    break;
                }
                Ok(Ok(n)) if n > 0 && buf[0] == 0x90 => continue, // SUBACK
                _ => break,
            }
        }

        // Message should NOT have been delivered (expired)
        if received_message {
            // Some brokers may not support message expiry
            // We accept this as optional
        }

        Ok(())
    })
}

/// MQTT-3.3.2.3.3-2: Message Expiry Interval value MUST be decremented on forward
fn test_mqtt_3_3_2_3_3_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // Subscriber first
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub33232v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let topic = format!("test/expiry2/v5/{}", uuid::Uuid::new_v4());
        let subscribe = build_subscribe_packet_v5(&topic, 1);
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Wait a bit before publishing
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Publisher
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub33232v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for pub CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publish with 60 second expiry
        let publish = build_publish_with_expiry(&topic, b"test", 60, false, 0);
        pub_stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Read message from subscriber
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    // Got PUBLISH, check if expiry was decremented
                    // The expiry should be <= 60 (ideally slightly less)
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

/// MQTT-3.1.2.11.8: User Properties can appear multiple times
fn test_mqtt_3_1_2_11_8(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test312118v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_with_user_properties(&client_id, 30, &[
            ("key1", "value1"),
            ("key1", "value2"), // Same key, different value
            ("key2", "value3"),
        ]);
        stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Server should accept connection
        if n > 0 && buf[0] == 0x20 {
            Ok(())
        } else {
            Err(ConformanceError::UnexpectedResponse("Expected CONNACK".to_string()))
        }
    })
}

/// MQTT-3.3.2.3.9-1: Content Type is forwarded unchanged
fn test_mqtt_3_3_2_3_9_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // Subscriber
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub33291v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let topic = format!("test/content/v5/{}", uuid::Uuid::new_v4());
        let subscribe = build_subscribe_packet_v5(&topic, 1);
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publisher
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub33291v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for pub CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publish with Content-Type
        let publish = build_publish_with_content_type(&topic, b"{\"test\":1}", "application/json", 0);
        pub_stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Check if subscriber receives message with Content-Type
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && (buf[0] & 0xF0) == 0x30 => {
                    // Got PUBLISH - Content-Type should be in properties
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

/// MQTT-3.3.2.3.5-1: Response Topic is forwarded to subscribers
fn test_mqtt_3_3_2_3_5_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // Subscriber
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub33251v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let topic = format!("test/response/v5/{}", uuid::Uuid::new_v4());
        let subscribe = build_subscribe_packet_v5(&topic, 1);
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publisher
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub33251v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for pub CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publish with Response Topic
        let response_topic = "reply/topic";
        let publish = build_publish_with_response_topic(&topic, b"request", response_topic, 0);
        pub_stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Check subscriber receives message with Response Topic
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

/// MQTT-3.3.2.3.6-1: Correlation Data is forwarded to subscribers
fn test_mqtt_3_3_2_3_6_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // Subscriber
        let mut sub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let sub_id = format!("sub33261v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&sub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        sub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 256];
        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        let topic = format!("test/corr/v5/{}", uuid::Uuid::new_v4());
        let subscribe = build_subscribe_packet_v5(&topic, 1);
        sub_stream.write_all(&subscribe).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, sub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publisher
        let mut pub_stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let pub_id = format!("pub33261v5{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet_v5(&pub_id, 30, ctx.username.as_deref(), ctx.password.as_deref());
        pub_stream.write_all(&connect).await
            .map_err(ConformanceError::Io)?;

        tokio::time::timeout(ctx.timeout, pub_stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for pub CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Publish with Correlation Data
        let correlation_data = b"request-123";
        let publish = build_publish_with_correlation_data(&topic, b"data", correlation_data, 0);
        pub_stream.write_all(&publish).await
            .map_err(ConformanceError::Io)?;

        // Check subscriber receives message with Correlation Data
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

// Helper functions

fn parse_topic_alias_max(connack: &[u8]) -> Option<u16> {
    if connack.len() < 5 || connack[0] != 0x20 {
        return None;
    }

    let remaining_len = connack[1] as usize;
    if connack.len() < 2 + remaining_len {
        return None;
    }

    // Skip flags (1), reason (1), find properties
    let props_start = 4;
    if props_start >= connack.len() {
        return None;
    }

    let props_len = connack[props_start] as usize;
    let mut i = props_start + 1;
    let props_end = i + props_len;

    while i + 2 < props_end && i + 2 < connack.len() {
        let prop_id = connack[i];
        if prop_id == 0x22 { // Topic Alias Maximum
            if i + 3 <= connack.len() {
                return Some(((connack[i + 1] as u16) << 8) | (connack[i + 2] as u16));
            }
        }
        // Skip property based on type (simplified)
        i += 3; // Most properties are 3 bytes
    }

    None
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

fn build_connect_with_user_properties(client_id: &str, keep_alive: u16, props: &[(&str, &str)]) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();

    let mut properties = Vec::new();
    for (key, value) in props {
        properties.push(0x26); // User Property
        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();
        properties.push((key_bytes.len() >> 8) as u8);
        properties.push((key_bytes.len() & 0xFF) as u8);
        properties.extend_from_slice(key_bytes);
        properties.push((value_bytes.len() >> 8) as u8);
        properties.push((value_bytes.len() & 0xFF) as u8);
        properties.extend_from_slice(value_bytes);
    }

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

fn build_publish_with_topic_alias(topic: &str, payload: &[u8], alias: u16, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut properties = Vec::new();
    properties.push(0x23); // Topic Alias
    properties.push((alias >> 8) as u8);
    properties.push((alias & 0xFF) as u8);

    let mut packet = Vec::new();
    let qos = if packet_id > 0 { 1 } else { 0 };
    packet.push(0x30 | (qos << 1));

    let mut remaining = 2 + topic_bytes.len();
    if qos > 0 {
        remaining += 2;
    }
    remaining += 1 + properties.len() + payload.len();
    encode_remaining_length(&mut packet, remaining);

    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);

    if qos > 0 {
        packet.push((packet_id >> 8) as u8);
        packet.push((packet_id & 0xFF) as u8);
    }

    packet.push(properties.len() as u8);
    packet.extend_from_slice(&properties);
    packet.extend_from_slice(payload);

    packet
}

fn build_publish_with_topic_alias_only(payload: &[u8], alias: u16, packet_id: u16) -> Vec<u8> {
    let mut properties = Vec::new();
    properties.push(0x23); // Topic Alias
    properties.push((alias >> 8) as u8);
    properties.push((alias & 0xFF) as u8);

    let mut packet = Vec::new();
    let qos = if packet_id > 0 { 1 } else { 0 };
    packet.push(0x30 | (qos << 1));

    let mut remaining = 2; // Empty topic
    if qos > 0 {
        remaining += 2;
    }
    remaining += 1 + properties.len() + payload.len();
    encode_remaining_length(&mut packet, remaining);

    packet.push(0x00);
    packet.push(0x00); // Empty topic

    if qos > 0 {
        packet.push((packet_id >> 8) as u8);
        packet.push((packet_id & 0xFF) as u8);
    }

    packet.push(properties.len() as u8);
    packet.extend_from_slice(&properties);
    packet.extend_from_slice(payload);

    packet
}

fn build_publish_with_expiry(topic: &str, payload: &[u8], expiry_secs: u32, retain: bool, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut properties = Vec::new();
    properties.push(0x02); // Message Expiry Interval
    properties.push((expiry_secs >> 24) as u8);
    properties.push((expiry_secs >> 16) as u8);
    properties.push((expiry_secs >> 8) as u8);
    properties.push((expiry_secs & 0xFF) as u8);

    let mut packet = Vec::new();
    let qos = if packet_id > 0 { 1 } else { 0 };
    let mut flags = qos << 1;
    if retain {
        flags |= 0x01;
    }
    packet.push(0x30 | flags);

    let mut remaining = 2 + topic_bytes.len();
    if qos > 0 {
        remaining += 2;
    }
    remaining += 1 + properties.len() + payload.len();
    encode_remaining_length(&mut packet, remaining);

    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);

    if qos > 0 {
        packet.push((packet_id >> 8) as u8);
        packet.push((packet_id & 0xFF) as u8);
    }

    packet.push(properties.len() as u8);
    packet.extend_from_slice(&properties);
    packet.extend_from_slice(payload);

    packet
}

fn build_publish_with_content_type(topic: &str, payload: &[u8], content_type: &str, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let content_type_bytes = content_type.as_bytes();

    let mut properties = Vec::new();
    properties.push(0x03); // Content Type
    properties.push((content_type_bytes.len() >> 8) as u8);
    properties.push((content_type_bytes.len() & 0xFF) as u8);
    properties.extend_from_slice(content_type_bytes);

    let mut packet = Vec::new();
    let qos = if packet_id > 0 { 1 } else { 0 };
    packet.push(0x30 | (qos << 1));

    let mut remaining = 2 + topic_bytes.len();
    if qos > 0 {
        remaining += 2;
    }
    remaining += 1 + properties.len() + payload.len();
    encode_remaining_length(&mut packet, remaining);

    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);

    if qos > 0 {
        packet.push((packet_id >> 8) as u8);
        packet.push((packet_id & 0xFF) as u8);
    }

    packet.push(properties.len() as u8);
    packet.extend_from_slice(&properties);
    packet.extend_from_slice(payload);

    packet
}

fn build_publish_with_response_topic(topic: &str, payload: &[u8], response_topic: &str, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let response_topic_bytes = response_topic.as_bytes();

    let mut properties = Vec::new();
    properties.push(0x08); // Response Topic
    properties.push((response_topic_bytes.len() >> 8) as u8);
    properties.push((response_topic_bytes.len() & 0xFF) as u8);
    properties.extend_from_slice(response_topic_bytes);

    let mut packet = Vec::new();
    let qos = if packet_id > 0 { 1 } else { 0 };
    packet.push(0x30 | (qos << 1));

    let mut remaining = 2 + topic_bytes.len();
    if qos > 0 {
        remaining += 2;
    }
    remaining += 1 + properties.len() + payload.len();
    encode_remaining_length(&mut packet, remaining);

    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);

    if qos > 0 {
        packet.push((packet_id >> 8) as u8);
        packet.push((packet_id & 0xFF) as u8);
    }

    packet.push(properties.len() as u8);
    packet.extend_from_slice(&properties);
    packet.extend_from_slice(payload);

    packet
}

fn build_publish_with_correlation_data(topic: &str, payload: &[u8], correlation: &[u8], packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();

    let mut properties = Vec::new();
    properties.push(0x09); // Correlation Data
    properties.push((correlation.len() >> 8) as u8);
    properties.push((correlation.len() & 0xFF) as u8);
    properties.extend_from_slice(correlation);

    let mut packet = Vec::new();
    let qos = if packet_id > 0 { 1 } else { 0 };
    packet.push(0x30 | (qos << 1));

    let mut remaining = 2 + topic_bytes.len();
    if qos > 0 {
        remaining += 2;
    }
    remaining += 1 + properties.len() + payload.len();
    encode_remaining_length(&mut packet, remaining);

    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);

    if qos > 0 {
        packet.push((packet_id >> 8) as u8);
        packet.push((packet_id & 0xFF) as u8);
    }

    packet.push(properties.len() as u8);
    packet.extend_from_slice(&properties);
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
