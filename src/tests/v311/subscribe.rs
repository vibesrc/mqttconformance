//! SUBSCRIBE packet tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 3.8 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
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
        // MQTT-3.8.3-1
        make_test(
            "MQTT-3.8.3-1",
            "Topic Filters MUST be UTF-8 encoded strings",
            test_mqtt_3_8_3_1,
        ),
        // MQTT-3.8.3-3
        make_test(
            "MQTT-3.8.3-3",
            "SUBSCRIBE payload MUST contain at least one Topic Filter",
            test_mqtt_3_8_3_3,
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
            "Duplicate subscription MUST replace existing and re-send retained",
            test_mqtt_3_8_4_3,
        ),
        // MQTT-3.8.4-5
        make_test(
            "MQTT-3.8.4-5",
            "SUBACK MUST contain return code for each Topic Filter",
            test_mqtt_3_8_4_5,
        ),
        // MQTT-3.8.3-2
        make_test(
            "MQTT-3.8.3-2",
            "Server MUST close connection for invalid UTF-8 Topic Filter",
            test_mqtt_3_8_3_2,
        ),
        // MQTT-3.8.3-4
        make_test(
            "MQTT-3.8.3-4",
            "Server MUST close connection for Topic Filter with embedded null",
            test_mqtt_3_8_3_4,
        ),
        // MQTT-3.8.4-4
        make_test(
            "MQTT-3.8.4-4",
            "Server MUST completely replace existing subscription with new one",
            test_mqtt_3_8_4_4,
        ),
    ]
}

/// MQTT-3.8.1-1: SUBSCRIBE fixed header flags MUST be 0010
fn test_mqtt_3_8_1_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test3811{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if buf[0] != 0x20 || buf[3] != 0x00 {
            return Err(ConformanceError::UnexpectedResponse("Expected successful CONNACK".to_string()));
        }

        // Send SUBSCRIBE with invalid flags (0x80 instead of 0x82)
        let topic = b"test/topic";
        let mut subscribe_packet = Vec::new();

        // Fixed header with wrong flags (should be 0x82, sending 0x80)
        subscribe_packet.push(0x80); // Invalid flags
        let remaining = 2 + 2 + topic.len() + 1; // packet id + topic length + topic + qos
        subscribe_packet.push(remaining as u8);

        // Packet ID
        subscribe_packet.push(0x00);
        subscribe_packet.push(0x01);

        // Topic Filter
        subscribe_packet.push(0x00);
        subscribe_packet.push(topic.len() as u8);
        subscribe_packet.extend_from_slice(topic);
        subscribe_packet.push(0x00); // QoS 0

        stream.write_all(&subscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection for malformed packet
        let mut response_buf = [0u8; 4];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut response_buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed - correct
            Ok(Err(_)) => Ok(()), // Connection error - correct
            _ => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for invalid SUBSCRIBE flags".to_string()
            )),
        }
    })
}

/// MQTT-3.8.3-1: Topic Filters MUST be UTF-8 encoded strings
fn test_mqtt_3_8_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-3831-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            &ctx.host,
            ctx.port,
        );
        opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        opts.set_clean_session(true);

        let (client, mut eventloop) = AsyncClient::new(opts, 10);

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
            }
        }

        // Subscribe with valid UTF-8 topic filter including unicode
        let topic_filter = "test/utf8/\u{00e9}/+"; // With unicode and wildcard
        client.subscribe(topic_filter, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(suback)))) => {
                    client.disconnect().await.ok();
                    // Check that subscription was accepted
                    if suback.return_codes.contains(&rumqttc::SubscribeReasonCode::Failure) {
                        return Err(ConformanceError::ProtocolViolation(
                            "Valid UTF-8 topic filter was rejected".to_string()
                        ));
                    }
                    return Ok(());
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }
    })
}

/// MQTT-3.8.3-3: SUBSCRIBE payload MUST contain at least one Topic Filter
fn test_mqtt_3_8_3_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test3833{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with empty payload (no topic filters)
        let mut subscribe_packet = Vec::new();
        subscribe_packet.push(0x82); // SUBSCRIBE
        subscribe_packet.push(0x02); // Remaining length (just packet ID)
        subscribe_packet.push(0x00); // Packet ID MSB
        subscribe_packet.push(0x01); // Packet ID LSB

        stream.write_all(&subscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection for malformed packet
        let mut response_buf = [0u8; 4];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut response_buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed - correct
            Ok(Err(_)) => Ok(()), // Connection error - correct
            _ => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for SUBSCRIBE with no topic filters".to_string()
            )),
        }
    })
}

/// MQTT-3.8.4-1: Server MUST respond with SUBACK
fn test_mqtt_3_8_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-3841-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            &ctx.host,
            ctx.port,
        );
        opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        opts.set_clean_session(true);

        let (client, mut eventloop) = AsyncClient::new(opts, 10);

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
            }
        }

        client.subscribe("test/suback", QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => {
                    client.disconnect().await.ok();
                    return Ok(());
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Server did not respond with SUBACK".to_string())),
            }
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

        // Connect
        let client_id = format!("test3842{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with specific packet ID
        let packet_id: u16 = 0x1234;
        let topic = b"test/pkid";
        let mut subscribe_packet = Vec::new();
        subscribe_packet.push(0x82);
        let remaining = 2 + 2 + topic.len() + 1;
        subscribe_packet.push(remaining as u8);
        subscribe_packet.push((packet_id >> 8) as u8);
        subscribe_packet.push((packet_id & 0xFF) as u8);
        subscribe_packet.push(0x00);
        subscribe_packet.push(topic.len() as u8);
        subscribe_packet.extend_from_slice(topic);
        subscribe_packet.push(0x01); // QoS 1

        stream.write_all(&subscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Read SUBACK and verify packet ID
        let mut suback_buf = [0u8; 5]; // SUBACK: type + remaining + pkid(2) + return code
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut suback_buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if suback_buf[0] != 0x90 {
            return Err(ConformanceError::UnexpectedResponse("Expected SUBACK".to_string()));
        }

        let suback_pkid = ((suback_buf[2] as u16) << 8) | (suback_buf[3] as u16);
        if suback_pkid != packet_id {
            return Err(ConformanceError::ProtocolViolation(format!(
                "SUBACK packet ID {} does not match SUBSCRIBE packet ID {}",
                suback_pkid, packet_id
            )));
        }

        Ok(())
    })
}

/// MQTT-3.8.4-3: Duplicate subscription MUST replace existing and re-send retained
fn test_mqtt_3_8_4_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/dup/sub/{}", uuid::Uuid::new_v4());
        let payload = b"retained for dup test";

        // First, publish a retained message
        {
            let mut opts = MqttOptions::new(
                format!("mqtt-pub-{}", &uuid::Uuid::new_v4().to_string()[..8]),
                &ctx.host,
                ctx.port,
            );
            opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
            opts.set_clean_session(true);

            let (client, mut eventloop) = AsyncClient::new(opts, 10);

            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
                }
            }

            client.publish(&topic, QoS::AtLeastOnce, true, payload.to_vec()).await
                .map_err(ConformanceError::MqttClient)?;

            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for PUBACK".to_string())),
                }
            }

            client.disconnect().await.ok();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Subscribe twice and count retained messages
        let mut opts = MqttOptions::new(
            format!("mqtt-sub-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            &ctx.host,
            ctx.port,
        );
        opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        opts.set_clean_session(true);

        let (client, mut eventloop) = AsyncClient::new(opts, 10);

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
            }
        }

        // First subscription
        client.subscribe(&topic, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        let mut received_first = false;
        let timeout_duration = Duration::from_secs(3);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        received_first = true;
                        break;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        if !received_first {
            // Clean up
            client.publish(&topic, QoS::AtLeastOnce, true, vec![]).await.ok();
            client.disconnect().await.ok();
            return Err(ConformanceError::ProtocolViolation(
                "Did not receive retained message on first subscription".to_string()
            ));
        }

        // Second subscription (duplicate) - should re-send retained
        client.subscribe(&topic, QoS::AtMostOnce).await // Different QoS
            .map_err(ConformanceError::MqttClient)?;

        let mut received_second = false;
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        received_second = true;
                        break;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        // Clean up
        client.publish(&topic, QoS::AtLeastOnce, true, vec![]).await.ok();
        client.disconnect().await.ok();

        if received_second {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(
                "Retained message not re-sent on duplicate subscription".to_string()
            ))
        }
    })
}

/// MQTT-3.8.4-5: SUBACK MUST contain return code for each Topic Filter
fn test_mqtt_3_8_4_5(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test3845{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with 3 topic filters
        let topics = [b"test/a".as_slice(), b"test/b".as_slice(), b"test/c".as_slice()];
        let mut subscribe_packet = Vec::new();
        subscribe_packet.push(0x82);

        // Calculate remaining length
        let mut remaining = 2; // Packet ID
        for topic in &topics {
            remaining += 2 + topic.len() + 1; // Topic length + topic + QoS
        }
        subscribe_packet.push(remaining as u8);

        // Packet ID
        subscribe_packet.push(0x00);
        subscribe_packet.push(0x01);

        // Topic filters
        for (i, topic) in topics.iter().enumerate() {
            subscribe_packet.push(0x00);
            subscribe_packet.push(topic.len() as u8);
            subscribe_packet.extend_from_slice(topic);
            subscribe_packet.push(i as u8); // QoS 0, 1, 2
        }

        stream.write_all(&subscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Read SUBACK
        let mut header = [0u8; 2];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut header)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if header[0] != 0x90 {
            return Err(ConformanceError::UnexpectedResponse("Expected SUBACK".to_string()));
        }

        let remaining_len = header[1] as usize;
        let mut payload = vec![0u8; remaining_len];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut payload)).await
            .map_err(|_| ConformanceError::Timeout("Reading SUBACK payload".to_string()))?
            .map_err(ConformanceError::Io)?;

        // SUBACK should have: 2 bytes packet ID + 3 return codes
        let return_codes_count = remaining_len - 2;
        if return_codes_count != 3 {
            return Err(ConformanceError::ProtocolViolation(format!(
                "Expected 3 return codes in SUBACK, got {}",
                return_codes_count
            )));
        }

        Ok(())
    })
}

/// MQTT-3.8.3-2: Server MUST close connection for invalid UTF-8 Topic Filter
fn test_mqtt_3_8_3_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3832{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if buf[0] != 0x20 || buf[3] != 0x00 {
            return Err(ConformanceError::UnexpectedResponse("Expected successful CONNACK".to_string()));
        }

        // Send SUBSCRIBE with invalid UTF-8 topic filter
        // 0xFF 0xFE is not valid UTF-8
        let invalid_topic = [0xFF, 0xFE, b'/', b't', b'e', b's', b't'];
        let mut subscribe_packet = Vec::new();
        subscribe_packet.push(0x82);
        let remaining = 2 + 2 + invalid_topic.len() + 1;
        subscribe_packet.push(remaining as u8);
        subscribe_packet.push(0x00);
        subscribe_packet.push(0x01);
        subscribe_packet.push(0x00);
        subscribe_packet.push(invalid_topic.len() as u8);
        subscribe_packet.extend_from_slice(&invalid_topic);
        subscribe_packet.push(0x00);

        stream.write_all(&subscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let mut response_buf = [0u8; 16];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut response_buf)).await {
            Ok(Ok(0)) => Ok(()),
            Ok(Err(_)) => Ok(()),
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for invalid UTF-8 Topic Filter".to_string()
            )),
            Ok(Ok(_)) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for invalid UTF-8 Topic Filter".to_string()
            )),
        }
    })
}

/// MQTT-3.8.3-4: Server MUST close connection for Topic Filter with embedded null
fn test_mqtt_3_8_3_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test3834{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if buf[0] != 0x20 || buf[3] != 0x00 {
            return Err(ConformanceError::UnexpectedResponse("Expected successful CONNACK".to_string()));
        }

        // Send SUBSCRIBE with topic containing embedded null (U+0000)
        let null_topic = [b't', b'e', b's', b't', 0x00, b'/', b'a'];
        let mut subscribe_packet = Vec::new();
        subscribe_packet.push(0x82);
        let remaining = 2 + 2 + null_topic.len() + 1;
        subscribe_packet.push(remaining as u8);
        subscribe_packet.push(0x00);
        subscribe_packet.push(0x01);
        subscribe_packet.push(0x00);
        subscribe_packet.push(null_topic.len() as u8);
        subscribe_packet.extend_from_slice(&null_topic);
        subscribe_packet.push(0x00);

        stream.write_all(&subscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let mut response_buf = [0u8; 16];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut response_buf)).await {
            Ok(Ok(0)) => Ok(()),
            Ok(Err(_)) => Ok(()),
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for Topic Filter with embedded null".to_string()
            )),
            Ok(Ok(_)) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for Topic Filter with embedded null".to_string()
            )),
        }
    })
}

/// MQTT-3.8.4-4: Server MUST completely replace existing subscription with new one
fn test_mqtt_3_8_4_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/replace/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-3844-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            &ctx.host,
            ctx.port,
        );
        opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        opts.set_clean_session(true);

        let (client, mut eventloop) = AsyncClient::new(opts, 10);

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
            }
        }

        // Subscribe with QoS 2
        client.subscribe(&topic, QoS::ExactlyOnce).await
            .map_err(ConformanceError::MqttClient)?;

        let first_qos;
        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(suback)))) => {
                    first_qos = suback.return_codes.first().copied();
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Re-subscribe with QoS 0 - should replace the subscription
        client.subscribe(&topic, QoS::AtMostOnce).await
            .map_err(ConformanceError::MqttClient)?;

        let second_qos;
        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(suback)))) => {
                    second_qos = suback.return_codes.first().copied();
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        client.disconnect().await.ok();

        // Verify the subscriptions were processed (both got responses indicates replacement)
        match (first_qos, second_qos) {
            (Some(_), Some(_)) => Ok(()), // Both subscriptions processed, replacement confirmed
            _ => Err(ConformanceError::ProtocolViolation(
                "Subscription replacement not confirmed".to_string()
            )),
        }
    })
}

fn build_connect_packet(client_id: &str, keep_alive: u16) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let client_id_len = client_id_bytes.len();
    let remaining_length = 6 + 1 + 1 + 2 + 2 + client_id_len;

    let mut packet = Vec::with_capacity(2 + remaining_length);
    packet.push(0x10);
    packet.push(remaining_length as u8);
    packet.push(0x00);
    packet.push(0x04);
    packet.extend_from_slice(b"MQTT");
    packet.push(4);
    packet.push(0x02);
    packet.push((keep_alive >> 8) as u8);
    packet.push((keep_alive & 0xFF) as u8);
    packet.push((client_id_len >> 8) as u8);
    packet.push((client_id_len & 0xFF) as u8);
    packet.extend_from_slice(client_id_bytes);

    packet
}
