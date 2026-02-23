//! Topic name and filter tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 4.7 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "topics".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-4.7.0-1
        make_test(
            "MQTT-4.7.0-1",
            "Topic Names and Filters MUST be at least one character",
            test_mqtt_4_7_0_1,
        ),
        // MQTT-4.7.1-1
        make_test(
            "MQTT-4.7.1-1",
            "Multi-level wildcard MUST be last character",
            test_mqtt_4_7_1_1,
        ),
        // MQTT-4.7.1-2
        make_test(
            "MQTT-4.7.1-2",
            "Single-level wildcard MUST occupy entire level",
            test_mqtt_4_7_1_2,
        ),
        // MQTT-4.7.2-1
        make_test(
            "MQTT-4.7.2-1",
            "Wildcard filters MUST NOT match $ topics",
            test_mqtt_4_7_2_1,
        ),
        // MQTT-4.7.3-1
        make_test(
            "MQTT-4.7.3-1",
            "Topic Names MUST NOT include wildcards",
            test_mqtt_4_7_3_1,
        ),
        // MQTT-4.7.3-2
        make_test(
            "MQTT-4.7.3-2",
            "Topic Names are case-sensitive",
            test_mqtt_4_7_3_2,
        ),
        // MQTT-4.7.1-3
        make_test(
            "MQTT-4.7.1-3",
            "Multi-level wildcard must be preceded by / unless alone",
            test_mqtt_4_7_1_3,
        ),
    ]
}

/// MQTT-4.7.0-1: Topic Names and Filters MUST be at least one character
fn test_mqtt_4_7_0_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test4701{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send SUBSCRIBE with empty topic filter
        let mut subscribe_packet = Vec::new();
        subscribe_packet.push(0x82); // SUBSCRIBE
        subscribe_packet.push(0x05); // Remaining length
        subscribe_packet.push(0x00); // Packet ID MSB
        subscribe_packet.push(0x01); // Packet ID LSB
        subscribe_packet.push(0x00); // Topic length MSB
        subscribe_packet.push(0x00); // Topic length LSB (empty topic)
        subscribe_packet.push(0x00); // QoS

        stream.write_all(&subscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection or reject
        let mut response_buf = [0u8; 5];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut response_buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Err(_)) => Ok(()), // Connection error
            Ok(Ok(n)) => {
                // Check if SUBACK with failure code
                if n >= 5 && response_buf[0] == 0x90 && response_buf[4] == 0x80 {
                    Ok(()) // SUBACK with failure
                } else {
                    Err(ConformanceError::ProtocolViolation(
                        "Server accepted empty topic filter".to_string()
                    ))
                }
            }
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not reject empty topic filter".to_string()
            )),
        }
    })
}

/// MQTT-4.7.1-1: Multi-level wildcard MUST be last character
fn test_mqtt_4_7_1_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-4711-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Valid multi-level wildcard patterns
        let valid_filters = vec![
            "#",
            "test/#",
            "test/+/#",
        ];

        for filter in valid_filters {
            client.subscribe(filter, QoS::AtLeastOnce).await
                .map_err(ConformanceError::MqttClient)?;

            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::SubAck(suback)))) => {
                        if suback.return_codes.contains(&rumqttc::SubscribeReasonCode::Failure) {
                            client.disconnect().await.ok();
                            return Err(ConformanceError::ProtocolViolation(format!(
                                "Valid wildcard filter '{}' was rejected",
                                filter
                            )));
                        }
                        break;
                    }
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout(format!("Waiting for SUBACK for '{}'", filter))),
                }
            }

            client.unsubscribe(filter).await.ok();
            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::UnsubAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    _ => break,
                }
            }
        }

        client.disconnect().await.ok();
        Ok(())
    })
}

/// MQTT-4.7.1-2: Single-level wildcard MUST occupy entire level
fn test_mqtt_4_7_1_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-4712-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Valid single-level wildcard patterns
        let valid_filters = vec![
            "+",
            "+/test",
            "test/+",
            "test/+/foo",
            "+/+/+",
        ];

        for filter in valid_filters {
            client.subscribe(filter, QoS::AtLeastOnce).await
                .map_err(ConformanceError::MqttClient)?;

            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::SubAck(suback)))) => {
                        if suback.return_codes.contains(&rumqttc::SubscribeReasonCode::Failure) {
                            client.disconnect().await.ok();
                            return Err(ConformanceError::ProtocolViolation(format!(
                                "Valid wildcard filter '{}' was rejected",
                                filter
                            )));
                        }
                        break;
                    }
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout(format!("Waiting for SUBACK for '{}'", filter))),
                }
            }

            client.unsubscribe(filter).await.ok();
            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::UnsubAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    _ => break,
                }
            }
        }

        client.disconnect().await.ok();
        Ok(())
    })
}

/// MQTT-4.7.2-1: Wildcard filters MUST NOT match $ topics
fn test_mqtt_4_7_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-4721-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Subscribe with wildcard
        client.subscribe("#", QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Publish to a $ topic
        let dollar_topic = format!("$SYS/test/{}", uuid::Uuid::new_v4());
        client.publish(&dollar_topic, QoS::AtLeastOnce, false, b"test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Wait briefly - should NOT receive the message via wildcard subscription
        let timeout_duration = Duration::from_secs(2);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic.starts_with("$") {
                        client.disconnect().await.ok();
                        return Err(ConformanceError::ProtocolViolation(
                            "Wildcard subscription matched $ topic".to_string()
                        ));
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();
        Ok(())
    })
}

/// MQTT-4.7.3-1: Topic Names MUST NOT include wildcards
fn test_mqtt_4_7_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test4731{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send PUBLISH with wildcard in topic name using QoS 1
        // so we can detect if server accepts it (via PUBACK)
        let topic_with_wildcard = b"test/+/invalid";
        let payload = b"test";
        let mut publish_packet = Vec::new();

        // PUBLISH with QoS 1 (0x32 = 0011 0010: PUBLISH, QoS 1)
        publish_packet.push(0x32);
        let remaining = 2 + topic_with_wildcard.len() + 2 + payload.len(); // +2 for packet ID
        publish_packet.push(remaining as u8);

        // Topic
        publish_packet.push(0x00);
        publish_packet.push(topic_with_wildcard.len() as u8);
        publish_packet.extend_from_slice(topic_with_wildcard);

        // Packet ID (required for QoS 1)
        publish_packet.push(0x00);
        publish_packet.push(0x01);

        // Payload
        publish_packet.extend_from_slice(payload);

        stream.write_all(&publish_packet).await
            .map_err(ConformanceError::Io)?;

        // Server should reject - either close connection or not send PUBACK
        // If we get a PUBACK, the server incorrectly accepted the wildcard topic
        let mut response_buf = [0u8; 4];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut response_buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed - correct rejection
            Ok(Err(_)) => Ok(()), // Connection error/RST - correct rejection
            Ok(Ok(n)) if n >= 1 && response_buf[0] == 0x40 => {
                // 0x40 = PUBACK - server incorrectly accepted wildcard topic
                Err(ConformanceError::ProtocolViolation(
                    "Server sent PUBACK for PUBLISH with wildcard in topic name".to_string()
                ))
            }
            Ok(Ok(_)) => Ok(()), // Some other response - might be disconnect
            Err(_) => Ok(()), // Timeout without PUBACK - server dropped it (acceptable)
        }
    })
}

/// MQTT-4.7.3-2: Topic Names are case-sensitive
fn test_mqtt_4_7_3_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let base_topic = format!("test/case/{}", uuid::Uuid::new_v4());
        let topic_lower = format!("{}/topic", base_topic);
        let topic_upper = format!("{}/TOPIC", base_topic);

        let mut opts = MqttOptions::new(
            format!("mqtt-4732-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Subscribe to lowercase topic only
        client.subscribe(&topic_lower, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Publish to UPPERCASE topic - should NOT be received
        client.publish(&topic_upper, QoS::AtLeastOnce, false, b"upper".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Publish to lowercase topic - should be received
        client.publish(&topic_lower, QoS::AtLeastOnce, false, b"lower".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Collect messages
        let mut received_lower = false;
        let mut received_upper = false;
        let timeout_duration = Duration::from_secs(3);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic_lower {
                        received_lower = true;
                    } else if publish.topic == topic_upper {
                        received_upper = true;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => {
                    if received_lower {
                        break;
                    }
                    continue;
                }
            }
        }

        client.disconnect().await.ok();

        if received_upper {
            Err(ConformanceError::ProtocolViolation(
                "Topics are not case-sensitive - received uppercase message on lowercase subscription".to_string()
            ))
        } else if !received_lower {
            Err(ConformanceError::Timeout("Did not receive lowercase message".to_string()))
        } else {
            Ok(())
        }
    })
}

/// MQTT-4.7.1-3: Multi-level wildcard must be preceded by / unless alone
fn test_mqtt_4_7_1_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-4713-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Valid: # alone
        client.subscribe("#", QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(suback)))) => {
                    if suback.return_codes.contains(&rumqttc::SubscribeReasonCode::Failure) {
                        client.disconnect().await.ok();
                        return Err(ConformanceError::ProtocolViolation(
                            "Valid '#' filter rejected".to_string()
                        ));
                    }
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK for '#'".to_string())),
            }
        }

        client.unsubscribe("#").await.ok();
        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::UnsubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                _ => break,
            }
        }

        // Valid: /# (preceded by /)
        client.subscribe("test/level/#", QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(suback)))) => {
                    if suback.return_codes.contains(&rumqttc::SubscribeReasonCode::Failure) {
                        client.disconnect().await.ok();
                        return Err(ConformanceError::ProtocolViolation(
                            "Valid 'test/level/#' filter rejected".to_string()
                        ));
                    }
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK for 'test/level/#'".to_string())),
            }
        }

        client.disconnect().await.ok();
        Ok(())
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
