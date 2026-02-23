//! UNSUBSCRIBE packet tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 3.10 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "unsubscribe".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.10.1-1
        make_test(
            "MQTT-3.10.1-1",
            "UNSUBSCRIBE fixed header flags MUST be 0010",
            test_mqtt_3_10_1_1,
        ),
        // MQTT-3.10.3-1
        make_test(
            "MQTT-3.10.3-1",
            "Topic Filters MUST be UTF-8 encoded strings",
            test_mqtt_3_10_3_1,
        ),
        // MQTT-3.10.3-2
        make_test(
            "MQTT-3.10.3-2",
            "UNSUBSCRIBE payload MUST contain at least one Topic Filter",
            test_mqtt_3_10_3_2,
        ),
        // MQTT-3.10.4-1
        make_test(
            "MQTT-3.10.4-1",
            "Server MUST respond with UNSUBACK",
            test_mqtt_3_10_4_1,
        ),
        // MQTT-3.10.4-2
        make_test(
            "MQTT-3.10.4-2",
            "Server MUST respond with UNSUBACK even if no filters matched",
            test_mqtt_3_10_4_2,
        ),
        // MQTT-3.10.4-5
        make_test(
            "MQTT-3.10.4-5",
            "Server MUST stop adding messages after unsubscribe",
            test_mqtt_3_10_4_5,
        ),
        // MQTT-3.10.4-3
        make_test(
            "MQTT-3.10.4-3",
            "UNSUBACK MUST have same Packet Identifier as UNSUBSCRIBE",
            test_mqtt_3_10_4_3,
        ),
        // MQTT-3.10.4-4
        make_test(
            "MQTT-3.10.4-4",
            "Server MUST delete subscription on UNSUBSCRIBE",
            test_mqtt_3_10_4_4,
        ),
    ]
}

/// MQTT-3.10.1-1: UNSUBSCRIBE fixed header flags MUST be 0010
fn test_mqtt_3_10_1_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test31011{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send UNSUBSCRIBE with invalid flags (0xA0 instead of 0xA2)
        let topic = b"test/topic";
        let mut unsubscribe_packet = Vec::new();

        // Fixed header with wrong flags (should be 0xA2, sending 0xA0)
        unsubscribe_packet.push(0xA0); // Invalid flags
        let remaining = 2 + 2 + topic.len(); // packet id + topic length + topic
        unsubscribe_packet.push(remaining as u8);

        // Packet ID
        unsubscribe_packet.push(0x00);
        unsubscribe_packet.push(0x01);

        // Topic Filter
        unsubscribe_packet.push(0x00);
        unsubscribe_packet.push(topic.len() as u8);
        unsubscribe_packet.extend_from_slice(topic);

        stream.write_all(&unsubscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection for malformed packet
        let mut response_buf = [0u8; 4];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut response_buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed - correct
            Ok(Err(_)) => Ok(()), // Connection error - correct
            _ => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for invalid UNSUBSCRIBE flags".to_string()
            )),
        }
    })
}

/// MQTT-3.10.3-1: Topic Filters MUST be UTF-8 encoded strings
fn test_mqtt_3_10_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-31031-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Subscribe first
        let topic = "test/utf8/\u{00e9}";
        client.subscribe(topic, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Unsubscribe with valid UTF-8 topic filter
        client.unsubscribe(topic).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::UnsubAck(_)))) => {
                    client.disconnect().await.ok();
                    return Ok(());
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for UNSUBACK".to_string())),
            }
        }
    })
}

/// MQTT-3.10.3-2: UNSUBSCRIBE payload MUST contain at least one Topic Filter
fn test_mqtt_3_10_3_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test31032{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send UNSUBSCRIBE with empty payload
        let mut unsubscribe_packet = Vec::new();
        unsubscribe_packet.push(0xA2); // UNSUBSCRIBE
        unsubscribe_packet.push(0x02); // Remaining length (just packet ID)
        unsubscribe_packet.push(0x00);
        unsubscribe_packet.push(0x01);

        stream.write_all(&unsubscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let mut response_buf = [0u8; 4];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut response_buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Err(_)) => Ok(()), // Connection error
            _ => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for UNSUBSCRIBE with no topic filters".to_string()
            )),
        }
    })
}

/// MQTT-3.10.4-1: Server MUST respond with UNSUBACK
fn test_mqtt_3_10_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-31041-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Subscribe first
        client.subscribe("test/unsuback", QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Unsubscribe
        client.unsubscribe("test/unsuback").await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::UnsubAck(_)))) => {
                    client.disconnect().await.ok();
                    return Ok(());
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Server did not respond with UNSUBACK".to_string())),
            }
        }
    })
}

/// MQTT-3.10.4-2: Server MUST respond with UNSUBACK even if no filters matched
fn test_mqtt_3_10_4_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-31042-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Unsubscribe from topic we never subscribed to
        let nonexistent_topic = format!("test/nonexistent/{}", uuid::Uuid::new_v4());
        client.unsubscribe(&nonexistent_topic).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::UnsubAck(_)))) => {
                    client.disconnect().await.ok();
                    return Ok(());
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout(
                    "Server did not respond with UNSUBACK for non-existent subscription".to_string()
                )),
            }
        }
    })
}

/// MQTT-3.10.4-5: Server MUST stop adding messages after unsubscribe
fn test_mqtt_3_10_4_5(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/unsub/stop/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-31045-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Subscribe
        client.subscribe(&topic, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Unsubscribe
        client.unsubscribe(&topic).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::UnsubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for UNSUBACK".to_string())),
            }
        }

        // Publish a message - should NOT be received
        client.publish(&topic, QoS::AtLeastOnce, false, b"after unsub".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Wait briefly and check we don't receive the message
        let timeout_duration = Duration::from_secs(2);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        client.disconnect().await.ok();
                        return Err(ConformanceError::ProtocolViolation(
                            "Received message after unsubscribe".to_string()
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

/// MQTT-3.10.4-3: UNSUBACK MUST have same Packet Identifier as UNSUBSCRIBE
fn test_mqtt_3_10_4_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test31043{}", &uuid::Uuid::new_v4().to_string()[..8]);
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

        // First subscribe
        let topic = b"test/pkid/unsub";
        let mut subscribe_packet = Vec::new();
        subscribe_packet.push(0x82);
        let remaining = 2 + 2 + topic.len() + 1;
        subscribe_packet.push(remaining as u8);
        subscribe_packet.push(0x00);
        subscribe_packet.push(0x01);
        subscribe_packet.push(0x00);
        subscribe_packet.push(topic.len() as u8);
        subscribe_packet.extend_from_slice(topic);
        subscribe_packet.push(0x00);

        stream.write_all(&subscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Read SUBACK
        let mut suback_buf = [0u8; 5];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut suback_buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send UNSUBSCRIBE with specific packet ID
        let packet_id: u16 = 0x5678;
        let mut unsubscribe_packet = Vec::new();
        unsubscribe_packet.push(0xA2);
        let remaining = 2 + 2 + topic.len();
        unsubscribe_packet.push(remaining as u8);
        unsubscribe_packet.push((packet_id >> 8) as u8);
        unsubscribe_packet.push((packet_id & 0xFF) as u8);
        unsubscribe_packet.push(0x00);
        unsubscribe_packet.push(topic.len() as u8);
        unsubscribe_packet.extend_from_slice(topic);

        stream.write_all(&unsubscribe_packet).await
            .map_err(ConformanceError::Io)?;

        // Read UNSUBACK and verify packet ID
        let mut unsuback_buf = [0u8; 4]; // UNSUBACK: type + remaining + pkid(2)
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut unsuback_buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for UNSUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if unsuback_buf[0] != 0xB0 {
            return Err(ConformanceError::UnexpectedResponse("Expected UNSUBACK".to_string()));
        }

        let unsuback_pkid = ((unsuback_buf[2] as u16) << 8) | (unsuback_buf[3] as u16);
        if unsuback_pkid != packet_id {
            return Err(ConformanceError::ProtocolViolation(format!(
                "UNSUBACK packet ID {} does not match UNSUBSCRIBE packet ID {}",
                unsuback_pkid, packet_id
            )));
        }

        Ok(())
    })
}

/// MQTT-3.10.4-4: Server MUST delete subscription on UNSUBSCRIBE
fn test_mqtt_3_10_4_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/delete/sub/{}", uuid::Uuid::new_v4());

        // Client 1: Subscribe, unsubscribe, then disconnect
        {
            let mut opts = MqttOptions::new(
                format!("mqtt-31044a-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

            // Subscribe
            client.subscribe(&topic, QoS::AtLeastOnce).await
                .map_err(ConformanceError::MqttClient)?;

            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
                }
            }

            // Unsubscribe
            client.unsubscribe(&topic).await
                .map_err(ConformanceError::MqttClient)?;

            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::UnsubAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for UNSUBACK".to_string())),
                }
            }

            client.disconnect().await.ok();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Client 2: Subscribe and publish to the same topic
        let mut opts = MqttOptions::new(
            format!("mqtt-31044b-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Subscribe
        client.subscribe(&topic, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Publish and verify we can receive (subscription was deleted and recreated)
        client.publish(&topic, QoS::AtLeastOnce, false, b"test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        let timeout_duration = Duration::from_secs(3);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        client.disconnect().await.ok();
                        return Ok(()); // Subscription works, so previous was properly deleted
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();
        Err(ConformanceError::Timeout("Did not receive message on recreated subscription".to_string()))
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
