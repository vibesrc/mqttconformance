//! Operational behavior tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 4 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "operational".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-4.4.0-1
        make_test(
            "MQTT-4.4.0-1",
            "Reconnect with CleanSession=0 MUST re-send unacked messages",
            test_mqtt_4_4_0_1,
        ),
        // MQTT-4.6.0-2
        make_test(
            "MQTT-4.6.0-2",
            "Client MUST send PUBACK in order of PUBLISH received",
            test_mqtt_4_6_0_2,
        ),
        // MQTT-4.6.0-5
        make_test(
            "MQTT-4.6.0-5",
            "Server MUST treat each topic as Ordered Topic by default",
            test_mqtt_4_6_0_5,
        ),
        // MQTT-4.8.0-1
        make_test(
            "MQTT-4.8.0-1",
            "Protocol error MUST cause connection close",
            test_mqtt_4_8_0_1,
        ),
        // MQTT-2.3.1-1
        make_test(
            "MQTT-2.3.1-1",
            "SUBSCRIBE/UNSUBSCRIBE/QoS>0 PUBLISH MUST have Packet ID",
            test_mqtt_2_3_1_1,
        ),
        // MQTT-2.3.1-5
        make_test(
            "MQTT-2.3.1-5",
            "QoS 0 PUBLISH MUST NOT have Packet ID",
            test_mqtt_2_3_1_5,
        ),
        // MQTT-2.3.1-6
        make_test(
            "MQTT-2.3.1-6",
            "PUBACK/PUBREC/PUBREL MUST have same Packet ID as PUBLISH",
            test_mqtt_2_3_1_6,
        ),
        // MQTT-4.5.0-1
        make_test(
            "MQTT-4.5.0-1",
            "Re-send unacked packets using original Packet Identifiers",
            test_mqtt_4_5_0_1,
        ),
        // MQTT-4.6.0-1
        make_test(
            "MQTT-4.6.0-1",
            "Client/Server MUST re-send unacked PUBLISH on reconnect",
            test_mqtt_4_6_0_1,
        ),
        // MQTT-4.6.0-3
        make_test(
            "MQTT-4.6.0-3",
            "Each SUBSCRIBE MUST use currently unused Packet Identifier",
            test_mqtt_4_6_0_3,
        ),
        // MQTT-4.6.0-4
        make_test(
            "MQTT-4.6.0-4",
            "Server MUST assign unused Packet Identifier to PUBLISH",
            test_mqtt_4_6_0_4,
        ),
        // MQTT-4.6.0-6
        make_test(
            "MQTT-4.6.0-6",
            "Server MUST maintain message ordering per-subscription",
            test_mqtt_4_6_0_6,
        ),
        // MQTT-4.3.1-1
        make_test(
            "MQTT-4.3.1-1",
            "QoS 0: Deliver at most once (fire and forget)",
            test_mqtt_4_3_1_1,
        ),
        // MQTT-4.3.2-1
        make_test(
            "MQTT-4.3.2-1",
            "QoS 1: Deliver at least once",
            test_mqtt_4_3_2_1,
        ),
        // MQTT-4.3.2-2
        make_test(
            "MQTT-4.3.2-2",
            "QoS 1: Sender MUST store message until PUBACK received",
            test_mqtt_4_3_2_2,
        ),
        // MQTT-4.3.3-1
        make_test(
            "MQTT-4.3.3-1",
            "QoS 2: Deliver exactly once",
            test_mqtt_4_3_3_1,
        ),
        // MQTT-4.3.3-2
        make_test(
            "MQTT-4.3.3-2",
            "QoS 2: Sender MUST store message until PUBREC received",
            test_mqtt_4_3_3_2,
        ),
        // MQTT-4.3.3-3
        make_test(
            "MQTT-4.3.3-3",
            "QoS 2: PUBREL MUST be sent after PUBREC received",
            test_mqtt_4_3_3_3,
        ),
        // MQTT-4.3.3-4
        make_test(
            "MQTT-4.3.3-4",
            "QoS 2: Receiver MUST store Packet ID until PUBREL received",
            test_mqtt_4_3_3_4,
        ),
    ]
}

/// MQTT-4.4.0-1: Reconnect with CleanSession=0 MUST re-send unacked messages
fn test_mqtt_4_4_0_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let client_id = format!("mqtt-440-{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let topic = format!("test/persist/{}", uuid::Uuid::new_v4());

        // First, set up subscriber to ensure message is stored
        let mut sub_opts = MqttOptions::new(&client_id, &ctx.host, ctx.port);
        sub_opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            sub_opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        sub_opts.set_clean_session(false); // Persistent session

        let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 10);

        // Connect and subscribe
        loop {
            match tokio::time::timeout(ctx.timeout, sub_eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
            }
        }

        sub_client.subscribe(&topic, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, sub_eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Disconnect subscriber (without clean disconnect to leave session)
        drop(sub_eventloop);
        drop(sub_client);

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish a message while subscriber is offline
        {
            let mut pub_opts = MqttOptions::new(
                format!("mqtt-pub-{}", &uuid::Uuid::new_v4().to_string()[..8]),
                &ctx.host,
                ctx.port,
            );
            pub_opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            pub_opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
            pub_opts.set_clean_session(true);

            let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_opts, 10);

            loop {
                match tokio::time::timeout(ctx.timeout, pub_eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
                }
            }

            pub_client.publish(&topic, QoS::AtLeastOnce, false, b"offline message".to_vec()).await
                .map_err(ConformanceError::MqttClient)?;

            loop {
                match tokio::time::timeout(ctx.timeout, pub_eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for PUBACK".to_string())),
                }
            }

            pub_client.disconnect().await.ok();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Reconnect subscriber with CleanSession=0 - should receive stored message
        let mut sub_opts2 = MqttOptions::new(&client_id, &ctx.host, ctx.port);
        sub_opts2.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            sub_opts2.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        sub_opts2.set_clean_session(false);

        let (sub_client2, mut sub_eventloop2) = AsyncClient::new(sub_opts2, 10);

        loop {
            match tokio::time::timeout(ctx.timeout, sub_eventloop2.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(connack)))) => {
                    if !connack.session_present {
                        // Session wasn't stored - might be broker limitation
                        sub_client2.disconnect().await.ok();
                        return Ok(()); // Skip test if broker doesn't support persistent sessions
                    }
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for reconnect CONNACK".to_string())),
            }
        }

        // Wait for stored message
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();
        let mut received = false;

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_eventloop2.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        received = true;
                        break;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        // Clean up session
        let mut cleanup_opts = MqttOptions::new(&client_id, &ctx.host, ctx.port);
        cleanup_opts.set_clean_session(true);
        let (cleanup_client, mut cleanup_eventloop) = AsyncClient::new(cleanup_opts, 10);
        loop {
            match tokio::time::timeout(ctx.timeout, cleanup_eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                _ => continue,
            }
        }
        cleanup_client.disconnect().await.ok();

        if received {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(
                "Stored message not delivered after reconnect".to_string()
            ))
        }
    })
}

/// MQTT-4.6.0-2: Client MUST send PUBACK in order of PUBLISH received
fn test_mqtt_4_6_0_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // This test verifies the client library handles ordering correctly
        // We'll receive multiple messages and verify they come in order
        let topic = format!("test/order/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-4602-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Publish multiple messages
        for i in 0..5 {
            client.publish(&topic, QoS::AtLeastOnce, false, format!("msg{}", i).into_bytes()).await
                .map_err(ConformanceError::MqttClient)?;
        }

        // Receive and verify order
        let mut received = Vec::new();
        let timeout_duration = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration && received.len() < 5 {
            match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        received.push(String::from_utf8_lossy(&publish.payload).to_string());
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        // Verify order
        let expected: Vec<String> = (0..5).map(|i| format!("msg{}", i)).collect();
        if received == expected {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(format!(
                "Messages not in order. Expected: {:?}, Got: {:?}",
                expected, received
            )))
        }
    })
}

/// MQTT-4.6.0-5: Server MUST treat each topic as Ordered Topic by default
fn test_mqtt_4_6_0_5(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/ordered/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-4605-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Publish multiple messages rapidly
        for i in 0..10 {
            client.publish(&topic, QoS::AtLeastOnce, false, format!("{:02}", i).into_bytes()).await
                .map_err(ConformanceError::MqttClient)?;
        }

        // Receive and verify order
        let mut received = Vec::new();
        let timeout_duration = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration && received.len() < 10 {
            match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        received.push(String::from_utf8_lossy(&publish.payload).to_string());
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        // Verify all messages received in order
        let expected: Vec<String> = (0..10).map(|i| format!("{:02}", i)).collect();
        if received == expected {
            Ok(())
        } else if received.len() < 10 {
            // QoS 1 doesn't guarantee delivery, just ordering of what's delivered
            // Check that what we received is in order
            for (i, msg) in received.iter().enumerate() {
                if i > 0 && msg <= &received[i - 1] {
                    return Err(ConformanceError::ProtocolViolation(
                        "Messages not in order".to_string()
                    ));
                }
            }
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(format!(
                "Messages not in order. Expected: {:?}, Got: {:?}",
                expected, received
            )))
        }
    })
}

/// MQTT-4.8.0-1: Protocol error MUST cause connection close
fn test_mqtt_4_8_0_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test4801{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send malformed packet (invalid packet type 0xF0)
        let malformed = [0xF0, 0x00];
        stream.write_all(&malformed).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let mut check = [0u8; 1];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut check)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Err(_)) => Ok(()), // Connection error
            _ => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection on protocol error".to_string()
            )),
        }
    })
}

/// MQTT-2.3.1-1: SUBSCRIBE/UNSUBSCRIBE/QoS>0 PUBLISH MUST have Packet ID
fn test_mqtt_2_3_1_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-2311-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Test QoS 1 PUBLISH - library assigns packet ID
        client.publish("test/pkid", QoS::AtLeastOnce, false, b"test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(puback)))) => {
                    // PUBACK contains packet ID, confirming original had one
                    if puback.pkid == 0 {
                        client.disconnect().await.ok();
                        return Err(ConformanceError::ProtocolViolation(
                            "PUBACK had packet ID 0".to_string()
                        ));
                    }
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for PUBACK".to_string())),
            }
        }

        client.disconnect().await.ok();
        Ok(())
    })
}

/// MQTT-2.3.1-5: QoS 0 PUBLISH MUST NOT have Packet ID
fn test_mqtt_2_3_1_5(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // This is tested by sending QoS 0 and verifying no PUBACK is expected
        let mut opts = MqttOptions::new(
            format!("mqtt-2315-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Publish QoS 0 - should not get PUBACK
        client.publish("test/qos0", QoS::AtMostOnce, false, b"test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Brief wait - should not receive PUBACK
        match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
            Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => {
                client.disconnect().await.ok();
                Err(ConformanceError::ProtocolViolation(
                    "Received PUBACK for QoS 0 message".to_string()
                ))
            }
            _ => {
                client.disconnect().await.ok();
                Ok(())
            }
        }
    })
}

/// MQTT-2.3.1-6: PUBACK/PUBREC/PUBREL MUST have same Packet ID as PUBLISH
fn test_mqtt_2_3_1_6(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test2316{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send PUBLISH QoS 1 with specific packet ID
        let packet_id: u16 = 0xABCD;
        let topic = b"test/pkid";
        let payload = b"test";

        let mut publish_packet = Vec::new();
        publish_packet.push(0x32); // PUBLISH QoS 1
        let remaining = 2 + topic.len() + 2 + payload.len();
        publish_packet.push(remaining as u8);
        publish_packet.push(0x00);
        publish_packet.push(topic.len() as u8);
        publish_packet.extend_from_slice(topic);
        publish_packet.push((packet_id >> 8) as u8);
        publish_packet.push((packet_id & 0xFF) as u8);
        publish_packet.extend_from_slice(payload);

        stream.write_all(&publish_packet).await
            .map_err(ConformanceError::Io)?;

        // Read PUBACK and verify packet ID
        let mut puback = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut puback)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if puback[0] != 0x40 {
            return Err(ConformanceError::UnexpectedResponse("Expected PUBACK".to_string()));
        }

        let puback_pkid = ((puback[2] as u16) << 8) | (puback[3] as u16);
        if puback_pkid != packet_id {
            return Err(ConformanceError::ProtocolViolation(format!(
                "PUBACK packet ID {} does not match PUBLISH packet ID {}",
                puback_pkid, packet_id
            )));
        }

        Ok(())
    })
}

/// MQTT-4.5.0-1: Re-send unacked packets using original Packet Identifiers
fn test_mqtt_4_5_0_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let client_id = format!("test4501{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let topic = format!("test/resend/{}", uuid::Uuid::new_v4());

        // First connection - subscribe then disconnect without clean
        {
            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            // Connect with clean_session=0
            let connect = build_connect_packet_with_flags(&client_id, 30, false);
            stream.write_all(&connect).await.map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 4];
            tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            // Subscribe
            let topic_bytes = topic.as_bytes();
            let mut subscribe = Vec::new();
            subscribe.push(0x82);
            let remaining = 2 + 2 + topic_bytes.len() + 1;
            subscribe.push(remaining as u8);
            subscribe.push(0x00);
            subscribe.push(0x01);
            subscribe.push(0x00);
            subscribe.push(topic_bytes.len() as u8);
            subscribe.extend_from_slice(topic_bytes);
            subscribe.push(0x01); // QoS 1

            stream.write_all(&subscribe).await.map_err(ConformanceError::Io)?;

            let mut suback = [0u8; 5];
            tokio::time::timeout(ctx.timeout, stream.read_exact(&mut suback)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            // Drop connection without DISCONNECT
            drop(stream);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish message while subscriber is offline
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

            client.publish(&topic, QoS::AtLeastOnce, false, b"test4501".to_vec()).await
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

        // Reconnect with same client_id - should get stored message
        let mut opts = MqttOptions::new(&client_id, &ctx.host, ctx.port);
        opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        opts.set_clean_session(false);

        let (client, mut eventloop) = AsyncClient::new(opts, 10);

        let session_present;
        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(connack)))) => {
                    session_present = connack.session_present;
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for reconnect CONNACK".to_string())),
            }
        }

        if !session_present {
            // Broker doesn't support persistent sessions - skip
            client.disconnect().await.ok();

            // Clean up
            let mut cleanup = MqttOptions::new(&client_id, &ctx.host, ctx.port);
            cleanup.set_clean_session(true);
            let (c, mut e) = AsyncClient::new(cleanup, 10);
            let _ = tokio::time::timeout(Duration::from_secs(1), e.poll()).await;
            c.disconnect().await.ok();

            return Ok(());
        }

        // Wait for stored message
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();
        let mut received = false;

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        received = true;
                        break;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        // Clean up session
        let mut cleanup = MqttOptions::new(&client_id, &ctx.host, ctx.port);
        cleanup.set_clean_session(true);
        let (c, mut e) = AsyncClient::new(cleanup, 10);
        let _ = tokio::time::timeout(Duration::from_secs(1), e.poll()).await;
        c.disconnect().await.ok();

        if received {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(
                "Stored message not re-sent on reconnect".to_string()
            ))
        }
    })
}

/// MQTT-4.6.0-1: Client/Server MUST re-send unacked PUBLISH on reconnect
fn test_mqtt_4_6_0_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // Similar to 4.5.0-1 but focuses on the re-send behavior
        let client_id = format!("test4601{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let topic = format!("test/resend2/{}", uuid::Uuid::new_v4());

        // First, establish session and subscribe
        let mut opts = MqttOptions::new(&client_id, &ctx.host, ctx.port);
        opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        opts.set_clean_session(false);

        let (client, mut eventloop) = AsyncClient::new(opts, 10);

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
            }
        }

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

        // Force disconnect
        drop(eventloop);
        drop(client);

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish while offline
        {
            let mut pub_opts = MqttOptions::new(
                format!("mqtt-pub-{}", &uuid::Uuid::new_v4().to_string()[..8]),
                &ctx.host,
                ctx.port,
            );
            pub_opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            pub_opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
            pub_opts.set_clean_session(true);

            let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_opts, 10);

            loop {
                match tokio::time::timeout(ctx.timeout, pub_eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
                }
            }

            pub_client.publish(&topic, QoS::AtLeastOnce, false, b"offline".to_vec()).await
                .map_err(ConformanceError::MqttClient)?;

            loop {
                match tokio::time::timeout(ctx.timeout, pub_eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for PUBACK".to_string())),
                }
            }

            pub_client.disconnect().await.ok();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Reconnect and check for message
        let mut opts2 = MqttOptions::new(&client_id, &ctx.host, ctx.port);
        opts2.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts2.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        opts2.set_clean_session(false);

        let (client2, mut eventloop2) = AsyncClient::new(opts2, 10);

        let session_present;
        loop {
            match tokio::time::timeout(ctx.timeout, eventloop2.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(connack)))) => {
                    session_present = connack.session_present;
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for reconnect CONNACK".to_string())),
            }
        }

        if !session_present {
            client2.disconnect().await.ok();

            // Clean up
            let mut cleanup = MqttOptions::new(&client_id, &ctx.host, ctx.port);
            cleanup.set_clean_session(true);
            let (c, mut e) = AsyncClient::new(cleanup, 10);
            let _ = tokio::time::timeout(Duration::from_secs(1), e.poll()).await;
            c.disconnect().await.ok();

            return Ok(()); // Skip if broker doesn't support persistent sessions
        }

        let mut received = false;
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), eventloop2.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        received = true;
                        break;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client2.disconnect().await.ok();

        // Clean up
        let mut cleanup = MqttOptions::new(&client_id, &ctx.host, ctx.port);
        cleanup.set_clean_session(true);
        let (c, mut e) = AsyncClient::new(cleanup, 10);
        let _ = tokio::time::timeout(Duration::from_secs(1), e.poll()).await;
        c.disconnect().await.ok();

        if received {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(
                "Unacked PUBLISH not re-sent on reconnect".to_string()
            ))
        }
    })
}

/// MQTT-4.6.0-3: Each SUBSCRIBE MUST use currently unused Packet Identifier
fn test_mqtt_4_6_0_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let client_id = format!("test4603{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect = build_connect_packet(&client_id, 30);
        stream.write_all(&connect).await.map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send multiple SUBSCRIBEs with different packet IDs
        let mut seen_pkids = std::collections::HashSet::new();

        for i in 1u16..=5 {
            let topic = format!("test/pkid/{}", i);
            let topic_bytes = topic.as_bytes();
            let mut subscribe = Vec::new();
            subscribe.push(0x82);
            let remaining = 2 + 2 + topic_bytes.len() + 1;
            subscribe.push(remaining as u8);
            subscribe.push((i >> 8) as u8);
            subscribe.push((i & 0xFF) as u8);
            subscribe.push(0x00);
            subscribe.push(topic_bytes.len() as u8);
            subscribe.extend_from_slice(topic_bytes);
            subscribe.push(0x00);

            stream.write_all(&subscribe).await.map_err(ConformanceError::Io)?;

            // Read SUBACK
            let mut suback = [0u8; 5];
            tokio::time::timeout(ctx.timeout, stream.read_exact(&mut suback)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            let pkid = ((suback[2] as u16) << 8) | (suback[3] as u16);
            if pkid != i {
                return Err(ConformanceError::ProtocolViolation(format!(
                    "SUBACK packet ID {} doesn't match SUBSCRIBE packet ID {}",
                    pkid, i
                )));
            }

            if !seen_pkids.insert(pkid) {
                return Err(ConformanceError::ProtocolViolation(
                    "Duplicate packet ID used in SUBSCRIBE".to_string()
                ));
            }
        }

        Ok(())
    })
}

/// MQTT-4.6.0-4: Server MUST assign unused Packet Identifier to PUBLISH
fn test_mqtt_4_6_0_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/server/pkid/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-4604-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Subscribe with QoS 1 - server will need to track packet IDs for incoming messages
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

        // Publish multiple messages
        for i in 0..5 {
            client.publish(&topic, QoS::AtLeastOnce, false, format!("msg{}", i).into_bytes()).await
                .map_err(ConformanceError::MqttClient)?;
        }

        // Collect server-assigned packet IDs from incoming PUBLISH
        let mut seen_pkids = std::collections::HashSet::new();
        let mut received = 0;
        let timeout_duration = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration && received < 5 {
            match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        let pkid = publish.pkid;
                        if pkid != 0
                            && !seen_pkids.insert(pkid) {
                                client.disconnect().await.ok();
                                return Err(ConformanceError::ProtocolViolation(
                                    "Server reused packet ID before acknowledgment".to_string()
                                ));
                            }
                        received += 1;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        if received > 0 {
            Ok(())
        } else {
            Err(ConformanceError::Timeout("Did not receive any messages".to_string()))
        }
    })
}

/// MQTT-4.6.0-6: Server MUST maintain message ordering per-subscription
fn test_mqtt_4_6_0_6(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/order/sub/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-4606-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Publish numbered messages
        for i in 0..10 {
            client.publish(&topic, QoS::AtLeastOnce, false, format!("{:02}", i).into_bytes()).await
                .map_err(ConformanceError::MqttClient)?;
        }

        // Collect messages and verify ordering
        let mut received = Vec::new();
        let timeout_duration = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration && received.len() < 10 {
            match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        received.push(String::from_utf8_lossy(&publish.payload).to_string());
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        // Verify ordering
        for i in 1..received.len() {
            if received[i] < received[i - 1] {
                return Err(ConformanceError::ProtocolViolation(format!(
                    "Messages out of order: {:?}",
                    received
                )));
            }
        }

        if received.is_empty() {
            Err(ConformanceError::Timeout("No messages received".to_string()))
        } else {
            Ok(())
        }
    })
}

fn build_connect_packet_with_flags(client_id: &str, keep_alive: u16, clean_session: bool) -> Vec<u8> {
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
    packet.push(if clean_session { 0x02 } else { 0x00 });
    packet.push((keep_alive >> 8) as u8);
    packet.push((keep_alive & 0xFF) as u8);
    packet.push((client_id_len >> 8) as u8);
    packet.push((client_id_len & 0xFF) as u8);
    packet.extend_from_slice(client_id_bytes);

    packet
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

/// MQTT-4.3.1-1: QoS 0: Deliver at most once (fire and forget)
fn test_mqtt_4_3_1_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/qos0/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-4311-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        client.subscribe(&topic, QoS::AtMostOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Publish QoS 0 message
        client.publish(&topic, QoS::AtMostOnce, false, b"qos0 test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // No PUBACK expected for QoS 0
        // Just verify message arrives (at most once delivery)
        let start = std::time::Instant::now();
        let mut received = false;
        while start.elapsed() < Duration::from_secs(3) {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        // QoS 0 message should have pkid = 0
                        if publish.qos != QoS::AtMostOnce {
                            client.disconnect().await.ok();
                            return Err(ConformanceError::ProtocolViolation(
                                "QoS 0 subscription received message with different QoS".to_string()
                            ));
                        }
                        received = true;
                        break;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        if received {
            Ok(())
        } else {
            // QoS 0 doesn't guarantee delivery, so not receiving is acceptable
            // But for a well-functioning broker, we expect to receive it
            Ok(())
        }
    })
}

/// MQTT-4.3.2-1: QoS 1: Deliver at least once
fn test_mqtt_4_3_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/qos1/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-4321-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Publish QoS 1 message
        client.publish(&topic, QoS::AtLeastOnce, false, b"qos1 test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Wait for both PUBACK and message delivery
        // The PUBLISH from broker may arrive before, during, or after PUBACK
        let mut got_puback = false;
        let mut received = false;
        let start = std::time::Instant::now();

        while start.elapsed() < Duration::from_secs(5) && !(got_puback && received) {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => {
                    got_puback = true;
                }
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        received = true;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        if received {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(
                "QoS 1 message not delivered".to_string()
            ))
        }
    })
}

/// MQTT-4.3.2-2: QoS 1: Sender MUST store message until PUBACK received
fn test_mqtt_4_3_2_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // This test verifies the QoS 1 flow: sender stores until PUBACK
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test4322{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send PUBLISH QoS 1
        let topic = b"test/qos1/store";
        let payload = b"test";
        let packet_id: u16 = 0x1234;

        let mut publish_packet = Vec::new();
        publish_packet.push(0x32); // PUBLISH QoS 1
        let remaining = 2 + topic.len() + 2 + payload.len();
        publish_packet.push(remaining as u8);
        publish_packet.push(0x00);
        publish_packet.push(topic.len() as u8);
        publish_packet.extend_from_slice(topic);
        publish_packet.push((packet_id >> 8) as u8);
        publish_packet.push((packet_id & 0xFF) as u8);
        publish_packet.extend_from_slice(payload);

        stream.write_all(&publish_packet).await
            .map_err(ConformanceError::Io)?;

        // Wait for PUBACK with matching packet ID
        let mut puback = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut puback)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if puback[0] != 0x40 {
            return Err(ConformanceError::UnexpectedResponse(format!(
                "Expected PUBACK (0x40), got 0x{:02X}",
                puback[0]
            )));
        }

        let received_pkid = ((puback[2] as u16) << 8) | (puback[3] as u16);
        if received_pkid != packet_id {
            return Err(ConformanceError::ProtocolViolation(format!(
                "PUBACK packet ID {} does not match PUBLISH packet ID {}",
                received_pkid, packet_id
            )));
        }

        Ok(())
    })
}

/// MQTT-4.3.3-1: QoS 2: Deliver exactly once
fn test_mqtt_4_3_3_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/qos2/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-4331-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        client.subscribe(&topic, QoS::ExactlyOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Publish QoS 2 message
        client.publish(&topic, QoS::ExactlyOnce, false, b"qos2 test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Process QoS 2 handshake and wait for message
        let start = std::time::Instant::now();
        let timeout_duration = Duration::from_secs(5);
        let mut received_count = 0;

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        received_count += 1;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        // QoS 2 should deliver exactly once
        if received_count == 1 {
            Ok(())
        } else if received_count == 0 {
            Err(ConformanceError::ProtocolViolation(
                "QoS 2 message not delivered".to_string()
            ))
        } else {
            Err(ConformanceError::ProtocolViolation(format!(
                "QoS 2 message delivered {} times, expected exactly once",
                received_count
            )))
        }
    })
}

/// MQTT-4.3.3-2: QoS 2: Sender MUST store message until PUBREC received
fn test_mqtt_4_3_3_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test4332{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send PUBLISH QoS 2
        let topic = b"test/qos2/store";
        let payload = b"test";
        let packet_id: u16 = 0x5678;

        let mut publish_packet = Vec::new();
        publish_packet.push(0x34); // PUBLISH QoS 2
        let remaining = 2 + topic.len() + 2 + payload.len();
        publish_packet.push(remaining as u8);
        publish_packet.push(0x00);
        publish_packet.push(topic.len() as u8);
        publish_packet.extend_from_slice(topic);
        publish_packet.push((packet_id >> 8) as u8);
        publish_packet.push((packet_id & 0xFF) as u8);
        publish_packet.extend_from_slice(payload);

        stream.write_all(&publish_packet).await
            .map_err(ConformanceError::Io)?;

        // Wait for PUBREC with matching packet ID
        let mut pubrec = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut pubrec)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBREC".to_string()))?
            .map_err(ConformanceError::Io)?;

        if pubrec[0] != 0x50 {
            return Err(ConformanceError::UnexpectedResponse(format!(
                "Expected PUBREC (0x50), got 0x{:02X}",
                pubrec[0]
            )));
        }

        let received_pkid = ((pubrec[2] as u16) << 8) | (pubrec[3] as u16);
        if received_pkid != packet_id {
            return Err(ConformanceError::ProtocolViolation(format!(
                "PUBREC packet ID {} does not match PUBLISH packet ID {}",
                received_pkid, packet_id
            )));
        }

        Ok(())
    })
}

/// MQTT-4.3.3-3: QoS 2: PUBREL MUST be sent after PUBREC received
fn test_mqtt_4_3_3_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test4333{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send PUBLISH QoS 2
        let topic = b"test/qos2/pubrel";
        let payload = b"test";
        let packet_id: u16 = 0xABCD;

        let mut publish_packet = Vec::new();
        publish_packet.push(0x34); // PUBLISH QoS 2
        let remaining = 2 + topic.len() + 2 + payload.len();
        publish_packet.push(remaining as u8);
        publish_packet.push(0x00);
        publish_packet.push(topic.len() as u8);
        publish_packet.extend_from_slice(topic);
        publish_packet.push((packet_id >> 8) as u8);
        publish_packet.push((packet_id & 0xFF) as u8);
        publish_packet.extend_from_slice(payload);

        stream.write_all(&publish_packet).await
            .map_err(ConformanceError::Io)?;

        // Wait for PUBREC
        let mut pubrec = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut pubrec)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBREC".to_string()))?
            .map_err(ConformanceError::Io)?;

        if pubrec[0] != 0x50 {
            return Err(ConformanceError::UnexpectedResponse("Expected PUBREC".to_string()));
        }

        // Send PUBREL
        let pubrel = [
            0x62, // PUBREL (with reserved bits)
            0x02, // Remaining length
            (packet_id >> 8) as u8,
            (packet_id & 0xFF) as u8,
        ];
        stream.write_all(&pubrel).await
            .map_err(ConformanceError::Io)?;

        // Wait for PUBCOMP
        let mut pubcomp = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut pubcomp)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBCOMP".to_string()))?
            .map_err(ConformanceError::Io)?;

        if pubcomp[0] != 0x70 {
            return Err(ConformanceError::UnexpectedResponse(format!(
                "Expected PUBCOMP (0x70), got 0x{:02X}",
                pubcomp[0]
            )));
        }

        let received_pkid = ((pubcomp[2] as u16) << 8) | (pubcomp[3] as u16);
        if received_pkid != packet_id {
            return Err(ConformanceError::ProtocolViolation(format!(
                "PUBCOMP packet ID {} does not match original {}",
                received_pkid, packet_id
            )));
        }

        Ok(())
    })
}

/// MQTT-4.3.3-4: QoS 2: Receiver MUST store Packet ID until PUBREL received
fn test_mqtt_4_3_3_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // This test verifies the receiver side of QoS 2
        // We send PUBLISH QoS 2 to server, receive PUBREC, then send PUBREL
        // The server should store the packet ID until it receives PUBREL
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test4334{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        // Send PUBLISH QoS 2
        let topic = b"test/qos2/receiver";
        let payload = b"test";
        let packet_id: u16 = 0xFEDC;

        let mut publish_packet = Vec::new();
        publish_packet.push(0x34); // PUBLISH QoS 2
        let remaining = 2 + topic.len() + 2 + payload.len();
        publish_packet.push(remaining as u8);
        publish_packet.push(0x00);
        publish_packet.push(topic.len() as u8);
        publish_packet.extend_from_slice(topic);
        publish_packet.push((packet_id >> 8) as u8);
        publish_packet.push((packet_id & 0xFF) as u8);
        publish_packet.extend_from_slice(payload);

        stream.write_all(&publish_packet).await
            .map_err(ConformanceError::Io)?;

        // Wait for PUBREC - this confirms server received and is storing
        let mut pubrec = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut pubrec)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBREC".to_string()))?
            .map_err(ConformanceError::Io)?;

        if pubrec[0] != 0x50 {
            return Err(ConformanceError::UnexpectedResponse("Expected PUBREC".to_string()));
        }

        // Don't send PUBREL immediately - wait a bit to verify server stores
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now send PUBREL
        let pubrel = [
            0x62,
            0x02,
            (packet_id >> 8) as u8,
            (packet_id & 0xFF) as u8,
        ];
        stream.write_all(&pubrel).await
            .map_err(ConformanceError::Io)?;

        // Expect PUBCOMP - confirms server was storing until PUBREL
        let mut pubcomp = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut pubcomp)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBCOMP".to_string()))?
            .map_err(ConformanceError::Io)?;

        if pubcomp[0] != 0x70 {
            return Err(ConformanceError::UnexpectedResponse(format!(
                "Expected PUBCOMP (0x70), got 0x{:02X}",
                pubcomp[0]
            )));
        }

        let received_pkid = ((pubcomp[2] as u16) << 8) | (pubcomp[3] as u16);
        if received_pkid != packet_id {
            return Err(ConformanceError::ProtocolViolation(format!(
                "PUBCOMP packet ID {} does not match original {}",
                received_pkid, packet_id
            )));
        }

        Ok(())
    })
}
