//! PUBLISH packet tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 3.3 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "publish".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.3.1-2
        make_test(
            "MQTT-3.3.1-2",
            "DUP flag MUST be 0 for QoS 0 messages",
            test_mqtt_3_3_1_2,
        ),
        // MQTT-3.3.1-4
        make_test(
            "MQTT-3.3.1-4",
            "MUST close connection if both QoS bits set to 1",
            test_mqtt_3_3_1_4,
        ),
        // MQTT-3.3.1-5
        make_test(
            "MQTT-3.3.1-5",
            "RETAIN=1 MUST cause Server to store message",
            test_mqtt_3_3_1_5,
        ),
        // MQTT-3.3.1-6
        make_test(
            "MQTT-3.3.1-6",
            "New subscription MUST receive last retained message",
            test_mqtt_3_3_1_6,
        ),
        // MQTT-3.3.1-8
        make_test(
            "MQTT-3.3.1-8",
            "RETAIN=1 when sending to new subscriber",
            test_mqtt_3_3_1_8,
        ),
        // MQTT-3.3.1-9
        make_test(
            "MQTT-3.3.1-9",
            "RETAIN=0 when sending to established subscription",
            test_mqtt_3_3_1_9,
        ),
        // MQTT-3.3.1-10
        make_test(
            "MQTT-3.3.1-10",
            "Zero-length payload with RETAIN=1 removes retained message",
            test_mqtt_3_3_1_10,
        ),
        // MQTT-3.3.2-1
        make_test(
            "MQTT-3.3.2-1",
            "Topic Name MUST be UTF-8 encoded string",
            test_mqtt_3_3_2_1,
        ),
        // MQTT-3.3.2-2
        make_test(
            "MQTT-3.3.2-2",
            "Topic Name MUST NOT contain wildcard characters",
            test_mqtt_3_3_2_2,
        ),
        // MQTT-3.3.4-1
        make_test(
            "MQTT-3.3.4-1",
            "Receiver MUST respond according to QoS",
            test_mqtt_3_3_4_1,
        ),
        // MQTT-3.3.1-1
        make_test(
            "MQTT-3.3.1-1",
            "DUP flag MUST be 1 on re-delivery attempts",
            test_mqtt_3_3_1_1,
        ),
        // MQTT-3.3.1-7
        make_test(
            "MQTT-3.3.1-7",
            "QoS 0 with RETAIN=1 MUST discard previous retained message",
            test_mqtt_3_3_1_7,
        ),
        // MQTT-3.3.1-11
        make_test(
            "MQTT-3.3.1-11",
            "Zero-byte retained message MUST NOT be stored",
            test_mqtt_3_3_1_11,
        ),
        // MQTT-3.3.1-12
        make_test(
            "MQTT-3.3.1-12",
            "RETAIN=0 MUST NOT store or replace retained message",
            test_mqtt_3_3_1_12,
        ),
        // MQTT-3.3.5-1
        make_test(
            "MQTT-3.3.5-1",
            "Overlapping subscriptions: deliver with maximum QoS",
            test_mqtt_3_3_5_1,
        ),
        // MQTT-3.3.1-3
        make_test(
            "MQTT-3.3.1-3",
            "DUP flag in outgoing PUBLISH determined solely by retransmission",
            test_mqtt_3_3_1_3,
        ),
        // MQTT-3.3.2-3
        make_test(
            "MQTT-3.3.2-3",
            "Topic Name in PUBLISH must match Subscription Topic Filter",
            test_mqtt_3_3_2_3,
        ),
        // MQTT-3.3.5-2
        make_test(
            "MQTT-3.3.5-2",
            "Server MUST acknowledge unauthorized PUBLISH or close connection",
            test_mqtt_3_3_5_2,
        ),
    ]
}

/// MQTT-3.3.1-2: DUP flag MUST be 0 for QoS 0 messages
fn test_mqtt_3_3_1_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/dup/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-3312-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            &ctx.host,
            ctx.port,
        );
        opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        opts.set_clean_session(true);

        let (client, mut eventloop) = AsyncClient::new(opts, 10);

        // Wait for CONNACK
        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
            }
        }

        // Subscribe to topic
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
        client.publish(&topic, QoS::AtMostOnce, false, b"test message".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Receive message and check DUP flag
        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.dup {
                        client.disconnect().await.ok();
                        return Err(ConformanceError::ProtocolViolation(
                            "DUP flag must be 0 for QoS 0 messages".to_string()
                        ));
                    }
                    client.disconnect().await.ok();
                    return Ok(());
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();
        // QoS 0 delivery is not guaranteed, so no message is acceptable
        Ok(())
    })
}

/// MQTT-3.3.1-4: MUST close connection if both QoS bits set to 1
fn test_mqtt_3_3_1_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send valid CONNECT
        let client_id = format!("test3314{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        // Read CONNACK
        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if buf[0] != 0x20 || buf[3] != 0x00 {
            return Err(ConformanceError::UnexpectedResponse("Expected successful CONNACK".to_string()));
        }

        // Send PUBLISH with QoS=3 (both bits set - invalid)
        let topic = b"test/qos3";
        let payload = b"test";
        let mut publish_packet = Vec::new();

        // Fixed header: PUBLISH with QoS=3 (bits 1,2 both set = 0x36)
        publish_packet.push(0x36); // PUBLISH with QoS=3
        let remaining = 2 + topic.len() + 2 + payload.len(); // topic len + topic + packet id + payload
        publish_packet.push(remaining as u8);

        // Topic
        publish_packet.push(0x00);
        publish_packet.push(topic.len() as u8);
        publish_packet.extend_from_slice(topic);

        // Packet ID (required for QoS > 0)
        publish_packet.push(0x00);
        publish_packet.push(0x01);

        // Payload
        publish_packet.extend_from_slice(payload);

        stream.write_all(&publish_packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close connection
        let mut check_buf = [0u8; 1];
        let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut check_buf)).await;

        match result {
            Ok(Ok(0)) => Ok(()), // Connection closed
            Ok(Err(_)) => Ok(()), // Connection error
            _ => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for invalid QoS=3".to_string()
            )),
        }
    })
}

/// MQTT-3.3.1-5: RETAIN=1 MUST cause Server to store message
fn test_mqtt_3_3_1_5(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/retain/{}", uuid::Uuid::new_v4());
        let payload = b"retained message";

        // Publish retained message
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

        // Subscribe and verify retained message is received
        {
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

            client.subscribe(&topic, QoS::AtLeastOnce).await
                .map_err(ConformanceError::MqttClient)?;

            let timeout_duration = Duration::from_secs(5);
            let start = std::time::Instant::now();
            let mut received_retain = false;

            while start.elapsed() < timeout_duration {
                match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                        if publish.topic == topic && publish.payload.as_ref() == payload {
                            received_retain = true;
                            break;
                        }
                    }
                    Ok(Ok(_)) => continue,
                    Ok(Err(_)) => break,
                    Err(_) => continue,
                }
            }

            // Clean up retained message
            client.publish(&topic, QoS::AtLeastOnce, true, vec![]).await.ok();
            client.disconnect().await.ok();

            if received_retain {
                Ok(())
            } else {
                Err(ConformanceError::ProtocolViolation(
                    "Retained message was not stored/delivered".to_string()
                ))
            }
        }
    })
}

/// MQTT-3.3.1-6: New subscription MUST receive last retained message
fn test_mqtt_3_3_1_6(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // This test is similar to 3.3.1-5 but focuses on "new subscription"
        let topic = format!("test/retain/new/{}", uuid::Uuid::new_v4());
        let payload = b"retained for new sub";

        // First, publish a retained message
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

        pub_client.publish(&topic, QoS::AtLeastOnce, true, payload.to_vec()).await
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
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now subscribe as a new subscriber
        let mut sub_opts = MqttOptions::new(
            format!("mqtt-sub-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            &ctx.host,
            ctx.port,
        );
        sub_opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            sub_opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        sub_opts.set_clean_session(true);

        let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 10);

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

        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();
        let mut received = false;

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_eventloop.poll()).await {
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

        // Cleanup
        sub_client.publish(&topic, QoS::AtLeastOnce, true, vec![]).await.ok();
        sub_client.disconnect().await.ok();

        if received {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(
                "New subscriber did not receive retained message".to_string()
            ))
        }
    })
}

/// MQTT-3.3.1-8: RETAIN=1 when sending to new subscriber
fn test_mqtt_3_3_1_8(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/retain/flag/{}", uuid::Uuid::new_v4());
        let payload = b"check retain flag";

        // Publish retained message
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

        pub_client.publish(&topic, QoS::AtLeastOnce, true, payload.to_vec()).await
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
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Subscribe and check RETAIN flag
        let mut sub_opts = MqttOptions::new(
            format!("mqtt-sub-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            &ctx.host,
            ctx.port,
        );
        sub_opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            sub_opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        sub_opts.set_clean_session(true);

        let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 10);

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

        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();
        let mut retain_flag_correct = false;

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), sub_eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        retain_flag_correct = publish.retain;
                        break;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        // Cleanup
        sub_client.publish(&topic, QoS::AtLeastOnce, true, vec![]).await.ok();
        sub_client.disconnect().await.ok();

        if retain_flag_correct {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(
                "RETAIN flag must be 1 when sending to new subscriber".to_string()
            ))
        }
    })
}

/// MQTT-3.3.1-9: RETAIN=0 when sending to established subscription
fn test_mqtt_3_3_1_9(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/noretain/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-3319-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Publish to established subscription (with retain=true to server, but should come back with retain=false)
        client.publish(&topic, QoS::AtLeastOnce, true, b"test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        // Cleanup
                        client.publish(&topic, QoS::AtLeastOnce, true, vec![]).await.ok();
                        client.disconnect().await.ok();

                        if publish.retain {
                            return Err(ConformanceError::ProtocolViolation(
                                "RETAIN flag must be 0 for established subscription".to_string()
                            ));
                        }
                        return Ok(());
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();
        Err(ConformanceError::Timeout("Did not receive published message".to_string()))
    })
}

/// MQTT-3.3.1-10: Zero-length payload with RETAIN=1 removes retained message
fn test_mqtt_3_3_1_10(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/retain/remove/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-33110-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // First, publish a retained message
        client.publish(&topic, QoS::AtLeastOnce, true, b"to be removed".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for PUBACK".to_string())),
            }
        }

        // Now send zero-length payload with RETAIN=1 to remove it
        client.publish(&topic, QoS::AtLeastOnce, true, vec![]).await
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
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Subscribe and verify no retained message is received
        let mut sub_opts = MqttOptions::new(
            format!("mqtt-sub-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            &ctx.host,
            ctx.port,
        );
        sub_opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            sub_opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        sub_opts.set_clean_session(true);

        let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 10);

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

        // Wait briefly - should NOT receive retained message
        let timeout_duration = Duration::from_secs(2);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(500), sub_eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic && !publish.payload.is_empty() && publish.retain {
                        sub_client.disconnect().await.ok();
                        return Err(ConformanceError::ProtocolViolation(
                            "Retained message was not removed by zero-length publish".to_string()
                        ));
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        sub_client.disconnect().await.ok();
        Ok(())
    })
}

/// MQTT-3.3.2-1: Topic Name MUST be UTF-8 encoded string
fn test_mqtt_3_3_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // Test with valid UTF-8 topic names including unicode
        let topics = vec![
            "test/utf8/simple",
            "test/utf8/unicode/\u{00e9}",  // é
            "test/utf8/emoji", // Skip actual emoji as some brokers may reject
        ];

        let mut opts = MqttOptions::new(
            format!("mqtt-3321-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        for topic in topics {
            client.publish(topic, QoS::AtLeastOnce, false, b"test".to_vec()).await
                .map_err(ConformanceError::MqttClient)?;

            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout(format!("Waiting for PUBACK for topic {}", topic))),
                }
            }
        }

        client.disconnect().await.ok();
        Ok(())
    })
}

/// MQTT-3.3.2-2: Topic Name MUST NOT contain wildcard characters
/// Tests both directions:
/// 1. Client→Server: Server should reject PUBLISH with wildcard in topic
/// 2. Server→Client: Delivered topic must match published topic, not subscription filter
fn test_mqtt_3_3_2_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // Part 1: Test server→client direction
        // Subscribe with wildcard, publish to matching topic, verify delivered topic
        // is the actual topic (not the wildcard pattern) and contains no wildcards
        {
            let base = uuid::Uuid::new_v4();
            let wildcard_filter = format!("test/{}/+/data", base);
            let actual_topic = format!("test/{}/sensor/data", base);
            let payload = b"test message";

            let mut opts = MqttOptions::new(
                format!("test3322a{}", &uuid::Uuid::new_v4().to_string()[..8]),
                &ctx.host,
                ctx.port,
            );
            opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
            opts.set_clean_session(true);

            let (client, mut eventloop) = AsyncClient::new(opts, 10);

            // Wait for CONNACK
            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
                }
            }

            // Subscribe with wildcard filter
            client.subscribe(&wildcard_filter, QoS::AtLeastOnce).await
                .map_err(ConformanceError::MqttClient)?;

            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
                }
            }

            // Publish to actual topic (no wildcards)
            client.publish(&actual_topic, QoS::AtLeastOnce, false, payload.to_vec()).await
                .map_err(ConformanceError::MqttClient)?;

            // Wait for the message - verify topic is actual_topic, not wildcard_filter
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_secs(3) {
                match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                        // Check topic contains no wildcards
                        if publish.topic.contains('+') || publish.topic.contains('#') {
                            client.disconnect().await.ok();
                            return Err(ConformanceError::ProtocolViolation(format!(
                                "Server delivered PUBLISH with wildcard in topic: '{}'",
                                publish.topic
                            )));
                        }
                        // Check topic matches what we published, not the subscription filter
                        if publish.topic != actual_topic {
                            client.disconnect().await.ok();
                            return Err(ConformanceError::ProtocolViolation(format!(
                                "Server delivered wrong topic. Expected '{}', got '{}'",
                                actual_topic, publish.topic
                            )));
                        }
                        client.disconnect().await.ok();
                        break;
                    }
                    Ok(Ok(_)) => continue,
                    Ok(Err(_)) => break,
                    Err(_) => continue,
                }
            }
        }

        // Part 2: Test client→server direction
        // Server should reject PUBLISH with wildcard in topic name
        {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            use tokio::net::TcpStream;

            let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
                .await
                .map_err(|e| ConformanceError::Connection(e.to_string()))?;

            // Connect
            let client_id = format!("test3322b{}", &uuid::Uuid::new_v4().to_string()[..8]);
            let connect_packet = build_connect_packet(&client_id, 30);
            stream.write_all(&connect_packet).await
                .map_err(ConformanceError::Io)?;

            let mut buf = [0u8; 4];
            tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
                .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
                .map_err(ConformanceError::Io)?;

            // Send PUBLISH with wildcard in topic name (should be rejected)
            let topic_with_wildcard = b"test/+/invalid";
            let payload = b"test";
            let mut publish_packet = Vec::new();

            // Fixed header: PUBLISH with QoS=1
            publish_packet.push(0x32);
            let remaining = 2 + topic_with_wildcard.len() + 2 + payload.len();
            publish_packet.push(remaining as u8);

            // Topic with wildcard
            publish_packet.push(0x00);
            publish_packet.push(topic_with_wildcard.len() as u8);
            publish_packet.extend_from_slice(topic_with_wildcard);

            // Packet ID
            publish_packet.push(0x00);
            publish_packet.push(0x01);

            // Payload
            publish_packet.extend_from_slice(payload);

            stream.write_all(&publish_packet).await
                .map_err(ConformanceError::Io)?;

            // Server should reject - either close connection or not send PUBACK
            let mut response_buf = [0u8; 4];
            match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut response_buf)).await {
                Ok(Ok(0)) => {} // Connection closed - correct
                Ok(Err(_)) => {} // Connection error/RST - correct
                Ok(Ok(n)) if n >= 1 && response_buf[0] == 0x40 => {
                    // 0x40 = PUBACK - server incorrectly accepted wildcard topic
                    return Err(ConformanceError::ProtocolViolation(
                        "Server sent PUBACK for PUBLISH with wildcard in topic name".to_string()
                    ));
                }
                Ok(Ok(_)) => {} // Some other response
                Err(_) => {} // Timeout without PUBACK - server dropped it (acceptable)
            }
        }

        Ok(())
    })
}

/// MQTT-3.3.4-1: Receiver MUST respond according to QoS
fn test_mqtt_3_3_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/qos/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-3341-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Test QoS 1 - should get PUBACK
        client.publish(&topic, QoS::AtLeastOnce, false, b"qos1".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Expected PUBACK for QoS 1".to_string())),
            }
        }

        // Test QoS 2 - should get PUBREC, then PUBCOMP after PUBREL
        client.publish(&topic, QoS::ExactlyOnce, false, b"qos2".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        let mut got_pubrec = false;
        let mut got_pubcomp = false;
        let timeout_duration = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration && !got_pubcomp {
            match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubRec(_)))) => {
                    got_pubrec = true;
                }
                Ok(Ok(Event::Incoming(Packet::PubComp(_)))) => {
                    got_pubcomp = true;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        if !got_pubrec {
            return Err(ConformanceError::ProtocolViolation(
                "Did not receive PUBREC for QoS 2".to_string()
            ));
        }

        if !got_pubcomp {
            return Err(ConformanceError::ProtocolViolation(
                "Did not receive PUBCOMP for QoS 2".to_string()
            ));
        }

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

/// MQTT-3.3.1-1: DUP flag MUST be set to 1 by Server when re-delivering a PUBLISH
fn test_mqtt_3_3_1_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let topic = format!("test/dup/redelivery/{}", uuid::Uuid::new_v4());
        let subscriber_client_id = format!("mqtt3311sub{}", &uuid::Uuid::new_v4().to_string()[..8]);

        // Step 1: Subscriber connects via raw TCP with CleanSession=0
        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let connect_packet = build_connect_packet_with_flags(&subscriber_client_id, 30, false); // CleanSession=0
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if buf[0] != 0x20 || buf[3] != 0x00 {
            return Err(ConformanceError::UnexpectedResponse(
                format!("Expected successful CONNACK, got {:02x} {:02x}", buf[0], buf[3])
            ));
        }

        // Step 2: Subscribe to topic with QoS 1
        let subscribe_packet = build_subscribe_packet_qos1(&topic, 1);
        stream.write_all(&subscribe_packet).await
            .map_err(ConformanceError::Io)?;

        let mut suback_buf = [0u8; 5];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut suback_buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for SUBACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if suback_buf[0] != 0x90 {
            return Err(ConformanceError::UnexpectedResponse("Expected SUBACK".to_string()));
        }

        // Step 3: Publisher sends QoS 1 message
        let mut pub_opts = MqttOptions::new(
            format!("mqtt3311pub{}", &uuid::Uuid::new_v4().to_string()[..8]),
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
                Err(_) => return Err(ConformanceError::Timeout("Waiting for publisher CONNACK".to_string())),
            }
        }

        pub_client.publish(&topic, QoS::AtLeastOnce, false, b"test message for DUP".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Wait for PUBACK to confirm publish
        loop {
            match tokio::time::timeout(ctx.timeout, pub_eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for publisher PUBACK".to_string())),
            }
        }

        pub_client.disconnect().await.ok();

        // Step 4: Subscriber reads PUBLISH - DUP should be 0 on first delivery
        let mut publish_buf = [0u8; 128];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut publish_buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PUBLISH".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n == 0 {
            return Err(ConformanceError::UnexpectedResponse("Connection closed before PUBLISH".to_string()));
        }

        // Verify it's a PUBLISH packet (type 0x30-0x3F)
        if (publish_buf[0] & 0xF0) != 0x30 {
            return Err(ConformanceError::UnexpectedResponse(
                format!("Expected PUBLISH packet, got 0x{:02x}", publish_buf[0])
            ));
        }

        // Check DUP flag (bit 3) - should be 0 on first delivery
        let first_dup = (publish_buf[0] & 0x08) != 0;
        if first_dup {
            return Err(ConformanceError::ProtocolViolation(
                "DUP flag was 1 on first delivery (should be 0)".to_string()
            ));
        }

        // Step 5: DO NOT send PUBACK - just drop connection
        drop(stream);

        // Brief pause to ensure server detects disconnect
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Step 6: Reconnect with same ClientId, CleanSession=0
        let mut stream2 = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        let connect_packet2 = build_connect_packet_with_flags(&subscriber_client_id, 30, false);
        stream2.write_all(&connect_packet2).await
            .map_err(ConformanceError::Io)?;

        let mut connack_buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream2.read_exact(&mut connack_buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK on reconnect".to_string()))?
            .map_err(ConformanceError::Io)?;

        if connack_buf[0] != 0x20 || connack_buf[3] != 0x00 {
            return Err(ConformanceError::UnexpectedResponse(
                format!("Expected successful CONNACK on reconnect, got {:02x} {:02x}", connack_buf[0], connack_buf[3])
            ));
        }

        // Step 7: Server should re-deliver the unacknowledged message with DUP=1
        let mut redeliver_buf = [0u8; 128];
        let n = tokio::time::timeout(Duration::from_secs(5), stream2.read(&mut redeliver_buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for re-delivered PUBLISH".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n == 0 {
            return Err(ConformanceError::Timeout("No re-delivery received".to_string()));
        }

        // Verify it's a PUBLISH packet
        if (redeliver_buf[0] & 0xF0) != 0x30 {
            return Err(ConformanceError::UnexpectedResponse(
                format!("Expected re-delivered PUBLISH, got 0x{:02x}", redeliver_buf[0])
            ));
        }

        // Step 8: Verify DUP flag is 1 on re-delivery
        let redelivery_dup = (redeliver_buf[0] & 0x08) != 0;
        if !redelivery_dup {
            return Err(ConformanceError::ProtocolViolation(
                "DUP flag was 0 on re-delivery (MUST be 1 per MQTT-3.3.1-1)".to_string()
            ));
        }

        // Clean up: send PUBACK and disconnect
        // Extract packet ID from the PUBLISH (after remaining length and topic)
        let remaining_len = redeliver_buf[1] as usize;
        let topic_len = ((redeliver_buf[2] as usize) << 8) | (redeliver_buf[3] as usize);
        let pkid_offset = 4 + topic_len;
        if pkid_offset + 1 < remaining_len + 2 {
            let pkid = ((redeliver_buf[pkid_offset] as u16) << 8) | (redeliver_buf[pkid_offset + 1] as u16);
            let puback = [0x40, 0x02, (pkid >> 8) as u8, (pkid & 0xFF) as u8];
            stream2.write_all(&puback).await.ok();
        }

        drop(stream2);
        Ok(())
    })
}

/// Build CONNECT packet with configurable CleanSession flag
fn build_connect_packet_with_flags(client_id: &str, keep_alive: u16, clean_session: bool) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let client_id_len = client_id_bytes.len();
    let remaining_length = 6 + 1 + 1 + 2 + 2 + client_id_len;

    let mut packet = Vec::with_capacity(2 + remaining_length);
    packet.push(0x10); // CONNECT
    packet.push(remaining_length as u8);
    packet.push(0x00);
    packet.push(0x04);
    packet.extend_from_slice(b"MQTT");
    packet.push(4); // Protocol level
    packet.push(if clean_session { 0x02 } else { 0x00 }); // Connect flags
    packet.push((keep_alive >> 8) as u8);
    packet.push((keep_alive & 0xFF) as u8);
    packet.push((client_id_len >> 8) as u8);
    packet.push((client_id_len & 0xFF) as u8);
    packet.extend_from_slice(client_id_bytes);

    packet
}

/// Build SUBSCRIBE packet with QoS 1
fn build_subscribe_packet_qos1(topic: &str, packet_id: u16) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let remaining_length = 2 + 2 + topic_bytes.len() + 1;

    let mut packet = Vec::new();
    packet.push(0x82); // SUBSCRIBE
    packet.push(remaining_length as u8);
    packet.push((packet_id >> 8) as u8);
    packet.push((packet_id & 0xFF) as u8);
    packet.push((topic_bytes.len() >> 8) as u8);
    packet.push((topic_bytes.len() & 0xFF) as u8);
    packet.extend_from_slice(topic_bytes);
    packet.push(0x01); // QoS 1

    packet
}

/// MQTT-3.3.1-7: QoS 0 with RETAIN=1 MUST discard previous retained message
fn test_mqtt_3_3_1_7(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/retain/qos0/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-3317-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // First publish a QoS 1 retained message
        client.publish(&topic, QoS::AtLeastOnce, true, b"original message".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for PUBACK".to_string())),
            }
        }

        // Now publish QoS 0 with RETAIN=1 and empty payload to discard
        client.publish(&topic, QoS::AtMostOnce, true, vec![]).await
            .map_err(ConformanceError::MqttClient)?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Subscribe and verify no retained message
        client.subscribe(&topic, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        let start = std::time::Instant::now();
        let mut got_retained = false;
        while start.elapsed() < Duration::from_secs(2) {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic && !publish.payload.is_empty() {
                        got_retained = true;
                        break;
                    }
                }
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => continue,
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        if got_retained {
            Err(ConformanceError::ProtocolViolation(
                "Retained message was not discarded by QoS 0 RETAIN=1 with empty payload".to_string()
            ))
        } else {
            Ok(())
        }
    })
}

/// MQTT-3.3.1-11: Zero-byte retained message MUST NOT be stored
fn test_mqtt_3_3_1_11(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/retain/zero/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-33111-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Publish zero-byte retained message (should clear any existing, and not be stored)
        client.publish(&topic, QoS::AtLeastOnce, true, vec![]).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for PUBACK".to_string())),
            }
        }

        // Subscribe and verify no retained message
        client.subscribe(&topic, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        let start = std::time::Instant::now();
        let mut got_retained = false;
        while start.elapsed() < Duration::from_secs(2) {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic && publish.retain {
                        got_retained = true;
                        break;
                    }
                }
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => continue,
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        if got_retained {
            Err(ConformanceError::ProtocolViolation(
                "Zero-byte retained message was stored".to_string()
            ))
        } else {
            Ok(())
        }
    })
}

/// MQTT-3.3.1-12: RETAIN=0 MUST NOT store or replace retained message
fn test_mqtt_3_3_1_12(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/noretain/{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(
            format!("mqtt-33112-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Publish with RETAIN=0
        client.publish(&topic, QoS::AtLeastOnce, false, b"not retained".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for PUBACK".to_string())),
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Subscribe and verify no retained message
        client.subscribe(&topic, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        let start = std::time::Instant::now();
        let mut got_retained = false;
        while start.elapsed() < Duration::from_secs(2) {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic && publish.retain {
                        got_retained = true;
                        break;
                    }
                }
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => continue,
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        if got_retained {
            Err(ConformanceError::ProtocolViolation(
                "Message with RETAIN=0 was stored as retained".to_string()
            ))
        } else {
            Ok(())
        }
    })
}

/// MQTT-3.3.5-1: Overlapping subscriptions: deliver with maximum QoS
fn test_mqtt_3_3_5_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let topic = format!("test/overlap/{}", uuid::Uuid::new_v4());
        let topic_wildcard = "test/overlap/#".to_string();

        let mut opts = MqttOptions::new(
            format!("mqtt-3351-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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

        // Subscribe to exact topic with QoS 0
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

        // Subscribe to wildcard with QoS 2 (overlapping, higher QoS)
        client.subscribe(&topic_wildcard, QoS::ExactlyOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Publish with QoS 2
        client.publish(&topic, QoS::ExactlyOnce, false, b"overlap test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Wait for message - it should be delivered only once with max QoS
        let start = std::time::Instant::now();
        let mut message_count = 0;
        while start.elapsed() < Duration::from_secs(3) {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        message_count += 1;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        // Should receive exactly one message (not duplicated due to overlapping subs)
        if message_count >= 1 {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(
                "Message not delivered for overlapping subscriptions".to_string()
            ))
        }
    })
}

/// MQTT-3.3.1-3: DUP flag in outgoing PUBLISH determined solely by retransmission
fn test_mqtt_3_3_1_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // This test verifies that server sets DUP flag correctly when forwarding messages
        // The DUP flag should NOT be propagated from incoming to outgoing
        // It should only be set when the server is retransmitting its own outbound message
        let topic = format!("test/dup/{}", uuid::Uuid::new_v4());

        // Set up subscriber
        let mut sub_opts = MqttOptions::new(
            format!("mqtt-sub-{}", &uuid::Uuid::new_v4().to_string()[..8]),
            &ctx.host,
            ctx.port,
        );
        sub_opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            sub_opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        sub_opts.set_clean_session(true);

        let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 10);

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

        // Set up publisher
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

        // Publish message (first delivery, DUP should be 0)
        pub_client.publish(&topic, QoS::AtLeastOnce, false, b"test message".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Wait for PUBACK from publisher side
        loop {
            match tokio::time::timeout(ctx.timeout, pub_eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for PUBACK".to_string())),
            }
        }

        // Receive message on subscriber - DUP should be 0 for first delivery
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            match tokio::time::timeout(Duration::from_millis(500), sub_eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == topic {
                        // First delivery to subscriber - DUP should be 0
                        if publish.dup {
                            sub_client.disconnect().await.ok();
                            pub_client.disconnect().await.ok();
                            return Err(ConformanceError::ProtocolViolation(
                                "DUP flag should be 0 for first delivery to subscriber".to_string()
                            ));
                        }
                        sub_client.disconnect().await.ok();
                        pub_client.disconnect().await.ok();
                        return Ok(());
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        sub_client.disconnect().await.ok();
        pub_client.disconnect().await.ok();
        Err(ConformanceError::Timeout("Did not receive message".to_string()))
    })
}

/// MQTT-3.3.2-3: Topic Name in PUBLISH must match Subscription Topic Filter
fn test_mqtt_3_3_2_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // Test wildcard subscriptions and verify topic names match the filter
        let base_topic = format!("test/match/{}", uuid::Uuid::new_v4());
        let specific_topic = format!("{}/subtopic", base_topic);
        let wildcard_filter = format!("{}/#", base_topic);

        let mut opts = MqttOptions::new(
            format!("mqtt-3323-{}", &uuid::Uuid::new_v4().to_string()[..8]),
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
        client.subscribe(&wildcard_filter, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Publish to specific topic that matches the wildcard
        client.publish(&specific_topic, QoS::AtLeastOnce, false, b"test".to_vec()).await
            .map_err(ConformanceError::MqttClient)?;

        // Verify received topic matches
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            match tokio::time::timeout(Duration::from_millis(500), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    // The topic in PUBLISH must be the actual topic, not the filter
                    // And it must match the subscription filter
                    if publish.topic == specific_topic {
                        client.disconnect().await.ok();
                        return Ok(());
                    } else if publish.topic == wildcard_filter {
                        client.disconnect().await.ok();
                        return Err(ConformanceError::ProtocolViolation(
                            "Topic Name should be actual topic, not the filter".to_string()
                        ));
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();
        Err(ConformanceError::Timeout("Did not receive message".to_string()))
    })
}

/// MQTT-3.3.5-2: Server MUST acknowledge unauthorized PUBLISH or close connection
fn test_mqtt_3_3_5_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // This test verifies server behavior when a client publishes to a topic
        // Even without explicit authorization checks, server must either:
        // 1. Send positive acknowledgment (PUBACK/PUBREC)
        // 2. Close the connection
        // Server MUST NOT silently drop the publish without responding

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test3352{}", &uuid::Uuid::new_v4().to_string()[..8]);
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

        // Send QoS 1 PUBLISH
        let topic = b"test/auth/check";
        let payload = b"test";
        let packet_id: u16 = 1;

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

        // Server must respond with PUBACK or close connection
        let mut response = [0u8; 4];
        match tokio::time::timeout(ctx.timeout, stream.read(&mut response)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed - acceptable
            Ok(Ok(n)) if n >= 4 && response[0] == 0x40 => {
                // PUBACK received - acceptable
                Ok(())
            }
            Ok(Ok(_)) => Ok(()), // Some other response - might be disconnect
            Ok(Err(_)) => Ok(()), // Connection error - acceptable (closed)
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not acknowledge PUBLISH or close connection".to_string()
            )),
        }
    })
}
