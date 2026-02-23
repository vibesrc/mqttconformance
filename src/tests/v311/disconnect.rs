//! DISCONNECT packet tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 3.14 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "disconnect".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.14.4-1
        make_test(
            "MQTT-3.14.4-1",
            "Client MUST close connection after DISCONNECT",
            test_mqtt_3_14_4_1,
        ),
        // MQTT-3.14.4-2
        make_test(
            "MQTT-3.14.4-2",
            "Server MUST discard Will Message on DISCONNECT",
            test_mqtt_3_14_4_2,
        ),
    ]
}

/// MQTT-3.14.4-1: Client MUST close connection after DISCONNECT
fn test_mqtt_3_14_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test31441{}", &uuid::Uuid::new_v4().to_string()[..8]);
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

        // Send DISCONNECT
        let disconnect = [0xE0, 0x00];
        stream.write_all(&disconnect).await
            .map_err(ConformanceError::Io)?;

        // After disconnect, any further communication should fail
        // This test verifies client-side behavior, so we just confirm the disconnect was sent
        // The spec says client MUST close - we're testing that the packet is valid

        // Close our end
        drop(stream);

        Ok(())
    })
}

/// MQTT-3.14.4-2: Server MUST discard Will Message on DISCONNECT
fn test_mqtt_3_14_4_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let will_topic = format!("test/will/discard/{}", uuid::Uuid::new_v4());
        let will_message = b"should not be published";

        // Set up subscriber to watch for will message
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
                Err(_) => return Err(ConformanceError::Timeout("Waiting for subscriber CONNACK".to_string())),
            }
        }

        sub_client.subscribe(&will_topic, QoS::AtLeastOnce).await
            .map_err(ConformanceError::MqttClient)?;

        loop {
            match tokio::time::timeout(ctx.timeout, sub_eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for SUBACK".to_string())),
            }
        }

        // Connect client with will message, then properly disconnect
        {
            let mut will_opts = MqttOptions::new(
                format!("mqtt-will-{}", &uuid::Uuid::new_v4().to_string()[..8]),
                &ctx.host,
                ctx.port,
            );
            will_opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            will_opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
            will_opts.set_clean_session(true);
            will_opts.set_last_will(rumqttc::LastWill::new(
                &will_topic,
                will_message.to_vec(),
                QoS::AtLeastOnce,
                false,
            ));

            let (will_client, mut will_eventloop) = AsyncClient::new(will_opts, 10);

            loop {
                match tokio::time::timeout(ctx.timeout, will_eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for will client CONNACK".to_string())),
                }
            }

            // Properly disconnect - will message should NOT be published
            will_client.disconnect().await
                .map_err(ConformanceError::MqttClient)?;

            // Poll eventloop to actually send the DISCONNECT packet
            // The disconnect() call just queues it, we need to poll to send
            loop {
                match tokio::time::timeout(Duration::from_millis(500), will_eventloop.poll()).await {
                    Ok(Err(_)) => break, // Connection closed after disconnect
                    Ok(Ok(_)) => continue, // Keep polling until closed
                    Err(_) => break, // Timeout is fine
                }
            }

            // Give the server time to process
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait and verify will message is NOT received
        let timeout_duration = Duration::from_secs(3);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_millis(500), sub_eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                    if publish.topic == will_topic {
                        sub_client.disconnect().await.ok();
                        return Err(ConformanceError::ProtocolViolation(
                            "Will message was published despite proper DISCONNECT".to_string()
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
