//! PINGREQ/PINGRESP tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 3.12 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "ping".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.12.4-1
        make_test(
            "MQTT-3.12.4-1",
            "Server MUST send PINGRESP in response to PINGREQ",
            test_mqtt_3_12_4_1,
        ),
    ]
}

/// MQTT-3.12.4-1: Server MUST send PINGRESP in response to PINGREQ
fn test_mqtt_3_12_4_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Connect
        let client_id = format!("test31241{}", &uuid::Uuid::new_v4().to_string()[..8]);
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

        // Send PINGREQ
        let pingreq = [0xC0, 0x00]; // PINGREQ packet
        stream.write_all(&pingreq).await
            .map_err(ConformanceError::Io)?;

        // Expect PINGRESP
        let mut pingresp = [0u8; 2];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut pingresp)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for PINGRESP".to_string()))?
            .map_err(ConformanceError::Io)?;

        if pingresp[0] == 0xD0 && pingresp[1] == 0x00 {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(format!(
                "Expected PINGRESP (0xD0 0x00), got {:02X} {:02X}",
                pingresp[0], pingresp[1]
            )))
        }
    })
}

/// Alternative test using rumqttc client
#[allow(dead_code)]
fn test_ping_with_client(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-ping-{}", uuid::Uuid::new_v4()),
            &ctx.host,
            ctx.port,
        );
        opts.set_keep_alive(Duration::from_secs(5)); // Short keep alive to trigger ping
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

        // Wait for ping/pong cycle (rumqttc handles this automatically)
        let timeout_duration = Duration::from_secs(10);
        let start = std::time::Instant::now();
        let mut saw_ping = false;

        while start.elapsed() < timeout_duration {
            match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::PingResp))) => {
                    saw_ping = true;
                    break;
                }
                Ok(Ok(Event::Outgoing(rumqttc::Outgoing::PingReq))) => {
                    // Client sent ping, keep waiting for response
                    continue;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => continue,
            }
        }

        client.disconnect().await.ok();

        if saw_ping {
            Ok(())
        } else {
            Err(ConformanceError::Timeout("Did not receive PINGRESP".to_string()))
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
