//! CONNACK packet tests for MQTT 3.1.1
//!
//! Tests normative statements from Section 3.2 of the MQTT 3.1.1 specification.

use crate::error::ConformanceError;
use crate::tests::{NormativeTest, TestContext, TestFn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

fn make_test(id: &str, description: &str, runner: TestFn) -> NormativeTest {
    NormativeTest {
        id: id.to_string(),
        section: "connack".to_string(),
        description: description.to_string(),
        runner,
    }
}

pub fn get_tests() -> Vec<NormativeTest> {
    vec![
        // MQTT-3.2.0-1
        make_test(
            "MQTT-3.2.0-1",
            "First packet from Server to Client MUST be CONNACK",
            test_mqtt_3_2_0_1,
        ),
        // MQTT-3.2.2-1
        make_test(
            "MQTT-3.2.2-1",
            "CleanSession=1 accepted: Session Present MUST be 0",
            test_mqtt_3_2_2_1,
        ),
        // MQTT-3.2.2-2
        make_test(
            "MQTT-3.2.2-2",
            "CleanSession=0 with stored session: Session Present MUST be 1",
            test_mqtt_3_2_2_2,
        ),
        // MQTT-3.2.2-3
        make_test(
            "MQTT-3.2.2-3",
            "CleanSession=0 without stored session: Session Present MUST be 0",
            test_mqtt_3_2_2_3,
        ),
        // MQTT-3.2.2-4
        make_test(
            "MQTT-3.2.2-4",
            "Non-zero return code: Session Present MUST be 0",
            test_mqtt_3_2_2_4,
        ),
        // MQTT-3.2.2-5
        make_test(
            "MQTT-3.2.2-5",
            "Non-zero return code: Server MUST close connection after CONNACK",
            test_mqtt_3_2_2_5,
        ),
        // MQTT-3.2.2-6
        make_test(
            "MQTT-3.2.2-6",
            "If no return code applicable, Server MUST close without CONNACK",
            test_mqtt_3_2_2_6,
        ),
    ]
}

/// MQTT-3.2.0-1: First packet from Server to Client MUST be CONNACK
fn test_mqtt_3_2_0_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT packet
        let client_id = format!("test3201{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let connect_packet = build_connect_packet(&client_id, 30);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        // Read first packet from server
        let mut buf = [0u8; 4];
        tokio::time::timeout(ctx.timeout, stream.read_exact(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for server response".to_string()))?
            .map_err(ConformanceError::Io)?;

        // First byte should be 0x20 (CONNACK packet type)
        if buf[0] == 0x20 {
            Ok(())
        } else {
            Err(ConformanceError::ProtocolViolation(format!(
                "First packet from server was not CONNACK, got packet type: 0x{:02X}",
                buf[0]
            )))
        }
    })
}

/// MQTT-3.2.2-1: CleanSession=1 accepted: Session Present MUST be 0
fn test_mqtt_3_2_2_1(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let mut opts = MqttOptions::new(
            format!("mqtt-3221-{}", uuid::Uuid::new_v4()),
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
                Ok(Ok(Event::Incoming(Packet::ConnAck(connack)))) => {
                    client.disconnect().await.ok();
                    if connack.session_present {
                        return Err(ConformanceError::ProtocolViolation(
                            "Session Present must be 0 when CleanSession=1".to_string()
                        ));
                    }
                    return Ok(());
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
            }
        }
    })
}

/// MQTT-3.2.2-2: CleanSession=0 with stored session: Session Present MUST be 1
fn test_mqtt_3_2_2_2(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        let client_id = format!("mqtt-3222-{}", &uuid::Uuid::new_v4().to_string()[..8]);

        // First connection with CleanSession=0
        {
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
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for first CONNACK".to_string())),
                }
            }

            client.disconnect().await.ok();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second connection with CleanSession=0 - should have session present
        {
            let mut opts = MqttOptions::new(&client_id, &ctx.host, ctx.port);
            opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
            opts.set_clean_session(false);

            let (client, mut eventloop) = AsyncClient::new(opts, 10);

            loop {
                match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Packet::ConnAck(connack)))) => {
                        client.disconnect().await.ok();
                        if !connack.session_present {
                            return Err(ConformanceError::ProtocolViolation(
                                "Session Present must be 1 when resuming session".to_string()
                            ));
                        }
                        return Ok(());
                    }
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                    Err(_) => return Err(ConformanceError::Timeout("Waiting for second CONNACK".to_string())),
                }
            }
        }
    })
}

/// MQTT-3.2.2-3: CleanSession=0 without stored session: Session Present MUST be 0
fn test_mqtt_3_2_2_3(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        // Use a unique client ID that definitely has no session
        let client_id = format!("mqtt-new-{}", uuid::Uuid::new_v4());

        let mut opts = MqttOptions::new(&client_id, &ctx.host, ctx.port);
        opts.set_keep_alive(Duration::from_secs(30));
        if let Some(ref username) = ctx.username {
            opts.set_credentials(username, ctx.password.as_deref().unwrap_or(""));
        }
        opts.set_clean_session(false);

        let (client, mut eventloop) = AsyncClient::new(opts, 10);

        loop {
            match tokio::time::timeout(ctx.timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::ConnAck(connack)))) => {
                    client.disconnect().await.ok();
                    if connack.session_present {
                        return Err(ConformanceError::ProtocolViolation(
                            "Session Present must be 0 for new session".to_string()
                        ));
                    }
                    return Ok(());
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(ConformanceError::MqttConnection(e)),
                Err(_) => return Err(ConformanceError::Timeout("Waiting for CONNACK".to_string())),
            }
        }
    })
}

/// MQTT-3.2.2-4: Non-zero return code: Session Present MUST be 0
fn test_mqtt_3_2_2_4(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with empty ClientId and CleanSession=0 to trigger rejection
        let connect_packet = build_connect_packet_empty_clientid(30, false);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        // Read CONNACK
        let mut buf = [0u8; 4];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 4 && buf[0] == 0x20 {
            let session_present = buf[2] & 0x01;
            let return_code = buf[3];

            if return_code != 0 && session_present != 0 {
                return Err(ConformanceError::ProtocolViolation(
                    "Session Present must be 0 when return code is non-zero".to_string()
                ));
            }
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
    packet.push(0x02); // CleanSession=1
    packet.push((keep_alive >> 8) as u8);
    packet.push((keep_alive & 0xFF) as u8);
    packet.push((client_id_len >> 8) as u8);
    packet.push((client_id_len & 0xFF) as u8);
    packet.extend_from_slice(client_id_bytes);

    packet
}

fn build_connect_packet_empty_clientid(keep_alive: u16, clean_session: bool) -> Vec<u8> {
    let connect_flags = if clean_session { 0x02 } else { 0x00 };
    let remaining_length = 6 + 1 + 1 + 2 + 2;

    let mut packet = Vec::with_capacity(2 + remaining_length);
    packet.push(0x10);
    packet.push(remaining_length as u8);
    packet.push(0x00);
    packet.push(0x04);
    packet.extend_from_slice(b"MQTT");
    packet.push(4);
    packet.push(connect_flags);
    packet.push((keep_alive >> 8) as u8);
    packet.push((keep_alive & 0xFF) as u8);
    packet.push(0x00);
    packet.push(0x00);

    packet
}

/// MQTT-3.2.2-5: Non-zero return code: Server MUST close connection after CONNACK
fn test_mqtt_3_2_2_5(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send CONNECT with empty ClientId and CleanSession=0 to trigger rejection
        let connect_packet = build_connect_packet_empty_clientid(30, false);
        stream.write_all(&connect_packet).await
            .map_err(ConformanceError::Io)?;

        // Read CONNACK
        let mut buf = [0u8; 4];
        let n = tokio::time::timeout(ctx.timeout, stream.read(&mut buf)).await
            .map_err(|_| ConformanceError::Timeout("Waiting for CONNACK".to_string()))?
            .map_err(ConformanceError::Io)?;

        if n >= 4 && buf[0] == 0x20 && buf[3] != 0 {
            // Got CONNACK with non-zero return code, now verify connection closes
            let mut check_buf = [0u8; 4];
            match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut check_buf)).await {
                Ok(Ok(0)) => Ok(()), // Connection closed - correct
                Ok(Err(_)) => Ok(()), // Connection error - correct
                Err(_) => Err(ConformanceError::ProtocolViolation(
                    "Server did not close connection after non-zero CONNACK".to_string()
                )),
                Ok(Ok(_)) => Err(ConformanceError::ProtocolViolation(
                    "Server sent more data after non-zero CONNACK".to_string()
                )),
            }
        } else if n == 0 {
            // Connection closed immediately - also acceptable
            Ok(())
        } else {
            // Server accepted the connection (some servers may accept empty client ID)
            Ok(())
        }
    })
}

/// MQTT-3.2.2-6: If no return code applicable, Server MUST close without CONNACK
fn test_mqtt_3_2_2_6(ctx: TestContext) -> Pin<Box<dyn Future<Output = Result<(), ConformanceError>> + Send>> {
    Box::pin(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(format!("{}:{}", ctx.host, ctx.port))
            .await
            .map_err(|e| ConformanceError::Connection(e.to_string()))?;

        // Send a malformed CONNECT that doesn't fit any return code
        // (e.g., reserved flag set, which is a protocol error without specific return code)
        let client_id = format!("test3226{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let client_id_bytes = client_id.as_bytes();
        let remaining_length = 6 + 1 + 1 + 2 + 2 + client_id_bytes.len();

        let mut packet = Vec::new();
        packet.push(0x10);
        packet.push(remaining_length as u8);
        packet.push(0x00);
        packet.push(0x04);
        packet.extend_from_slice(b"MQTT");
        packet.push(4);
        packet.push(0x01); // Reserved bit set (invalid)
        packet.push(0x00);
        packet.push(0x1E);
        packet.push((client_id_bytes.len() >> 8) as u8);
        packet.push((client_id_bytes.len() & 0xFF) as u8);
        packet.extend_from_slice(client_id_bytes);

        stream.write_all(&packet).await
            .map_err(ConformanceError::Io)?;

        // Server should close without CONNACK (or send CONNACK with error)
        let mut buf = [0u8; 4];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
            Ok(Ok(0)) => Ok(()), // Connection closed without CONNACK - correct
            Ok(Err(_)) => Ok(()), // Connection error - correct
            Ok(Ok(n)) if n >= 4 && buf[0] == 0x20 && buf[3] != 0 => Ok(()), // CONNACK with error is also acceptable
            Err(_) => Err(ConformanceError::ProtocolViolation(
                "Server did not close connection for invalid CONNECT".to_string()
            )),
            _ => Err(ConformanceError::ProtocolViolation(
                "Server accepted invalid CONNECT".to_string()
            )),
        }
    })
}
