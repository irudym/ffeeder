// WebSocket communication with Elixir Phoenix backend
//  

use log::{error, info, warn};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use websocket::client::sync::Client;
use websocket::receiver::Reader;
use websocket::result::{WebSocketError, WebSocketResult};
use websocket::sender::Writer;
use websocket::{ClientBuilder, Message, OwnedMessage};
use crossbeam::channel;

use super::feeder::Command;

pub fn connect(host: &str) -> WebSocketResult<Client<TcpStream>> {
    match ClientBuilder::new(host) {
        Ok(client_builder) => client_builder.add_protocol("pubsub").connect_insecure(),
        Err(_error) => WebSocketResult::Err(WebSocketError::ProtocolError("Cannot connect to the host")),
    }
}

pub fn reconnect(host: &str) -> Client<TcpStream> {
    match connect(host) {
        Ok(client) => return client,
        Err(error) => {
            error!("Error: {:?}", error);
            warn!("Reconnect...");
            thread::sleep(Duration::from_secs(5));
            return reconnect(host);
        } 
    }
}

pub fn receiver(mut receiver: Reader<TcpStream>, sender_channel: channel::Sender<websocket::OwnedMessage>, storage_channel: channel::Sender<Command>) {
    for message in receiver.incoming_messages() {
        let message = match message {
            Ok(m) => m,
            Err(e) => {
              error!("Received error from message stream: {:?}", e);
              thread::sleep(Duration::from_millis(2000));
              OwnedMessage::Close(None)
            }
        };

        match message {
            OwnedMessage::Ping(data) => {
                // need to answer with pong
                if let Err(error) = sender_channel.send(OwnedMessage::Pong(data)) {
                  error!("{}", error);
                }
            },
            OwnedMessage::Pong(_data) => {
                // info!("got pong response: {:?}", data);
            },
            OwnedMessage::Close(_) => {
                // Got a close message, so send a close message and return
                if let Err(error) = sender_channel.send(OwnedMessage::Close(None)) {
                    error!("{}", error);
                }
                return;
            },
            // Say what we received
            _ => {
                info!("Receive Loop: {:?}", &message);
                let message = match message {
                    OwnedMessage::Text(msg) => msg,
                    _ => "".to_string(),
                };
                
                //send message to the response channel
                if let Err(error) = storage_channel.send(Command::Activate(message)) {
                    error!("WebSocket Receiver error: {}", error);
                }
            }
        }
    }
}

pub fn sender(mut tcp_stream: Writer<TcpStream>, channel: channel::Receiver<OwnedMessage>) {
    while let Ok(message) = channel.recv() {
        match message {
            OwnedMessage::Close(None) => {
                return;
            }
            _ => {}
        }
    
        match tcp_stream.send_message(&message) {
            Ok(()) => (),
            Err(e) => {
                    error!("WebSocket Sender error: {}", e);
                    let _ = tcp_stream.send_message(&Message::close());
                    // need to reconnect
                    return;
            }
        }
        // info!("Message sent: {:?}", &message);
        // println!("Message sent: {:?}", &message);

    }
}

pub fn pinger(interval: u64, sender_channel: channel::Sender<OwnedMessage>) {
    loop {
        if let Err(error) = sender_channel.send(OwnedMessage::Ping("ping".as_bytes().to_vec())) {
            error!("Error during WebSocket ping: {}", error);
            return;
        }
        thread::sleep(Duration::from_millis(interval));
    }
}

pub fn join_phoenix(sender_channel: channel::Sender<OwnedMessage>) {
    // send join message
    let join_message =
    OwnedMessage::Text("[\"1\",\"1\",\"devices\", \"phx_join\", {}]".to_string());
    
    if let Err(error) = sender_channel.send(join_message) {
        error!("Cannot send join message due to error: {}", error);
    }
}