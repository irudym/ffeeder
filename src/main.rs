use std::{ time::Duration, thread };
use log::{info, warn, error};
use uuid::Uuid;
use crossbeam::channel;
use ffeeder::feeder;
use ffeeder::phoenix;


fn main() {
    // Initialize the logger from the environment
    env_logger::init();
    let mqtt_host = "tcp://localhost:1883".to_string();

    //TODO: load credential from ENV!
    let mysql_host = "mysql://phoenix:phoenix@localhost:3306/fennec_dev";
    let websocket_host = "ws://127.0.0.1:4000/socket/websocket?vsn=2.0.0";

    // Create the client. Use an ID for a persistent session.
    let client_id = format!("fennec-feeder-{}", Uuid::new_v4());

    let subscriptions = [ "devices/+/data", "tests"];
    let qos = [1, 1];

    let (storage_sender, storage_receiver) = channel::unbounded();
    let (db_storage_sender, db_storage_receiver) = channel::unbounded();

    let storage_sender_ws = storage_sender.clone();

    
    

    let storage = thread::spawn(move || {
        info!("Start Storage thread...");
        loop {
            feeder::storage(storage_receiver.clone(), db_storage_sender.clone());
            error!("Restarting Storage thread");
        }
    });
    thread::sleep(Duration::from_secs(3));

    let db_storage = thread::spawn(move || {
        info!("Start DBStorage thread...");
        loop {
            feeder::db_storage(mysql_host, db_storage_receiver.clone());
            error!("Restarting DBStorage thread");
        }
    });
    thread::sleep(Duration::from_secs(3));


    let websocket_supervisor = thread::spawn(move || {
        info!("Start WebSocket Supervisor thread...");
        loop {
            let (sender_sender, sender_receiver) = channel::unbounded();
            let sender_for_pinger = sender_sender.clone();
    
            let client = phoenix::reconnect(websocket_host);
            info!("Connected to {}", websocket_host);

            let (receiver, sender) = client.split().unwrap();
            
            let storage_sender_2 = storage_sender_ws.clone();

            phoenix::join_phoenix(sender_sender.clone());

            let receiver_loop = std::thread::spawn(move || {
                phoenix::receiver(receiver, sender_sender.clone(), storage_sender_2);
            });

            let sender_loop = std::thread::spawn(move || {
                phoenix::sender(sender, sender_receiver);
            });

            let pinger_loop = std::thread::spawn(move || {
                phoenix::pinger(5000, sender_for_pinger);
            });

            if let Err(error) = receiver_loop.join() {
                error!("WebSocket Receiver thread error: {:?}", error);
            }

            if let Err(error) = sender_loop.join() {
                error!("WebSocket Sender thread error: {:?}", error);
            }

            if let Err(error) = pinger_loop.join() {
                error!("WebSocket Pinger thread error: {:?}", error);
            }
            
            warn!("Reconnection to WebSocket host");
        }
    });

    let subscriber = thread::spawn(move || {
        info!("Start Subscriber thread...");
        loop {
            feeder::subscriber(&mqtt_host, &client_id, &subscriptions, &qos, storage_sender.clone(), 1024);
            warn!("Reconnection to MQTT broker");
        }
    });
    thread::sleep(Duration::from_secs(3));

    subscriber.join().unwrap();
    storage.join().unwrap();
    db_storage.join().unwrap();
    websocket_supervisor.join().unwrap();
}
