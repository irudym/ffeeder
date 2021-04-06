use std::{
  process,
  thread,
  time::Duration,
};
use std::sync::mpsc::Receiver;
use std::collections::BTreeMap;
use paho_mqtt as mqtt;
use log::{info, trace, warn, error};
use crossbeam::channel;
use serde_json::{Result, Value};
use mysql::prelude::*;
use mysql::Pool;

#[derive(Debug)]
pub enum Command {
    Add(String, String),
    Store(usize, Value),
    Load(channel::Sender<DeviceMap>),                   // load devices from DB
    UpdateDeviceList(DeviceMap),
    Activate(String),
    Disconnect,
}


pub type DeviceMap = BTreeMap<String, Option<usize>>;  //device UID and id (from DB)


pub fn mqtt_connect(mqtt_host: &str, client_id: &str, subscriptions: &[&str], qos: &[i32]) -> (mqtt::Client, Receiver<Option<mqtt::Message>>) {
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(mqtt_host)
        .client_id(client_id)
        .finalize();

    let mut mqtt_client = mqtt::Client::new(create_opts).unwrap_or_else(|e| {
        error!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(false)
        .finalize();

    let message_receiver = mqtt_client.start_consuming();

    info!("Connecting to MQTT broker");
    match mqtt_client.connect(conn_opts) {
        Ok(rsp) => {
            if let Some(conn_rsp) = rsp.connect_response() {
                info!("Connected to: '{}' with MQTT version {}",
                         conn_rsp.server_uri, conn_rsp.mqtt_version);
                if !conn_rsp.session_present {
                    // Register subscriptions on the server
                    info!("Subscribing to topics, with requested QoS: {:?}...", qos);

                    match mqtt_client.subscribe_many(&subscriptions, &qos) {
                        Ok(qosv) => info!("QoS granted: {:?}", qosv),
                        Err(e) => {
                            error!("Error subscribing to topics: {:?}", e);
                            mqtt_client.disconnect(None).unwrap();
                            process::exit(1);
                        }
                    }
                }
            }
        },
        Err(e) => {
            error!("Error connecting to the broker: {:?}", e);
            thread::sleep(Duration::from_millis(3000));
            info!("Reconnect to MQTT");
            return mqtt_connect(mqtt_host, client_id, subscriptions, qos)
        }
    }
    ( mqtt_client, message_receiver )
}

pub fn subscriber(mqtt_host: &str, client_id: &str, subscriptions: &[&str], qos: &[i32], storage_sender: channel::Sender<Command>, max_payload_size: usize) {
    // Make the connection to the broker
    let ( mqtt_client, mqtt_message_receiver ) = mqtt_connect(&mqtt_host, &client_id, &subscriptions, &qos);

    // Just loop on incoming messages.
    // If we get a None message, check if we got disconnected,
    // and then try a reconnect.
    info!("Waiting for messages...");
    while let Ok(msg) = mqtt_message_receiver.recv() {
        use Command::*;
        match msg {
            Some(message) => {
                if message.payload().len() > max_payload_size { // DDoS protection
                    error!("Payload size is unacceptable (bigger than {} bytes", max_payload_size);
                } else {
                    // device UID
                    let device_id = message.topic().split("/").nth(1).unwrap_or("");
                    // println!("Send message: STORE({})", message.payload_str());
                    if let Err(error) = storage_sender.send(Add(device_id.to_string(), message.payload_str().to_string())) {
                        error!("{}", error);
                    }
                }
            },
            None => {
                error!("Connection lost");
                break;
            }
        }
    }

    // If we're still connected, then disconnect now,
    // otherwise we're already disconnected.
    if mqtt_client.is_connected() {
        warn!("Disconnecting...");
        mqtt_client.unsubscribe_many(&subscriptions).unwrap();
        mqtt_client.disconnect(None).unwrap();
    }
}

pub fn storage(storage_receiver: channel::Receiver<Command>, db_storage_sender: channel::Sender<Command>) {
    let mut devices = DeviceMap::new(); // if ID from database provided, then the device is active, otherwise it's NONE
    // load devices from DB
    use Command::*;

    let (s_sender, s_receiver) = channel::bounded(1);
    
    if let Err(error) = db_storage_sender.send(Load(s_sender)) {
        error!("Storage thread error: {}", error);
        thread::sleep(Duration::from_millis(5000));

        // restart the thread
        return;
    }

    // update device list from DB
    if let Ok(message) = s_receiver.recv() {
        devices = message;
        info!("Loaded {} devices", devices.len());
    }

    
    while let Ok(message) = storage_receiver.recv() {
        

        match message {
            Add(uid, payload) => {
                info!("Store for {}, message: {}", uid, payload);
                match devices.get(&uid) {
                    Some(None) => {
                        warn!("Device: {} is inactive", &uid);
                    },
                    Some(Some(id)) => {
                        // send payload to DB stream
                        // let process_db_storage_tx = db_storage_tx.clone();
                        let device_id = *id;

                        // info!("Device id: {} message: {}", &id, &payload);
                        // parse JSON payload to Value struct and pass it to DB thread
                        let process_db_storage_sender = db_storage_sender.clone();
                        thread::spawn(move || {
                            match serde_json::from_str::<Value>(&payload) {
                                Ok(value) => {
                                    info!("Parsed: {:?}", value);
                                    if let Err(error) = process_db_storage_sender.send(Store(device_id, value)) {
                                        error!("{}", error);
                                    }
                                },
                                Err(error) => {
                                    error!("{}", error);
                                }
                            }
                        });
                    },
                    None => {
                        info!("Add device with UID: {} to devices", &uid);
                        devices.insert(uid.clone(), None);
                        
                        //create a device in DB
                        // if let Err(error) = db_storage_tx.send(Create(device_uid)).await {
                        //    error!("{}", error);
                        //}
                    }
                }
            },
            Disconnect => {
                return;
            },
            _ => {
                warn!("Storage got unimplemented message: {:?}", message);
            }
        }
    }
}

pub fn db_storage(db_host: &str, db_storage_receiver: channel::Receiver<Command>) {
    let pool = Pool::new(db_host).unwrap();
    let mut conn = pool.get_conn().unwrap();

    while let Ok(message) = db_storage_receiver.recv() {
        use Command::*;

        match message {
            Load(sender) => {
                let mut devices = DeviceMap::new(); 
                let _list_of_devices = conn
                    // TODO: add where active is true
                    .query_map("SELECT id, uid FROM devices;", |(id, uid)| {
                        // println!("Device info: (id: {}, uid: {})", id, &uid);
                        // if device is active
                        devices.insert(uid, Some(id));
                });
                if let Err(error) = sender.send(devices) {
                    error!("DBStorage thread error: {}", error);
                }
            },
            Store(id, value) => {
                info!("Put {} to DB with id: {:?}", value, id);
            },
            _ => {
                warn!("DBStorage thread: unimplemented command!");
            }
        }
    }
}

