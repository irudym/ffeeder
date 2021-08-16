use std::{
  process,
  thread,
  time::Duration,
};
use std::sync::mpsc::Receiver;
use std::collections::BTreeMap;
use paho_mqtt as mqtt;
use log::{info, warn, error};
use crossbeam::channel;
use serde_json::{Value};
use mysql::prelude::*;
use mysql::Pool;
use mysql::params;
use chrono::prelude::*;

use super::units_storage::*;
use super::commands::*;
// use super::commands::Command::*;


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

                    match mqtt_client.subscribe_many(subscriptions, qos) {
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
    let ( mqtt_client, mqtt_message_receiver ) = mqtt_connect(mqtt_host, client_id, subscriptions, qos);

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
                    let device_id = message.topic().split('/').nth(1).unwrap_or("");
                    // info!("Send message: STORE({})", message.payload_str());
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
        mqtt_client.unsubscribe_many(subscriptions).unwrap();
        mqtt_client.disconnect(None).unwrap();
    }
}


pub fn storage(storage_receiver: channel::Receiver<Command>, db_storage_sender: channel::Sender<Command>) {
    let mut devices = DeviceMap::new(); // if ID from database provided, then the device is active, otherwise it's NONE
    // load devices from DB
    use Command::*;

    let (s_sender, s_receiver) = channel::bounded(1);

    let (units_storage_sender, units_storage_receiver) = channel::unbounded();

    let db_storage_sender_for_units = db_storage_sender.clone();
    thread::spawn(move || {
        units_storage(units_storage_receiver, db_storage_sender_for_units);
    });
    
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
                // info!("Store for {}, message: {}", uid, payload);
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
                        let process_units_storage_sender = units_storage_sender.clone();
                        
                        // start a processing thread which should parse the received payload and prepare data to be put in DB
                        thread::spawn(move || {
                            match serde_json::from_str::<Value>(&payload) {
                                Ok(value) => {
                                    if let Value::Object(map) = value {
                                        // info!("Parsed: {:?}", map);
                                        // iterate the map and find unit ids
                                        let mut records:BTreeMap<usize, String> = BTreeMap::new();

                                        for (unit, value) in &map {
                                            //find unit id
                                            let (u_sender, u_receiver) = channel::bounded(1);

                                            // find unit_id by name
                                            if let Err(error) = process_units_storage_sender.send(GetUnit(unit.clone(), u_sender)) {
                                                error!("Processing thread error: {}", error);
                                            }

                                            let mut unit_id:Option<usize> = None;  

                                            // update device list from DB
                                            if let Ok(message) = u_receiver.recv() {
                                                match message {
                                                    Some(u_id) => {
                                                        unit_id = Some(u_id); 
                                                        records.insert(u_id, value.to_string());
                                                    },
                                                    None => {
                                                        error!("Processing thread error: Cannot find unit_id in DB and cannot create a new record");
                                                    }
                                                }    
                                            }

                                            // check if the device can send measurement with this unit
                                            let (ud_sender, ud_receiver):(channel::Sender<bool>, channel::Receiver<bool>) = channel::bounded(1);
                                            if let Some(unit_id) = unit_id {
                                                if let Err(error) = process_units_storage_sender.send(CheckDeviceUnit(device_id, unit_id, ud_sender)) {
                                                    error!("Processing thread error: {}", error);
                                                }

                                                if let Ok(message) = ud_receiver.recv() {
                                                    if !message {
                                                        if let Err(error) = process_units_storage_sender.send(LinkDeviceToUnit(device_id, unit_id)) {
                                                            error!("Processing thread error: {}", error);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if let Err(error) = process_db_storage_sender.send(Store(device_id, records)) {
                                            error!("{}", error);
                                        }
                                    } else {
                                        error!("Storage thread: the message should be a map");
                                    }
                                },
                                Err(error) => {
                                    error!("Cannot parse the payload string due to error: {}", error);
                                }
                            }
                        });
                    },
                    None => {
                        warn!("No device with UID: {} in devices, an user needs to add it at first", &uid);
                    }
                }
            },
            Disconnect => {
                return;
            },
            Activate(id, uid) => {
                info!("Activate device: {} with id: {}", &uid, id);
                devices.insert(uid, Some(id));
            },
            ActivateUnit(id, name) => {
                info!("Activate unit: {} with id: {}", &name, id);
                // units.insert(name, id);
                if let Err(error) = units_storage_sender.send(ActivateUnit(id, name)) {
                    error!("Storage thread error: {}", error);
                }
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
            Store(id, map) => {
                info!("Put {:?} to DB with id: {:?}", map, id);
                let utc_timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
                for (unit_id, value) in map {
                    let res = conn.exec_drop("INSERT INTO records (device_id, unit_id, value, inserted_at, updated_at) VALUES (:device_id, :unit_id, :value, :inserted_at, :updated_at)",
                        params! { "device_id" => id, "unit_id" => unit_id, "value" => value, "inserted_at" => &utc_timestamp, "updated_at" => &utc_timestamp});
                    if let Err(error) = res {
                        error!("DBStorage thread error: {}", error);
                    }
                }
            },
            LoadDevicesUnits(sender) => {
                let mut devices_units = DevicesUnitsStorage::new();
                let _list_of_records = conn
                    .query_map("SELECT unit_id, device_id FROM devices_units;", |(unit_id, device_id)| {
                        // get units vector
                        devices_units.add(unit_id, device_id, ());
                        info!("Record: {:?} : {:?} ", unit_id, device_id);
                });
                if let Err(error) = sender.send(devices_units) {
                    error!("DBStorage thread error: {}", error);
                }
            },
            LoadUnits(sender) => {
                let mut units = UnitMap::new();
                let _list_of_units = conn
                    .query_map("SELECT id, name FROM units;", |(id, name)| {
                        units.insert(name, id);
                });
                if let Err(error) = sender.send(units) {
                    error!("DBStorage thread error: {}", error);
                }
            },
            CreateUnit(name, sender) => {
                let utc_timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
                let _res = conn.exec_first::<u32, _, _>("INSERT INTO units (name, inserted_at, updated_at) VALUES (:name, :inserted_at, :updated_at);", 
                    params! { "name" => name, "inserted_at" => &utc_timestamp,
                                "updated_at" => utc_timestamp});

                // Get ID of the created record. In case of PostgreSQL could be omitted as queries in PostgreSQL return ID of the record
                let res:mysql::Result<Option<usize>> = conn.query_first("SELECT LAST_INSERT_ID();");

                if let Ok(unit_id) = res {
                    info!("Created unit record with ID: {:?}", &unit_id);
                    if let Err(error) = sender.send(unit_id) {
                        error!("DBStorage thread error: {}", error);
                    }
                } else {
                    error!("DBStorage thread: Cannot create unit record in DB");
                }
            },
            LinkDeviceToUnit(device_id, unit_id) => {
                info!("Create a record in devices_units table: (device_id: {}, unit_id: {}", device_id, unit_id);
                let utc_timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
                let res = conn.exec_first::<u32, _, _,>("INSERT INTO devices_units (device_id, unit_id, inserted_at, updated_at) 
                            VALUES (:device_id, :unit_id, :inserted_at, :updated_at);", 
                    params! { "device_id" => device_id, "unit_id" => unit_id, "inserted_at" => &utc_timestamp, "updated_at" => utc_timestamp });
                if let Err(error) = res {
                    error!("DBStorage thread error: {}", error);
                }
            }
            _ => {
                warn!("DBStorage thread: unimplemented command!");
            },
        }
    }
}

