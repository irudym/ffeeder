/// Unit storage module
/// Defines unit storage thread which is responsible to cache units and devices_units tables 
///
/// 

use std::{
    thread,
    time::Duration,
  };
use crossbeam::channel;
use log::{info, warn, error};

use super::commands::*;

/// Send a 'message' to provided 'sender'
/// print error message and wait provided 'sleep_millis' in milliseconds in case of errors
fn send_to<T>(sender: &channel::Sender<T>, message: T, sleep_millis: u64) -> Result<(), &str> {
    if let Err(error) = sender.send(message) {
        error!("Units Storage thread error: {}", error);
        thread::sleep(Duration::from_millis(sleep_millis));

        Err("Error")
    } else {
        Ok(())
    }
}

/// Send a 'message' to provided 'sender'
/// the function returns nothing and just print an error message in case of errors.
fn send_to_without_errors<T>(sender: &channel::Sender<T>, message: T) {
    if send_to(sender, message, 0).is_err() {
        error!("Error during message sending!");
    }
}

/// A thread which is responsible to store units list and units-devices relationship matrix. 
/// The storage behaves like a cache between DB and the feeder thread
/// The function is used in Storage thread
pub fn units_storage(units_storage_receiver: channel::Receiver<Command>, db_storage_sender: channel::Sender<Command>) {
    use Command::*;

    // update list of units
    let mut units = UnitMap::new();
    let mut units_devices = DevicesUnitsStorage::new();

    let (units_sender, units_receiver) = channel::bounded(1);
    if send_to(&db_storage_sender, LoadUnits(units_sender), 5000).is_err() {
        // restart the thread
        return;
    }

    // update list of units
    if let Ok(message) = units_receiver.recv() {
        units = message;
        info!("Loaded {} units", units.len());
    }

    let (devices_units_sender, devices_units_receiver) = channel::bounded(1);
    if send_to(&db_storage_sender, LoadDevicesUnits(devices_units_sender), 5000).is_err() {
        // restart the thread
        return;
    }

    // update list of relationship between units and devices
    if let Ok(message) = devices_units_receiver.recv() {
        units_devices = message;
        info!("Loaded {} units-devices relationships", units_devices.rows_count());
        info!("Units_Devices: {:?}", units_devices);
    } else {
        error!("Cannot load units-devices relationship matrix, use empty list instead!");
    }

    while let Ok(message) = units_storage_receiver.recv() {
        match message {
            LinkDeviceToUnit(device_id, unit_id) => {
                units_devices.add(unit_id, device_id, ());
                // create a record in DB
                send_to_without_errors(&db_storage_sender, LinkDeviceToUnit(device_id, unit_id));
            },
            CheckDeviceUnit(device_id, unit_id, sender) => {
                if units_devices.get(unit_id, device_id).is_some() {
                    send_to_without_errors(&sender, true);
                } else {
                    send_to_without_errors(&sender, false);
                }
            },
            GetUnit(name, sender) => {
                if let Some(id) = units.get(&name) {
                    send_to_without_errors(&sender, Some(*id));
                } else {
                    // add unit to the list and to DB
                    let mut unit_id:Option<usize> = None;
                    warn!("Cannot find Unit named: {}. Create unit record in DB", &name);
                    let (units_sender, units_receiver) = channel::bounded(1);
                    send_to_without_errors(&db_storage_sender, CreateUnit(name.clone(), units_sender));
                    
                    // get response from DB
                    if let Ok(message) = units_receiver.recv() {
                        match message {
                            Some(id) => {
                                units.insert(name, id);
                            },
                            None => {
                                error!("Unit ID is unknown!");
                            }
                        }
                        unit_id = message;
                    }

                    send_to_without_errors(&sender, unit_id);
                };
            },
            _ => {
                warn!("Units Storage thread: unimplemented message: {:?}", message);
            }
        }
    }
}


