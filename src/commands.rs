use crossbeam::channel;
use std::collections::BTreeMap;
use super::matrix_storage::*;

pub type DeviceMap = BTreeMap<String, Option<usize>>;  //device UID and id (from DB)
pub type DevicesUnitsStorage = MatrixStorage<()>; // unit_id: array of device_ids. Stores relationships (many-to-many) 
                                                    // between devices and units
pub type UnitMap = BTreeMap<String, usize>;         // unit name and id (from DB)

#[derive(Debug)]
pub enum Command {
    Add(String, String),
    Store(usize, BTreeMap<usize, String>),
    Load(channel::Sender<DeviceMap>),                   // load devices from DB
    UpdateDeviceList(DeviceMap),
    Activate(usize, String), // id and uid
    ActivateUnit(usize, String), // id and name
    LoadUnits(channel::Sender<UnitMap>),
    CreateUnit(String, channel::Sender<Option<usize>>),
    LoadDevicesUnits(channel::Sender<DevicesUnitsStorage>), //load table with lnk between devices and units 
    GetUnit(String, channel::Sender<Option<usize>>), // get unit id by name, or create a new record in DB in case of none
    CheckDeviceUnit(usize, usize, channel::Sender<bool>), //device_id, unit_id; check if a device has measurements by specific unit type
    LinkDeviceToUnit(usize, usize), //device_id, unit_id  
    Disconnect,
}