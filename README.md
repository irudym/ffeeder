### Fennec-Feeder

A broker to collect data from MQTT streams and put data to fennec DB.

#### Message formats

**Topic**: devices/[device_id]/data
**Payload** (should be a map object where key and value are strings): {"[unit_name]"/"[value]"}



##### Example
Publish temperature data in C: pub devices/1223456/data/temperature/23


#### Tests
for test purposes it's recommended to use mqttools package
to run publish message run following command
mqttool -- -p "devices/uid-77777777/data" -m "temperature/23"