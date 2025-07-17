import threading
from pymodbus.datastore import (
    ModbusServerContext,
    ModbusSlaveContext,
    ModbusSparseDataBlock,
)
from pymodbus.server import (
    StartSerialServer,
)
from pymodbus.payload import BinaryPayloadBuilder, BinaryPayloadDecoder
from pymodbus.constants import Endian
import paho.mqtt.client as mqtt
import rw_lock


class CallbackDataBlock(ModbusSparseDataBlock):
    """A datablock that stores the new value in memory,.

    and passes the operation to a message queue for further processing.
    """
    def __init__(self, values: list[int]):
        """Initialize."""
        #self.queue = queue
        super().__init__({0: values, 0x5000:[8405,8405]})
        self.rwlock = rw_lock.ReadWriteLock()

    def setValues(self, address, value):
        """Set the requested values of the datastore."""
        with self.rwlock.get_writer_lock():
            super().setValues(address, value)
        txt = f"Callback from setValues with address {address}, value {value}"
        #_logger.debug(txt)

    def getValues(self, address, count=1):
        """Return the requested values from the datastore."""
        if address>=0x80:
            if address>=0x164:
                if address<0x200:
                    pass
                else:
                    # XX!
                    pass
            else:
                # XX!
                pass
        with self.rwlock.get_reader_lock():
            result = super().getValues(address, count=count)
        txt = f"Callback from getValues with address {address}, count {count}, data {result}"
        #_logger.debug(txt)
        return result

#0
dtsu666_mem = [0, 0, 0x361f, 0x0, 0x361f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x35bd, 0x0, 0x35bd, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x62, 0x0, 0x62, 0x0, 0x0, 0x0, 0x0, \
0x0, 0x0, 0x0, 0x31b, 0x0, 0x31b, 0x0, 0xa4ad, 0xffff, 0xfff9, 0x0, 0x9d81, 0x45b2, 0x8777, 0x0, 0xe, 0x0, 0x31b, 0x0, 0x31b, 0x0, 0x4, 0x0, 0xa4ad, 0xffff, 0xfff9, 0x0, 0x31b, 0x0, 0x9d81, 0x0, 0x31b, 0x0, 0x31b, 0x0, 0x31b, 0xffff, 0xfff9, 0x0, 0x4, 0x0, 0x4, 0x0, 0x31b, 0xffff, 0xfff9, 0xffff, 0xfff9, 0xffff, 0xfff9, 0xffff, 0xfff9, 0x0, 0xe, 0x0, 0x31b, 0xffff, 0xfff9, 0xffff, 0xfff9, 0x0, 0x3, 0x0, 0x31b, 0x0, 0x12, 0, 0,0, 0,0x93c, 0x950, 0x4f, 0x30, 0x72, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1389, 0x100d, 0x100d, 0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, \
0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0x0, 0x7d, 0x0, 0x5c, 0x0, 0xa0, 0x0, 0x17d, 0x0, 0x89, 0x0, 0x28, 0xffff, 0xff98, 0x0, 0x49, 0x0, 0xbc, 0x0, 0x70, 0x0, 0x10a, 0x0, 0x237, 0x29a, 0x32f, 0x26f, 0x2a3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0]

def sync_mqtt(dtsu666_hr: CallbackDataBlock):
    def on_connect(mosq, obj, rc, a):
        mqttc.subscribe("smartmeter/0/1-0:16_7_0__255/value", 0)
        mqttc.subscribe("smartmeter/0/1-0:36_7_0__255/value", 0)
        mqttc.subscribe("smartmeter/0/1-0:56_7_0__255/value", 0)
        mqttc.subscribe("smartmeter/0/1-0:76_7_0__255/value", 0)
        #print("rc: " + str(rc))

    def on_message(mosq, obj, message):
        match message.topic:
            case "smartmeter/0/1-0:16_7_0__255/value":
                builder = BinaryPayloadBuilder(byteorder=Endian.BIG, wordorder=Endian.BIG)
                builder.add_32bit_int(round(float(message.payload.decode())))
                dtsu666_hr.setValues(0x16A+1,builder.to_registers())
                #print(message.payload.decode())
            case "smartmeter/0/1-0:76_7_0__255/value":#was 36 - pins on meter changed!
                builder = BinaryPayloadBuilder(byteorder=Endian.BIG, wordorder=Endian.BIG)
                builder.add_32bit_int(round(float(message.payload.decode())))
                # builder.add_32bit_int(124)
                dtsu666_hr.setValues(0x164+1,builder.to_registers()) 
            case "smartmeter/0/1-0:56_7_0__255/value"
                builder = BinaryPayloadBuilder(byteorder=Endian.BIG, wordorder=Endian.BIG)
                builder.add_32bit_int(round(float(message.payload.decode())))
                dtsu666_hr.setValues(0x166+1,builder.to_registers()) 
            case "smartmeter/0/1-0:36_7_0__255/value":#was 76
                builder = BinaryPayloadBuilder(byteorder=Endian.BIG, wordorder=Endian.BIG)
                builder.add_32bit_int(round(float(message.payload.decode())))
                dtsu666_hr.setValues(0x168+1,builder.to_registers()) 
            case _:
                pass

    #def on_publish(mosq, obj, mid):
    #    print("mid: " + str(mid))

    #def on_subscribe(mosq, obj, mid, granted_qos):
    #    print("Subscribed: " + str(mid) + " " + str(granted_qos))

    #def on_log(mosq, obj, level, string):
    #    print(string)

    mqttc = mqtt.Client()
    # Assign event callbacks
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    #mqttc.on_publish = on_publish
    #mqttc.on_subscribe = on_subscribe
    # Connect
    mqttc.connect("192.168.0.10")

    # Continue the network loop
    mqttc.loop_forever()

if __name__ == "__main__":
    dtsu666_hr=CallbackDataBlock(dtsu666_mem)
    slavecontext = ModbusSlaveContext(
        di=dtsu666_hr,
        co=dtsu666_hr,
        ir=dtsu666_hr,
        hr=dtsu666_hr
    )

    thread = threading.Thread(target = sync_mqtt, args=(dtsu666_hr,))
    thread.start()
    StartSerialServer(
        context=ModbusServerContext(slaves=slavecontext, single=True),  # Data storage
        # identity=args.identity,  # server identify
        timeout=0.040,  # waiting time for request to complete
        port="COM15",  # serial port
        # custom_functions=[],  # allow custom handling
        # framer=args.framer,  # The framer strategy to use
        # stopbits=1,  # The number of stop bits to use
        # bytesize=8,  # The bytesize of the serial messages
        # parity="N",  # Which kind of parity to use
        baudrate=9600,  # The baud rate to use for the serial device
        handle_local_echo=True,  # Handle local echo of the USB-to-RS485 adaptor
        ignore_missing_slaves=True,  # ignore request to a missing slave
        # broadcast_enable=False,  # treat slave 0 as broadcast address,
    )
