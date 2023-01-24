# Energymeter Python Module

This is a Python module that implements the communication to several electricity meters and related equipment.

At the moment, the following meters are supported:

* ABB B21, B23, A43 and similar ABB meters (Modbus Serial)
* SMA SunnyBoy (Modbus TCP)
* MultiCube meters

Some base classes are also provided for you to implement new meter variants.

## Installation

```
git clone https://github.com/ElaadNL/python-energymeter
cd python-energymeter
pip3 install .
```

## Usage

To read all registers from a meter:

```
from energymeter import ABBMeter

meter = ABBMeter(port="/dev/ttyUSB0", baudrate=38400, slaveaddress=1, model="B23")
data = meter.read()
```

This returns a dictionary with all key-value pairs of data.

To read a single register:

```
data = meter.read('current_l1')
```

This returns just the value (usualy a float or int).

To read multiple registers:

```
data = meter.read(['current_l1', 'voltage_l1_n'])
```

This returns a dictionary with the requested keys and values.
