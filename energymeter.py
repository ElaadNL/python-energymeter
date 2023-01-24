#!/usr/bin/env python3
import random
import socket
import struct
import asyncio
import time

import minimalmodbus
import tinysbus

from collections.abc import Iterable


class ModbusRTUMeter:
    """Meter class that uses a minimalmodbus Instrument to query an ABB meter."""
    def __init__(self, port, baudrate=38400, slaveaddress=1, timeout=0.5):
        """ Initialize the ModbusRTUmeter object.

        Arguments:
            * port: a serial port (string)
            * baudrate: the baudrate to use (integer)
            * slaveaddress: the address of the modbus device (integer)
            * Specification of the type. Used to limit the registers to a specific set.

        Returns:
            * An ABBMeter object

        """
        self.instrument = minimalmodbus.Instrument(port, slaveaddress)
        self.instrument.serial.baudrate = baudrate
        self.instrument.serial.timeout = timeout

    def read(self, regnames=None):
        """ Read one, many or all registers from the device

        Args:
            * regnames (str or list). If None, read all. If string, read
              single register. If list, read all registers from list.

        Returns:
            * If single register, it returns a single value. If all or list,
              return a dict with the keys and values.

        Raises:
            * KeyError, TypeError, IOError
        """
        if regnames is None:
            return self._batch_read(self.REGS)
        if type(regnames) is list:
            registers = [register for register in self.REGS if register['name'] in regnames]
            if len(registers) < len(regnames):
                regs_not_available = [regname for regname in regnames if regname not in
                                      [register['name'] for register in self.REGS]]
                print("Warning: the following registers are not available on this device: " +
                      ", ".join(regs_not_available))
                print("The available registers are: %s" +
                      ", ".join(register['name'] for register in self.REGS))
            if len(registers) == 0:
                return {}
            registers.sort(key=lambda reg: reg['start'])
            return self._batch_read(registers)
        elif type(regnames) is str:
            registers = [register for register in self.REGS if register['name'] == regnames]
            if len(registers) == 0:
                return "Register not found on device."
            return self._read_single(registers[0])
        else:
            raise TypeError

    def _read_single(self, register):
        """
        Read a single register and return the value. Not to be called directly.

        Arguments:
        * register: a 'register' dict that contains info on the register.

        Returns:
        * The interpreted value from the meter.
        """

        if register['length'] == 1:
            return self.instrument.read_register(registeraddress=register['start'],
                                                 number_of_decimals=register['decimals'],
                                                 signed=register['signed'])
        if register['length'] == 2:
            if register.get('is_float'):
                return self.instrument.read_float(registeraddress=register['start'],
                                                  number_of_registers=2)
            else:
                value = self.instrument.read_long(registeraddress=register['start'],
                                                  signed=register['signed'])
                return value / 10 ** register['decimals']

        if register['length'] == 4:
            value = self.instrument.read_registers(registeraddress=register['start'],
                                                   number_of_registers=register['length'])
            return self._convert_value(values=value,
                                       signed=register['signed'],
                                       number_of_decimals=register['decimals'],
                                       is_float=register.get('is_float'))

    def _read_multiple(self, registers):
        """
        Read multiple registers from the slave device and return their values as a dict.

        Arguments:
            * A list of registers (complete structs)

        Returns:
            * A dict containing all keys and values
        """

        first_reg = min([register['start'] for register in registers])
        num_regs = max([register['start'] + register['length'] for register in registers]) - first_reg
        values = self.instrument.read_registers(registeraddress=first_reg,
                                                number_of_registers=num_regs)
        return self._interpret_result(values, registers)

    def _batch_read(self, registers):
        """
        Read multiple registers in batches, limiting each batch to at most 125 registers.

        Arguments:
            * A list of registers (complete structs)

        Returns:
            * A dict containing all keys and values
        """
        # Count up to at most 128 registers:
        start_reg = registers[0]['start']
        batch = []
        results = {}
        for register in registers:
            if register['start'] + register['length'] - start_reg <= 125:
                batch.append(register)
            else:
                results.update(self._read_multiple(batch))
                batch = []
                batch.append(register)
                start_reg = register['start']

        results.update(self._read_multiple(batch))
        return results

    def _interpret_result(self, data, registers):
        """
        Pull the returned string apart and package the data back to its
        intended form.

        Arguments:
            * data: list of register values returned from the device
            * registers: the original requested set of registers

        Returns:
            * A dict containing the register names and resulting values

        """
        first_reg = min([register['start'] for register in registers])

        results = {}
        for register in registers:
            regname = register['name']
            start = register['start'] - first_reg
            end = start + register['length']
            values = data[start:end]
            results[regname] = self._convert_value(values=values,
                                                   signed=register['signed'],
                                                   number_of_decimals=register['decimals'],
                                                   is_float=register.get('is_float'))
        return results

    def _convert_value(self, values, signed=False, number_of_decimals=0, is_float=False):
        """
        Convert a list of returned integers to the intended value.

        Arguments:
            * bytestring: a list of integers that together represent the value
            * signed: whether the value is a signed value
            * decimals: number of decimals the return value should contain
        """
        if is_float:
            if len(values) == 2:
                bytestring = struct.pack('>HH', *values)
                return struct.unpack('>f', bytestring)[0]
            elif len(values) == 4:
                bytestring = struct.pack('>HHHH', *values)
                return struct.unpack('>d', bytestring)[0]

        number_of_registers = len(values)
        formatcode_i = '>'
        formatcode_o = '>'

        if number_of_registers == 1:
            formatcode_i += "H"
            if signed:
                formatcode_o += "h"
            else:
                formatcode_o += "H"

        if number_of_registers == 2:
            formatcode_i += 'HH'
            if signed:
                formatcode_o += "l"
            else:
                formatcode_o += "L"

        if number_of_registers == 4:
            formatcode_i += "HHHH"
            if signed:
                formatcode_o += "q"
            else:
                formatcode_o += "Q"

        bytestring = struct.pack(formatcode_i, *values)
        value = struct.unpack(formatcode_o, bytestring)[0]

        if value in ABBMeter.NULLS:
            return None
        else:
            return float(value) / 10 ** number_of_decimals

    NULLS = []
    REGS = []


class ABBMeter(ModbusRTUMeter):
    def __init__(self, port, baudrate=38400, slaveaddress=1, timeout=0.5, model=None):
        super().__init__(port, baudrate, slaveaddress, timeout)
        if model in ABBMeter.REGSETS:
            self.REGS = [register for register in ABBMeter._REGS if register['name'] in ABBMeter.REGSETS[model]]
        else:
            self.REGS = ABBMeter._REGS

    # Register map of the ABB A and B series energy meters.
    _REGS = [{'name': 'active_import',              'start': 20480, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'active_export',              'start': 20484, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'active_net',                 'start': 20488, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'reactive_import',            'start': 20492, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'reactive_export',            'start': 20496, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'reactive_net',               'start': 20500, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'apparent_import',            'start': 20504, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'apparent_export',            'start': 20508, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'apparent_net',               'start': 20512, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'active_import_co2',          'start': 20516, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'active_import_currency',     'start': 20532, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'active_import_tariff_1',     'start': 20848, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_import_tariff_2',     'start': 20852, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_import_tariff_3',     'start': 20856, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_import_tariff_4',     'start': 20860, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_export_tariff_1',     'start': 20880, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_export_tariff_2',     'start': 20884, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_export_tariff_3',     'start': 20888, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_export_tariff_4',     'start': 20892, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_import_tariff_1',   'start': 20912, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_import_tariff_2',   'start': 20916, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_import_tariff_3',   'start': 20920, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_import_tariff_4',   'start': 20924, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_export_tariff_1',   'start': 20944, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_export_tariff_2',   'start': 20948, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_export_tariff_3',   'start': 20952, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_export_tariff_4',   'start': 20956, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_import_l1',           'start': 21600, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_import_l2',           'start': 21604, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_import_l3',           'start': 21608, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_export_l1',           'start': 21612, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_export_l2',           'start': 21616, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_export_l3',           'start': 21620, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'active_net_l1',              'start': 21624, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'active_net_l2',              'start': 21628, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'active_net_l3',              'start': 21632, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'reactive_import_l1',         'start': 21636, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_import_l2',         'start': 21640, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_import_l3',         'start': 21644, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_export_l1',         'start': 21648, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_export_l2',         'start': 21652, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_export_l3',         'start': 21656, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'reactive_net_l1',            'start': 21660, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'reactive_net_l2',            'start': 21664, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'reactive_net_l3',            'start': 21668, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'apparent_import_l1',         'start': 21672, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'apparent_import_l2',         'start': 21676, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'apparent_import_l3',         'start': 21680, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'apparent_export_l1',         'start': 21684, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'apparent_export_l2',         'start': 21688, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'apparent_export_l3',         'start': 21692, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'apparent_net_l1',            'start': 21696, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'apparent_net_l2',            'start': 21700, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'apparent_net_l3',            'start': 21704, 'length': 4, 'signed': True, 'decimals': 2},
             {'name': 'resettable_active_import',   'start': 21804, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'resettable_active_export',   'start': 21808, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'resettable_reactive_import', 'start': 21812, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'resettable_reactive_export', 'start': 21816, 'length': 4, 'signed': False, 'decimals': 2},
             {'name': 'voltage_l1_n',               'start': 23296, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_l2_n',               'start': 23298, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_l3_n',               'start': 23300, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_l1_l2',              'start': 23302, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_l3_l2',              'start': 23304, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_l1_l3',              'start': 23306, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_l1',                 'start': 23308, 'length': 2, 'signed': False, 'decimals': 2},
             {'name': 'current_l2',                 'start': 23310, 'length': 2, 'signed': False, 'decimals': 2},
             {'name': 'current_l3',                 'start': 23312, 'length': 2, 'signed': False, 'decimals': 2},
             {'name': 'current_n',                  'start': 23314, 'length': 2, 'signed': False, 'decimals': 2},
             {'name': 'active_power_total',         'start': 23316, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'active_power_l1',            'start': 23318, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'active_power_l2',            'start': 23320, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'active_power_l3',            'start': 23322, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'reactive_power_total',       'start': 23324, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'reactive_power_l1',          'start': 23326, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'reactive_power_l2',          'start': 23328, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'reactive_power_l3',          'start': 23330, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'apparent_power_total',       'start': 23332, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'apparent_power_l1',          'start': 23334, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'apparent_power_l2',          'start': 23336, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'apparent_power_l3',          'start': 23338, 'length': 2, 'signed': True, 'decimals': 2},
             {'name': 'frequency',                  'start': 23340, 'length': 1, 'signed': False, 'decimals': 2},
             {'name': 'phase_angle_power_total',    'start': 23341, 'length': 1, 'signed': True, 'decimals': 1},
             {'name': 'phase_angle_power_l1',       'start': 23342, 'length': 1, 'signed': True, 'decimals': 1},
             {'name': 'phase_angle_power_l2',       'start': 23343, 'length': 1, 'signed': True, 'decimals': 1},
             {'name': 'phase_angle_power_l3',       'start': 23344, 'length': 1, 'signed': True, 'decimals': 1},
             {'name': 'phase_angle_voltage_l1',     'start': 23345, 'length': 1, 'signed': True, 'decimals': 1},
             {'name': 'phase_angle_voltage_l2',     'start': 23346, 'length': 1, 'signed': True, 'decimals': 1},
             {'name': 'phase_angle_voltage_l3',     'start': 23347, 'length': 1, 'signed': True, 'decimals': 1},
             {'name': 'phase_angle_current_l1',     'start': 23351, 'length': 1, 'signed': True, 'decimals': 1},
             {'name': 'phase_angle_current_l2',     'start': 23352, 'length': 1, 'signed': True, 'decimals': 1},
             {'name': 'phase_angle_current_l3',     'start': 23353, 'length': 1, 'signed': True, 'decimals': 1},
             {'name': 'power_factor_total',         'start': 23354, 'length': 1, 'signed': True, 'decimals': 3},
             {'name': 'power_factor_l1',            'start': 23355, 'length': 1, 'signed': True, 'decimals': 3},
             {'name': 'power_factor_l2',            'start': 23356, 'length': 1, 'signed': True, 'decimals': 3},
             {'name': 'power_factor_l3',            'start': 23357, 'length': 1, 'signed': True, 'decimals': 3},
             {'name': 'current_quadrant_total',     'start': 23358, 'length': 1, 'signed': False, 'decimals': 0},
             {'name': 'current_quadrant_l1',        'start': 23359, 'length': 1, 'signed': False, 'decimals': 0},
             {'name': 'current_quadrant_l2',        'start': 23360, 'length': 1, 'signed': False, 'decimals': 0},
             {'name': 'current_quadrant_l3',        'start': 23361, 'length': 1, 'signed': False, 'decimals': 0},
             {'name': 'voltage_harmonics_l1_n_thd', 'start': 23808, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_2nd', 'start': 23810, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_3rd', 'start': 23812, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_4th', 'start': 23814, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_5th', 'start': 23816, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_6th', 'start': 23818, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_7th', 'start': 23820, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_8th', 'start': 23822, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_9th', 'start': 23824, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_10th', 'start': 23826, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_11th', 'start': 23828, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_12th', 'start': 23830, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_13th', 'start': 23832, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_14th', 'start': 23834, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_15th', 'start': 23836, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_n_16th', 'start': 23838, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_thd', 'start': 23936, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_2nd', 'start': 23938, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_3rd', 'start': 23940, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_4th', 'start': 23942, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_5th', 'start': 23944, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_6th', 'start': 23946, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_7th', 'start': 23948, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_8th', 'start': 23950, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_9th', 'start': 23952, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_10th', 'start': 23954, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_11th', 'start': 23956, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_12th', 'start': 23958, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_13th', 'start': 23960, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_14th', 'start': 23962, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_15th', 'start': 23964, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l2_n_16th', 'start': 23966, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_thd', 'start': 24064, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_2nd', 'start': 24066, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_3rd', 'start': 24068, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_4th', 'start': 24070, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_5th', 'start': 24072, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_6th', 'start': 24074, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_7th', 'start': 24076, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_8th', 'start': 24078, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_9th', 'start': 24080, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_10th', 'start': 24082, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_11th', 'start': 24084, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_12th', 'start': 24086, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_13th', 'start': 24088, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_14th', 'start': 24090, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_15th', 'start': 24092, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_n_16th', 'start': 24094, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_thd', 'start': 24192, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_2nd', 'start': 24194, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_3rd', 'start': 24196, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_4th', 'start': 24198, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_5th', 'start': 24200, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_6th', 'start': 24202, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_7th', 'start': 24204, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_8th', 'start': 24206, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_9th', 'start': 24208, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_10th', 'start': 24210, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_11th', 'start': 24212, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_12th', 'start': 24214, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_13th', 'start': 24216, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_14th', 'start': 24218, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_15th', 'start': 24220, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l2_16th', 'start': 24222, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_thd', 'start': 24320, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_2nd', 'start': 24322, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_3rd', 'start': 24324, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_4th', 'start': 24326, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_5th', 'start': 24328, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_6th', 'start': 24330, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_7th', 'start': 24332, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_8th', 'start': 24334, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_9th', 'start': 24336, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_10th', 'start': 24338, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_11th', 'start': 24340, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_12th', 'start': 24342, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_13th', 'start': 24344, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_14th', 'start': 24346, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_15th', 'start': 24348, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l3_l2_16th', 'start': 24350, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_thd', 'start': 24448, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_2nd', 'start': 24450, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_3rd', 'start': 24452, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_4th', 'start': 24454, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_5th', 'start': 24456, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_6th', 'start': 24458, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_7th', 'start': 24460, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_8th', 'start': 24462, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_9th', 'start': 24464, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_10th', 'start': 24466, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_11th', 'start': 24468, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_12th', 'start': 24470, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_13th', 'start': 24472, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_14th', 'start': 24474, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_15th', 'start': 24476, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'voltage_harmonics_l1_l3_16th', 'start': 24478, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_thd', 'start': 24576, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_2nd', 'start': 24578, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_3rd', 'start': 24580, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_4th', 'start': 24582, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_5th', 'start': 24584, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_6th', 'start': 24586, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_7th', 'start': 24588, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_8th', 'start': 24590, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_9th', 'start': 24592, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_10th', 'start': 24594, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_11th', 'start': 24596, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_12th', 'start': 24598, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_13th', 'start': 24600, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_14th', 'start': 24602, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_15th', 'start': 24604, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l1_16th', 'start': 24606, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_thd', 'start': 24704, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_2nd', 'start': 24706, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_3rd', 'start': 24708, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_4th', 'start': 24710, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_5th', 'start': 24712, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_6th', 'start': 24714, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_7th', 'start': 24716, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_8th', 'start': 24718, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_9th', 'start': 24720, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_10th', 'start': 24722, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_11th', 'start': 24724, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_12th', 'start': 24726, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_13th', 'start': 24728, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_14th', 'start': 24730, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_15th', 'start': 24732, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l2_16th', 'start': 24734, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_thd', 'start': 24832, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_2nd', 'start': 24834, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_3rd', 'start': 24836, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_4th', 'start': 24838, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_5th', 'start': 24840, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_6th', 'start': 24842, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_7th', 'start': 24844, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_8th', 'start': 24846, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_9th', 'start': 24848, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_10th', 'start': 24850, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_11th', 'start': 24852, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_12th', 'start': 24854, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_13th', 'start': 24856, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_14th', 'start': 24858, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_15th', 'start': 24860, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_l3_16th', 'start': 24862, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_thd', 'start': 24960, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_2nd', 'start': 24962, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_3rd', 'start': 24964, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_4th', 'start': 24966, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_5th', 'start': 24968, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_6th', 'start': 24970, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_7th', 'start': 24972, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_8th', 'start': 24974, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_9th', 'start': 24976, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_10th', 'start': 24978, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_11th', 'start': 24980, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_12th', 'start': 24982, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_13th', 'start': 24984, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_14th', 'start': 24986, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_15th', 'start': 24988, 'length': 2, 'signed': False, 'decimals': 1},
             {'name': 'current_harmonics_n_16th', 'start': 24990, 'length': 2, 'signed': False, 'decimals': 1}]
    REGSETS = {
               "A43": ["active_import", "active_export", "active_net",
                       "reactive_import", "reactive_export", "reactive_net",
                       "apparent_import", "apparent_export", "apparent_net",
                       "active_import_co2", "active_import_currency",
                       "active_import_l1", "active_import_l2", "active_import_l3",
                       "active_export_l1", "active_export_l2", "active_export_l3",
                       "active_net_l1", "active_net_l2", "active_net_l3",
                       "reactive_import_l1", "reactive_import_l2",
                       "reactive_import_l3", "reactive_export_l1",
                       "reactive_export_l2", "reactive_export_l3",
                       "reactive_net_l1", "reactive_net_l2", "reactive_net_l3",
                       "apparent_import_l1", "apparent_import_l2",
                       "apparent_import_l3", "apparent_export_l1",
                       "apparent_export_l2", "apparent_export_l3",
                       "apparent_net_l1", "apparent_net_l2", "apparent_net_l3",
                       "voltage_l1_n", "voltage_l2_n", "voltage_l3_n",
                       "voltage_l1_l2", "voltage_l3_l2", "voltage_l1_l3",
                       "current_l1", "current_l2", "current_l3",
                       "active_power_total", "active_power_l1", "active_power_l2",
                       "active_power_l3", "reactive_power_total",
                       "reactive_power_l1", "reactive_power_l2",
                       "reactive_power_l3", "apparent_power_total",
                       "apparent_power_l1", "apparent_power_l2",
                       "apparent_power_l3", "frequency", "phase_angle_power_total",
                       "power_factor_total", "power_factor_l1", "power_factor_l2",
                       "power_factor_l3", "current_quadrant_total",
                       "current_quadrant_l1", "current_quadrant_l2",
                       "current_quadrant_l3"],
               "B23": ["active_import", "active_export", "active_net",
                       "reactive_import", "reactive_export", "reactive_net",
                       "apparent_import", "apparent_export", "apparent_net",
                       "active_import_co2", "active_import_currency",
                       "active_import_l1", "active_import_l2", "active_import_l3",
                       "active_export_l1", "active_export_l2", "active_export_l3",
                       "active_net_l1", "active_net_l2", "active_net_l3",
                       "reactive_import_l1", "reactive_import_l2",
                       "reactive_import_l3", "reactive_export_l1",
                       "reactive_export_l2", "reactive_export_l3",
                       "reactive_net_l1", "reactive_net_l2", "reactive_net_l3",
                       "apparent_import_l1", "apparent_import_l2",
                       "apparent_import_l3", "apparent_export_l1",
                       "apparent_export_l2", "apparent_export_l3",
                       "apparent_net_l1", "apparent_net_l2", "apparent_net_l3",
                       "voltage_l1_n", "voltage_l2_n", "voltage_l3_n",
                       "voltage_l1_l2", "voltage_l3_l2", "voltage_l1_l3",
                       "current_l1", "current_l2", "current_l3",
                       "active_power_total", "active_power_l1", "active_power_l2",
                       "active_power_l3", "reactive_power_total",
                       "reactive_power_l1", "reactive_power_l2",
                       "reactive_power_l3", "apparent_power_total",
                       "apparent_power_l1", "apparent_power_l2",
                       "apparent_power_l3", "frequency", "phase_angle_power_total",
                       "power_factor_total", "power_factor_l1", "power_factor_l2",
                       "power_factor_l3", "current_quadrant_total",
                       "current_quadrant_l1", "current_quadrant_l2",
                       "current_quadrant_l3"],
               "B21": ["active_import", "active_export", "active_net",
                       "reactive_import", "reactive_export", "reactive_net",
                       "apparent_import", "apparent_export", "apparent_net",
                       "active_import_co2", "active_import_currency",
                       "voltage_l1_n", "current_l1", "active_power_total",
                       "reactive_power_total", "apparent_power_total", "frequency",
                       "phase_angle_power_total", "power_factor_total",
                       "current_quadrant_total"]
               }
    NULLS = [pow(2, n) - 1 for n in (64, 63, 32, 31, 16, 15)]


class MEM001(ModbusRTUMeter):
    NULLS = []
    REGS = [{'name': 'feeder_1_id', 'start': 0x2005, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'feeder_2_id', 'start': 0x2006, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'feeder_3_id', 'start': 0x2007, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'feeder_4_id', 'start': 0x2008, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'feeder_5_id', 'start': 0x2009, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'feeder_6_id', 'start': 0x200A, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'feeder_7_id', 'start': 0x200B, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'feeder_8_id', 'start': 0x200C, 'length': 1, 'signed': False, 'decimals': 0},

            {'name': 'voltage_l1_n', 'start': 0xD006, 'length': 2, 'signed': False, 'decimals': 0, 'is_float': True},
            {'name': 'voltage_l2_n', 'start': 0xD008, 'length': 2, 'signed': False, 'decimals': 0, 'is_float': True},
            {'name': 'voltage_l3_n', 'start': 0xD00A, 'length': 2, 'signed': False, 'decimals': 0, 'is_float': True},

            {'name': 'current_l1', 'start': 0xD012, 'length': 2, 'signed': False, 'decimals': 0, 'is_float': True},
            {'name': 'current_l2', 'start': 0xD014, 'length': 2, 'signed': False, 'decimals': 0, 'is_float': True},
            {'name': 'current_l3', 'start': 0xD016, 'length': 2, 'signed': False, 'decimals': 0, 'is_float': True},

            {'name': 'active_power_total', 'start': 0xD01A, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'reactive_power_total', 'start': 0xD01C, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},

            {'name': 'active_power_l1', 'start': 0xD023, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'active_power_l2', 'start': 0xD025, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'active_power_l3', 'start': 0xD027, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},

            {'name': 'reactive_power_l1', 'start': 0xD029, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'reactive_power_l2', 'start': 0xD02B, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'reactive_power_l3', 'start': 0xD02D, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},

            {'name': 'power_factor_l1', 'start': 0xD035, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'power_factor_l2', 'start': 0xD037, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'power_factor_l3', 'start': 0xD039, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},

            {'name': 'current_harmonics_l1', 'start': 0xD044, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'current_harmonics_l2', 'start': 0xD046, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'current_harmonics_l3', 'start': 0xD048, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},

            {'name': 'current_harmonics_l1_3rd', 'start': 0xD052, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'current_harmonics_l2_3rd', 'start': 0xD054, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'current_harmonics_l3_3rd', 'start': 0xD056, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},

            {'name': 'current_harmonics_l1_5th', 'start': 0xD058, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'current_harmonics_l2_5th', 'start': 0xD05A, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'current_harmonics_l3_5th', 'start': 0xD05C, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},

            {'name': 'current_harmonics_l1_7th', 'start': 0xD05E, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'current_harmonics_l2_7th', 'start': 0xD060, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True},
            {'name': 'current_harmonics_l3_7th', 'start': 0xD062, 'length': 2, 'signed': True, 'decimals': 0, 'is_float': True}
           ]


class ModbusTCPMeter:
    """
    Implementation for a Modbus TCP Energy Meter.
    """
    def __init__(self, port, tcp_port=502, slaveaddress=126, model=None, baudrate=None):
        self.port = port
        self.tcp_port = tcp_port
        self.device_id = slaveaddress

    def read(self, regnames=None):
        if regnames is None:
            registers = self.REGS
            return self._read_multiple(registers)

        if type(regnames) is str:
            registers = [register for register in self.REGS if register['name'] == regnames]
            return self._read_single(registers[0])

        if type(regnames) is list:
            registers = [register for register in self.REGS if register['name'] in regnames]
            return self._read_multiple(registers)

    def _read_single(self, register):
        message = self._modbus_message(start_reg=register['start'], num_regs=register['length'])
        data = self._perform_request(message)
        return self._convert_value(data, signed=register['signed'], decimals=register['decimals'])

    def _read_multiple(self, registers):
        registers.sort(key=lambda reg: reg['start'])
        results = {}
        for reg_range in self._split_ranges(registers):
            first_reg = min([register['start'] for register in reg_range])
            num_regs = max([register['start'] + register['length'] for register in reg_range]) - first_reg
            message = self._modbus_message(start_reg=first_reg, num_regs=num_regs)
            data = self._perform_request(message)
            results.update(self._interpret_result(data, reg_range))
        return results

    def _split_ranges(self, registers):
        """
        Generator that splits the registers list into continuous parts.
        """
        reg_list = []
        prev_end = registers[0]['start'] - 1
        for r in registers:
            if r['start'] - prev_end > 1:
                yield reg_list
                reg_list = []
            reg_list.append(r)
            prev_end = r['start'] + r['length']
        yield reg_list


    def _modbus_message(self, start_reg, num_regs):
        transaction_id = random.randint(1, 2**16 - 1)
        return struct.pack(">HHHBBHH", transaction_id,
                                       self.PROTOCOL_CODE,
                                       6,
                                       self.device_id,
                                       self.FUNCTION_CODE,
                                       start_reg - self.REG_OFFSET,
                                       num_regs)

    def _perform_request(self, message):
        if self.device is None:
            self._connect()
        self.device.send(message)
        data = bytes()
        expect_bytes = 9 + 2 * struct.unpack(">H", message[-2:])[0]
        attempt = 1
        while len(data) is not expect_bytes:
            time.sleep(0.05)
            data += self.device.recv(2048)
            if attempt >= 10:
                return 2 * struct.unpack(">H", message[-2:])[0] * [0]
            attempt += 1
        return data[9:]

    def _interpret_result(self, data, registers):
        """
        Pull the returned string apart and package the data back to its
        intended form.

        Arguments:
            * data: list of register values returned from the device
            * registers: the original requested set of registers

        Returns:
            * A dict containing the register names and resulting values

        """
        first_reg = min([register['start'] for register in registers])

        results = {}
        for register in registers:
            regname = register['name']
            start = (register['start'] - first_reg) * 2
            end = start + register['length'] * 2
            values = data[start:end]
            results[regname] = self._convert_value(values=values,
                                                   signed=register['signed'],
                                                   decimals=register['decimals'],
                                                   is_float=register.get('is_float'))
            if regname == "power_factor_total" and results[regname] == 0:
                results[regname] = 1    # The SMA will send out a 0 when the power factor is 100%
        return results

    def _convert_value(self, values, signed=False, decimals=0, is_float=False):
        """
        Convert a list of returned integers to the intended value.

        Arguments:
            * bytestring: a list of integers that together represent the value
            * signed: whether the value is a signed value
            * decimals: number of decimals the return value should contain
            * is_float: whether the valie is a float

        """
        numberOfBytes = len(values)
        formatcode_o = '>'

        if is_float:
            formatcode_o += 'f'

        elif numberOfBytes == 1:
            if signed:
                formatcode_o += "b"
            else:
                formatcode_o += "B"

        elif numberOfBytes == 2:
            if signed:
                formatcode_o += "h"
            else:
                formatcode_o += "H"

        elif numberOfBytes == 4:
            if signed:
                formatcode_o += "l"
            else:
                formatcode_o += "L"


        value = struct.unpack(formatcode_o, bytes(values))[0]

        if value in self.NULLS:
            return None
        else:
            return float(value) / 10 ** decimals

    def _connect(self):
        self.device = socket.create_connection(address=(self.port, self.tcp_port))


class SMAMeter(ModbusTCPMeter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    REGS = [
            {'name': 'current_ac', 'start': 40188, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'current_l1', 'start': 40189, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'current_l2', 'start': 40190, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'current_l3', 'start': 40191, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'voltage_l1_l2', 'start': 40193, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'voltage_l2_l3', 'start': 40194, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'voltage_l3_l1', 'start': 40195, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'voltage_l1_n', 'start': 40196, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'voltage_l2_n', 'start': 40197, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'voltage_l3_n', 'start': 40198, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'active_power_total', 'start': 40200, 'length': 1, 'signed': True, 'decimals': -1},
            {'name': 'frequency', 'start': 40202, 'length': 1, 'signed': False, 'decimals': 2},
            {'name': 'apparent_power_total', 'start': 40204, 'length': 1, 'signed': True, 'decimals': -1},
            {'name': 'reactive_power_total', 'start': 40206, 'length': 1, 'signed': True, 'decimals': -1},
            {'name': 'power_factor_total', 'start': 40208, 'length': 1, 'signed': True, 'decimals': 3},
            {'name': 'active_export', 'start': 40210, 'length': 2, 'signed': False, 'decimals': 3},
            {'name': 'dc_power', 'start': 40217, 'length': 1, 'signed': False, 'decimals': -1},
            {'name': 'temperature_internal', 'start': 40219, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'temperature_other', 'start': 40222, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'operating_status', 'start': 40224, 'length': 1, 'signed': False, 'decimals': 0}
           ]

    REG_OFFSET = 1
    PROTOCOL_CODE = 0
    FUNCTION_CODE = 3
    NULLS = [2**16 - 1, 2**15 - 1, 2**15, -2**15]


class MulticubeMeter(ModbusTCPMeter):
    """
    Implementation for a Multicube energy meter over Modbus TCP.
    """
    def __init__(self, port, tcp_port=1502, slaveaddress=1, model=None, baudrate=None, auto_scale=True):
        super().__init__(port, tcp_port, slaveaddress, model, baudrate)
        self.device = socket.create_connection(address=(port, tcp_port))
        self.device_id = slaveaddress
        self.REGS = [
            {'name': 'energy_scale', 'start': 512, 'length': 2, 'decimals': 0, 'signed': True},
            {'name': 'active_net', 'start': 514, 'length': 2, 'decimals': 0, 'signed': True},
            {'name': 'apparent_net', 'start': 516, 'length': 2, 'decimals': 0, 'signed': True},
            {'name': 'reactive_net', 'start': 518, 'length': 2, 'decimals': 0, 'signed': True},
            {'name': 'active_power_total', 'start': 2816, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'apparent_power_total', 'start': 2817, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'reactive_power_total', 'start': 2818, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'power_factor_total', 'start': 2819, 'length': 1, 'decimals': 3, 'signed': True},
            {'name': 'frequency', 'start': 2820, 'length': 1, 'decimals': 1, 'signed': True},
            {'name': 'voltage_l1_n', 'start': 2821, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'current_l1', 'start': 2822, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'active_power_l1', 'start': 2823, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'voltage_l2_n', 'start': 2824, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'current_l2', 'start': 2825, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'active_power_l2', 'start': 2826, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'voltage_l3_n', 'start': 2827, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'current_l3', 'start': 2828, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'active_power_l3', 'start': 2829, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'power_factor_l1', 'start': 2830, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'power_factor_l2', 'start': 2831, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'power_factor_l3', 'start': 2832, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'voltage_l1_l2', 'start': 2833, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'voltage_l2_l3', 'start': 2834, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'voltage_l3_l1', 'start': 2835, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'current_n', 'start': 2836, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'amps_scale', 'start': 2837, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'phase_volts_scale', 'start': 2838, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'line_volts_scale', 'start': 2839, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'power_scale', 'start': 2840, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'apparent_power_l1', 'start': 3072, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'apparent_power_l2', 'start': 3073, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'apparent_power_l3', 'start': 3074, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'reactive_power_l1', 'start': 3075, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'reactive_power_l2', 'start': 3076, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'reactive_power_l3', 'start': 3077, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'peak_current_l1', 'start': 3078, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'peak_current_l2', 'start': 3079, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'peak_current_l3', 'start': 3080, 'length': 1, 'decimals': 0, 'signed': True},
            {'name': 'current_l1_thd', 'start': 3081, 'length': 1, 'decimals': 3, 'signed': True},
            {'name': 'current_l2_thd', 'start': 3082, 'length': 1, 'decimals': 3, 'signed': True},
            {'name': 'current_l3_thd', 'start': 3083, 'length': 1, 'decimals': 3, 'signed': True}
           ]
        if auto_scale:
            self.set_scaling()

    def set_scaling(self):
        """
        Call this function before reading anything to set up the correct scaling for this meter.
        """
        decimals_mapping = {1: 2, 2: 1, 3: 0, 4: -1, 5: -2, 6: -3, 7: -4}

        a_registers = ['current_l1', 'current_l2', 'current_l3', 'current_n']
        scale = int(self.read('amps_scale'))
        for r in self.REGS:
            if r['name'] in a_registers:
                r['decimals'] = decimals_mapping[scale]

        pv_registers = ['voltage_l1_n', 'voltage_l2_n', 'voltage_l3_n']
        scale = int(self.read('phase_volts_scale'))
        for r in self.REGS:
            if r['name'] in pv_registers:
                r['decimals'] = decimals_mapping[scale]

        lv_registers = ['voltage_l1_l2', 'voltage_l2_l3', 'voltage_l3_l1']
        scale = int(self.read('line_volts_scale'))
        for r in self.REGS:
            if r['name'] in lv_registers:
                r['decimals'] = decimals_mapping[scale]

        p_registers = ['active_power_total',
                       'reactive_power_total',
                       'apparent_power_total',
                       'active_power_l1',
                       'active_power_l2',
                       'active_power_l3',
                       'apparent_power_l1',
                       'apparent_power_l2',
                       'apparent_power_l3',
                       'reactive_power_l1',
                       'reactive_power_l2',
                       'reactive_power_l3']
        scale = int(self.read('power_scale'))
        for r in self.REGS:
            if r['name'] in p_registers:
                r['decimals'] = decimals_mapping[scale]

        e_registers = ['active_net', 'apparent_net', 'reactive_net']
        decimals_mapping = {3: 3, 4: 2, 5: 1, 6: 0, 7: -1}
        scale = int(self.read('energy_scale'))
        for r in self.REGS:
            if r['name'] in e_registers:
                r['decimals'] = decimals_mapping[scale]

    REG_OFFSET = 0
    PROTOCOL_CODE = 0
    FUNCTION_CODE = 3
    NULLS = [2**16 - 1, 2**15 - 1, 2**15, -2**15]


class AsyncModbusTCPMeter(ModbusTCPMeter):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reader = self.writer = None

    async def _connect(self):
        self.reader, self.writer = await asyncio.open_connection(host=self.port, port=self.tcp_port)

    async def read(self, regnames=None):
        if regnames is None:
            registers = self.REGS
            return await self._read_multiple(registers)

        if isinstance(regnames, str):
            registers = [register for register in self.REGS if register['name'] == regnames]
            if len(registers) == 0:
                return "Register not found on device."
            return await self._read_single(registers[0])

        if isinstance(regnames, Iterable):
            registers = [register for register in self.REGS if register['name'] in regnames]
            return await self._read_multiple(registers)

    async def _read_single(self, register):
        num_regs = register['length']
        message = self._modbus_message(register['start'], num_regs)
        if self.writer is None:
            await self._connect()
        self.writer.write(message)
        await self.writer.drain()

        data = await self.reader.readexactly(9 + 2 * num_regs)
        return self._convert_value(data[9:], signed=register['signed'], decimals=register['decimals'], is_float=register.get('is_float'))

    async def _read_multiple(self, registers):
        registers.sort(key=lambda reg: reg['start'])
        results = {}
        for reg_range in self._split_ranges(registers):
            # Prepare the request
            first_reg = min([register['start'] for register in reg_range])
            num_regs = max([register['start'] + register['length'] for register in reg_range]) - first_reg

            if self.writer is None:
                await self._connect()
            self.writer.write(self._modbus_message(first_reg, num_regs))
            await self.writer.drain()

            # Receive the response

            data = await self.reader.readexactly(9 + 2 * num_regs)
            results.update(self._interpret_result(data[9:], reg_range))
        return results


class AsyncABBTCPMeter(AsyncModbusTCPMeter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if model in ABBMeter.REGSETS:
            self.REGS = [register for register in ABBMeter._REGS if register['name'] in ABBMeter.REGSETS[model]]
        else:
            self.REGS = ABBMeter.REGS

    NULLS = ABBMeter.NULLS
    PROTOCOL_CODE = 0
    FUNCTION_CODE = 3
    REG_OFFSET = 0


class SaiaMeter:
    def __init__(self, port, baudrate=38400, slaveaddress=1, model=None, **kwargs):
        """ Initialize the ABBMeter object.

        Arguments:
            * port: a serial port (string)
            * baudrate: the baudrate to use (integer)
            * slaveaddress: the address of the modbus device (integer)
            * Specification of the type. Used to limit the registers to a specific set.

        Returns:
            * An ABBMeter object

        """
        self.instrument = tinysbus.Instrument(address=slaveaddress,
                                              serial_port=port,
                                              baudrate=baudrate,
                                              **kwargs)

    def read(self, regnames=None):
        """ Read one, many or all registers from the device

        Args:
            * regnames (str or list). If None, read all. If string, read
              single register. If list, read all registers from list.

        Returns:
            * If single register, it returns a single value. If all or list,
              return a dict with the keys and values.

        Raises:
            * KeyError, TypeError, IOError
        """
        if regnames is None:
            return self._batch_read(self.REGS)
        if type(regnames) is list:
            registers = [register for register in self.REGS if register['name'] in regnames]
            if len(registers) < len(regnames):
                regs_not_available = [regname for regname in regnames if regname not in \
                                      [register['name'] for register in self.REGS]]
                print("Warning: the following registers are not available on this device: " +
                      ", ".join(regs_not_available))
                print("The available registers are: %s" +
                      ", ".join(register['name'] for register in self.REGS))
            if len(registers) == 0:
                return {}
            registers.sort(key=lambda reg: reg['start'])
            return self._batch_read(registers)
        elif type(regnames) is str:
            registers = [register for register in self.REGS if register['name'] == regnames]
            if len(registers) == 0:
                raise ValueError("Register not found on device.")
            return self._read_single(registers[0])
        else:
            raise TypeError

    def _read_single(self, register):
        """
        Read a single register and return the value. Not to be called directly.

        Arguments:
        * register: a 'register' dict that contains info on the register.

        Returns:
        * The interpreted value from the meter.
        """

        if register['length'] == 1:
            return self.instrument.read_register(register_address=register['start'],
                                                 number_of_decimals=register['decimals'],
                                                 signed=register['signed'])
        if register['length'] == 2:
            value = self.instrument.read_long(register_address=register['start'],
                                              signed=register['signed'])
            return value / 10 ** register['decimals']

        if register['length'] == 4:
            value = self.instrument.read_registers(register_address=register['start'],
                                                   number_of_registers=register['length'])
            return self._convert_value(values=value,
                                       signed=register['signed'],
                                       number_of_decimals=register['decimals'])

    def _read_multiple(self, registers):
        """
        Read multiple registers from the slave device and return their values as a dict.

        Arguments:
            * A list of registers (complete structs)

        Returns:
            * A dict containing all keys and values
        """

        first_reg = min([register['start'] for register in registers])
        num_regs = max([register['start'] + register['length'] for register in registers]) - first_reg
        values = self.instrument.read_registers(register_address=first_reg,
                                                number_of_registers=num_regs)
        return self._interpret_result(values, registers)

    def _batch_read(self, registers):
        """
        Read multiple registers in batches, limiting each batch to at most 10 registers.

        Arguments:
            * A list of registers (complete structs)

        Returns:
            * A dict containing all keys and values
        """
        # Count up to at most 10 registers:
        start_reg = registers[0]['start']
        batch = []
        results = {}
        for register in registers:
            if register['start'] + register['length'] - start_reg <= 10:
                batch.append(register)
            else:
                results.update(self._read_multiple(batch))
                batch = []
                batch.append(register)
                start_reg = register['start']

        results.update(self._read_multiple(batch))
        return results

    def _interpret_result(self, data, registers):
        """
        Pull the returned string apart and package the data back to its
        intended form.

        Arguments:
            * data: list of register values returned from the device
            * registers: the original requested set of registers

        Returns:
            * A dict containing the register names and resulting values

        """
        first_reg = min([register['start'] for register in registers])

        results = {}
        for register in registers:
            regname = register['name']
            start = register['start'] - first_reg
            end = start + register['length']
            values = data[start:end]
            results[regname] = self._convert_value(values=values,
                                                   signed=register['signed'],
                                                   number_of_decimals=register['decimals'])
        return results

    def _convert_value(self, values, signed=False, number_of_decimals=0):
        """
        Convert a list of returned integers to the intended value.

        Arguments:
            * bytestring: a list of integers that together represent the value
            * signed: whether the value is a signed value
            * decimals: number of decimals the return value should contain
        """

        number_of_registers = len(values)
        formatcode_i = '>'
        formatcode_o = '>'

        if number_of_registers == 1:
            formatcode_i += "L"
            if signed:
                formatcode_o += "l"
            else:
                formatcode_o += "L"

        if number_of_registers == 2:
            formatcode_i += 'LL'
            if signed:
                formatcode_o += "q"
            else:
                formatcode_o += "Q"

        bytestring = struct.pack(formatcode_i, *values)
        value = struct.unpack(formatcode_o, bytestring)[0]

        return float(value) / 10 ** number_of_decimals

    REGISTER_BYTES = 4
    REGS = [{'name': 'firmware_version',           'start': 0, 'length': 1, 'signed': True, 'decimals': 0},
            {'name': 'num_registers',              'start': 1, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'num_flags',                  'start': 2, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'baudrate',                   'start': 3, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'serial_number',              'start': 11, 'length': 2, 'signed': False, 'decimals': 0},
            {'name': 'status_protect',             'start': 14, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'sbus_timeout',               'start': 15, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'sbus_address',               'start': 16, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'error_flags',                'start': 17, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'tariff',                     'start': 19, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'active_import_tariff_1',     'start': 20, 'length': 1, 'signed': False, 'decimals': 2},
            {'name': 'resettable_active_import_t1','start': 21, 'length': 1, 'signed': False, 'decimals': 2},
            {'name': 'active_import_tariff_2',     'start': 22, 'length': 1, 'signed': False, 'decimals': 2},
            {'name': 'resettable_active_import_t2','start': 23, 'length': 1, 'signed': False, 'decimals': 2},
            {'name': 'voltage_l1_n',               'start': 24, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'current_l1',                 'start': 25, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'active_power_l1',            'start': 26, 'length': 1, 'signed': True, 'decimals': 2},
            {'name': 'reactive_power_l1',          'start': 27, 'length': 1, 'signed': True, 'decimals': 2},
            {'name': 'power_factor_l1',            'start': 28, 'length': 1, 'signed': True, 'decimals': 2},
            {'name': 'voltage_l2_n',               'start': 29, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'current_l2',                 'start': 30, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'active_power_l2',            'start': 31, 'length': 1, 'signed': True, 'decimals': 2},
            {'name': 'reactive_power_l2',          'start': 32, 'length': 1, 'signed': True, 'decimals': 2},
            {'name': 'power_factor_l2',            'start': 33, 'length': 1, 'signed': True, 'decimals': 2},
            {'name': 'voltage_l3_n',               'start': 34, 'length': 1, 'signed': False, 'decimals': 0},
            {'name': 'current_l3',                 'start': 35, 'length': 1, 'signed': False, 'decimals': 1},
            {'name': 'active_power_l3',            'start': 36, 'length': 1, 'signed': True, 'decimals': 2},
            {'name': 'reactive_power_l3',          'start': 37, 'length': 1, 'signed': True, 'decimals': 2},
            {'name': 'power_factor_l3',            'start': 38, 'length': 1, 'signed': True, 'decimals': 2},
            {'name': 'active_power_total',         'start': 39, 'length': 1, 'signed': True, 'decimals': 2},
            {'name': 'reactive_power_total',       'start': 40, 'length': 1, 'signed': True, 'decimals': 2}]


