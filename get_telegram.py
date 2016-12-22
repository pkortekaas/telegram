#! /usr/bin/env python

# Python script to retrieve and parse a DSMR2 or DSMR4 telegram from a P1 port
# requires pyserial, crcmod

import sys
import os
import re
from datetime import datetime, timedelta
import sqlite3 as db
import serial
import crcmod.predefined

SIMULATION = True   # Use serial or file as input
DEBUGGING = 0       # Show extra output
DSRM_VERSION = 4    # dsrm version of telegram 2 or 4
SERIAL_PORT = '/dev/ttyUSB0'
DB_FILE = r'F:\Data\telegram.db'

# The true telegram ends with an exclamation mark after a CR/LF
CHECKSUM_PATTERN = re.compile('\r\n(?=!)')
TIMESTAMP_PATTERN = re.compile(r'(?P<Code>0-0:1\.0\.0)\((?P<Value>[0-9]{12})(?P<Timezone>[SW])\)')
ELECTRICITY_PATTERN = re.compile(r'(?P<Code>1-0:1\.(8\.1|8\.2|7\.0))\((?P<Value>[0-9.]+)\*kW')
GAS_PATTERN4 = re.compile(r'(?P<Code>0-1:24\.2\.1)\((?P<Timestamp>[0-9]{12})'
                          r'(?P<Timezone>[SW])\)\((?P<Value>[0-9\.]+)\*m3\)')
GAS_PATTERN2 = re.compile(r'(?P<Code>0-1:24\.3\.0)\((?P<Timestamp>[0-9]{12}).*\r\n\('
                          r'(?P<Value>[0-9.]+)\)')
CRC16 = crcmod.predefined.mkPredefinedCrcFun('crc16')

class Telegram(object):

    TIMESTAMP = '0-0:1.0.0'
    TARIFF1 = '1-0:1.8.1'
    TARIFF2 = '1-0:1.8.2'
    ACTUAL = '1-0:1.7.0'
    GAS4 = '0-1:24.2.1'
    GAS2 = '0-1:24.3.0'

    def __init__(self):
        self.__timestamp = int((datetime.utcnow()-datetime(1970, 1, 1)).total_seconds())
        self.__localtime = datetime.now().strftime('%y%m%d%H%M%S')
        self.__actual = 0.0
        self.__tariff1 = 0.0
        self.__tariff2 = 0.0
        self.__gas = 0.0

    @property
    def localtime(self):
        return self.__localtime

    @localtime.setter
    def localtime(self, localtime):
        self.__localtime = localtime

    @property
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, timestamp):
        self.__timestamp = int(timestamp)

    @property
    def actual(self):
        return self.__actual

    @actual.setter
    def actual(self, actual):
        self.__actual = float(actual)

    @property
    def tariff1(self):
        return self.__tariff1

    @tariff1.setter
    def tariff1(self, tariff1):
        self.__tariff1 = float(tariff1)

    @property
    def tariff2(self):
        return self.__tariff2

    @tariff2.setter
    def tariff2(self, tariff2):
        self.__tariff2 = float(tariff2)

    @property
    def gas(self):
        return self.__gas

    @gas.setter
    def gas(self, gas):
        self.__gas = float(gas)

    def to_sql(self):
        return (('insert into Telegram (timestamp,actual,tar1,tar2,gas) '
                'values ({0},{1},{2},{3},{4});')
                .format(self.timestamp, self.actual, self.tariff1, self.tariff2, self.gas))

    def to_string(self):
        return ('Timestamp: {0}\r\nActual: {1}\r\nTariff 1: {2}\r\nTariff 2: {3}\r\nGas: {4}\r\n'
                .format(self.localtime, self.actual, self.tariff1, self.tariff2, self.gas))

    def to_json(self):
        return (('{{"Telegrams":[{{"Timestamp":"{0}", "Actual":"{1}", "Tariff1":"{2}", '
                '"Tariff2":"{3}", "Gas":"{4}"}}]}}')
                .format(self.localtime, self.actual, self.tariff1, self.tariff2, self.gas))                

    def to_xml(self):
        return (('<Telegram><Timestamp>{0}</Timestamp><Actual>{1}</Actual><Tariff1>{2}</Tariff1>'
                '<Tariff2>{3}</Tariff2><Gas>{4}</Gas></Telegram>')
                .format(self.localtime, self.actual, self.tariff1, self.tariff2, self.gas))

class DbWriter(object):

    __SQL_CREATE_TABLE = \
"""create table Telegram (
timestamp INT PRIMARY KEY not null,
actual REAL not null,
tar1 REAL not null,
tar2 REAL not null,
gas REAL not null);"""

    def __init__(self, database):
        create_table = not os.path.exists(database)
        self.__con = db.connect(database)
        self.__cur = self.__con.cursor()
        if create_table:
            self.write(self.__SQL_CREATE_TABLE)

    def write(self, sql, autocommit=True):
        self.__cur.execute(sql)
        if autocommit:
            self.__con.commit()

    def close(self):
          self.__con.close()

def get_telegram(ser):

    telegram = ''
    checksum_found = False
    begin_found = False
    good_checksum = True
    result = Telegram()

    # Read in all the lines until we find the checksum
    # line starting with an exclamation mark
    while not begin_found:
        telegram_line = ser.readline().decode('ascii')
        if telegram_line.startswith('/'):
            telegram = telegram_line
            begin_found = True
    while not checksum_found:
        telegram_line = ser.readline().decode('ascii')
        if telegram_line.startswith('!'):
            telegram = telegram + telegram_line
            if DEBUGGING:
                print('Found checksum!')
            checksum_found = True
        else:
            telegram = telegram + telegram_line

    # Look for the checksum in the telegram
    if DSRM_VERSION == 4:
        for m in CHECKSUM_PATTERN.finditer(telegram):
            given_checksum = int('0x' + telegram[m.end() + 1:], 16)
            # The exclamation mark is also part of the text to be CRC16'd
            calculated_checksum = CRC16(telegram[:m.end() + 1].encode('ascii'))
            good_checksum = given_checksum == calculated_checksum

    if good_checksum:
        for telegram_line in telegram.split('\r\n'):
            m = re.match(TIMESTAMP_PATTERN, telegram_line)
            if m:
                dt = (datetime.strptime(m.group('Value'), '%y%m%d%H%M%S') -
                    timedelta(hours = 1 if m.group('Timezone') == 'W' else 2))
                result.localtime = m.group('Value')
                result.timestamp = (dt-datetime(1970,1,1)).total_seconds()
            else:
                m = re.match(ELECTRICITY_PATTERN, telegram_line)
                if m:
                    if m.group('Code') == Telegram.ACTUAL:
                        result.actual = m.group('Value')
                    elif m.group('Code') == Telegram.TARIFF1:
                        result.tariff1 = m.group('Value')
                    elif m.group('Code') == Telegram.TARIFF2:
                        result.tariff2 = m.group('Value')
                elif DSRM_VERSION == 4:
                    m = re.match(GAS_PATTERN4, telegram_line)
                    if m:
                        result.gas = m.group('Value')
        if DSRM_VERSION == 2:
            m = re.search(GAS_PATTERN2, telegram)
            if m:
                result.gas = m.group('Value')

    return result

def main():

    ser = None
    writer = None

    try:
        writer = DbWriter(DB_FILE)

        if not SIMULATION:
            ser = serial.Serial()
            ser.baudrate = 115200
            ser.bytesize = serial.EIGHTBITS
            ser.parity = serial.PARITY_NONE
            ser.stopbits = serial.STOPBITS_ONE
            ser.xonxoff = 0
            ser.rtscts = 1
            ser.timeout = 12
            ser.port = SERIAL_PORT
            ser.open()
        else:
            print("Running in simulation mode")
            telegram_file = 'telegram2.dat' if DSRM_VERSION == 2 else 'telegram4.dat'
            ser = open(telegram_file, 'rb')

        telegram = get_telegram(ser)
        print(telegram.to_sql())
        print(telegram.to_json())
        print(telegram.to_xml())
        print(telegram.to_string())
        #writer.write(telegram.to_sql())

    except db.Error as dex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(dex).__name__, dex.args)
        print(message)
        sys.exit("Database Exception %s. Program aborted." % dex.args)
    except serial.SerialException as sex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(sex).__name__, sex.args)
        print(message)
        sys.exit("Serial Exception %s. Program aborted." % ser.name)
    except Exception as ex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        sys.exit("Exception %s. Program aborted.") % ex
    finally:
        if ser:
            ser.close()
        if writer:
            writer.close()

if __name__ == '__main__':
    main()
