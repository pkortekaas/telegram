#! /usr/bin/env python

# Python script to retrieve and parse a DSMR4 telegram from a P1 port

import sys
import os
import re
from datetime import datetime, timedelta
import sqlite3 as db
import serial
import paho.mqtt.client as mqtt
import crcmod.predefined


SIMULATION = False  # Use serial or file as input
SERIAL_PORT = '/dev/ttyUSB0'
DB_FILE = './telegram.db'
USERNAME = 'DVES_USER'
PASSWORD = '********'
HOSTNAME = 'host'
CLIENT = 'p1-client'
TOPIC = 'dsmr/4.0/datagram'

# The true telegram ends with an exclamation mark after a CR/LF
CHECKSUM_PATTERN = re.compile('\r\n(?=!)')
TIMESTAMP_PATTERN = re.compile(r'(?P<Code>0-0:1\.0\.0)\((?P<Value>[0-9]{12})(?P<Timezone>[SW])\)')
ELECTRICITY_PATTERN = re.compile(r'(?P<Code>1-0:1\.(8\.1|8\.2|7\.0))\((?P<Value>[0-9.]+)\*kW')
GAS_PATTERN4 = re.compile(r'(?P<Code>0-1:24\.2\.1)\((?P<Timestamp>[0-9]{12})'
                          r'(?P<Timezone>[SW])\)\((?P<Value>[0-9\.]+)\*m3\)')
CRC16 = crcmod.predefined.mkPredefinedCrcFun('crc16')

class Telegram():
    # pylint: disable=too-many-instance-attributes
    TARIFF1 = '1-0:1.8.1'
    TARIFF2 = '1-0:1.8.2'
    ACTUAL = '1-0:1.7.0'

    def __init__(self):
        self.__timestamp = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())
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
        return (f'insert into Telegram (timestamp,actual,tar1,tar2,gas) '
                f'values ({self.timestamp},{self.actual},{self.tariff1},{self.tariff2},{self.gas});')

    def to_json(self, week):
        return (f'{{"Telegram":{{"Timestamp":"{self.localtime}", "Actual":"{self.actual}", "Week":"{week}", '
                f'"Tariff1":"{self.tariff1}", "Tariff2":"{self.tariff2}", "Gas":"{self.gas}"}}}}')

class DbWriter():

    __SQL_CREATE_TABLE = \
        """create table Telegram (
timestamp INT PRIMARY KEY not null,
actual REAL not null,
tar1 REAL not null,
tar2 REAL not null,
gas REAL not null);"""

    __SQL_THIS_WEEK = \
        """select
round(IfNull(max(tar1)+max(tar2) - (min(tar1)+min(tar2)),0),2) as [kWh]
from Telegram
where strftime('%Y', datetime(timestamp, 'unixepoch', 'localtime')) = strftime('%Y', datetime('now'))
and strftime('%W', datetime(timestamp, 'unixepoch', 'localtime')) = strftime('%W', datetime('now'));"""

    def __init__(self, database):
        create_table = not os.path.exists(database)
        self.con = db.connect(database)
        self.cur = self.con.cursor()
        if create_table:
            self.write(self.__SQL_CREATE_TABLE)

    def write(self, sql, autocommit=True):
        self.cur.execute(sql)
        if autocommit:
            self.con.commit()

    def this_week(self):
        self.cur.execute(self.__SQL_THIS_WEEK)
        return self.cur.fetchone()[0]

    def close(self):
        self.con.close()

def get_telegram(ser):

    telegram = ''
    checksum_found = False
    begin_found = False
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
        telegram = telegram + telegram_line
        checksum_found = telegram_line.startswith('!')

    # Look for the checksum in the telegram
    for m in CHECKSUM_PATTERN.finditer(telegram):
        given_checksum = int('0x' + telegram[m.end() + 1:], 16)
        # The exclamation mark is also part of the text to be CRC16'd
        calculated_checksum = CRC16(telegram[:m.end() + 1].encode('ascii'))
        if given_checksum != calculated_checksum:
            raise ValueError("Checksum error")

    for telegram_line in telegram.split('\r\n'):
        m = re.match(TIMESTAMP_PATTERN, telegram_line)
        if m:
            dt = (datetime.strptime(m.group('Value'), '%y%m%d%H%M%S') -
                  timedelta(hours=1 if m.group('Timezone') == 'W' else 2))
            result.localtime = m.group('Value')
            result.timestamp = (dt - datetime(1970, 1, 1)).total_seconds()
            continue
        m = re.match(ELECTRICITY_PATTERN, telegram_line)
        if m:
            if m.group('Code') == Telegram.ACTUAL:
                result.actual = m.group('Value')
            elif m.group('Code') == Telegram.TARIFF1:
                result.tariff1 = m.group('Value')
            elif m.group('Code') == Telegram.TARIFF2:
                result.tariff2 = m.group('Value')
            continue
        m = re.match(GAS_PATTERN4, telegram_line)
        if m:
            result.gas = m.group('Value')

    return result

def send_mqtt(message):
    client = mqtt.Client(CLIENT)
    client.username_pw_set(USERNAME, PASSWORD)
    client.connect(HOSTNAME)
    client.publish(TOPIC, message)
    client.disconnect()

def main():

    ser = None
    writer = None
    sql = True
    template = "An exception of type {0} occured. Arguments:\n{1!r}"

    try:
        writer = DbWriter(DB_FILE)
        if len(sys.argv) == 2 and sys.argv[1] == '--json':
            sql = False

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
            telegram_file = 'telegram4.dat'
            ser = open(telegram_file, 'rb')

        telegram = get_telegram(ser)
        if sql:
            writer.write(telegram.to_sql())
        else:
            send_mqtt(telegram.to_json(writer.this_week()))

    except db.Error as dex:
        print(template.format(type(dex).__name__, dex.args))
        sys.exit(f"Database Exception {dex.args}. Program aborted.")
    except serial.SerialException as sex:
        print(template.format(type(sex).__name__, sex.args))
        sys.exit(f"Serial Exception {ser.name}. Program aborted.")
    except Exception as ex:  # pylint: disable=broad-except
        print(template.format(type(ex).__name__, ex.args))
        sys.exit(f"Exception {ex}. Program aborted.")
    finally:
        if ser:
            ser.close()
        if writer:
            writer.close()

if __name__ == '__main__':
    main()
