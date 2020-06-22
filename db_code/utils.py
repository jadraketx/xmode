import os.path
import sys
import logging
import configparser
import pandas as pd
import numpy as np
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, Float, Table, DateTime
from sqlalchemy.orm import relationship
Base = declarative_base()


#---
# Initialize logger
def initLogger(args):
    logging.basicConfig(format="%(message)s",level=logging.INFO,stream=sys.stdout)
    if args is None:
        return

    # allow user-provided loglevel (e.g. --log=DEBUG or --log=debug)
    valid_logLevels = ["DEBUG","INFO","WARNING","ERROR","CRITICAL"]
    if args.loglevel:
        args.loglevel=args.loglevel.upper()
        if args.loglevel not in valid_logLevels:
            ERROR("Invalid log level specified: %s" % args.loglevel)
        else:
            logger = logging.getLogger()
            logger.setLevel(args.loglevel)
            logging.debug("Enabled user-supplied log-level -> %s" % args.loglevel)

# Simple error wrapper to include exit
def ERROR(output):
    logging.error(output)
    sys.exit()


venue_venue_category = Table('venue_venue_category', Base.metadata,
                             Column('venue_id', ForeignKey('venue.id'), primary_key=True),
                             Column('venue_category_id', ForeignKey('venue_category.id'), primary_key=True)
                        )

class Venue(Base):
    __tablename__ = 'venue'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    venue_category = relationship('Venue_category', secondary = venue_venue_category,back_populates='venue')
    pings = relationship('Pings', back_populates = 'venue')

class Venue_category(Base):
    __tablename__ = 'venue_category'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    venue = relationship('Venue', secondary=venue_venue_category, back_populates = 'venue_category')

class Carrier(Base):
    __tablename__ = 'carrier'
    id = Column(Integer, primary_key=True)
    carrier_name = Column(String, nullable=True)
    device = relationship('Device', back_populates='carrier')

class Device_model(Base):
    __tablename__ = 'device_model'
    id = Column(Integer, primary_key=True)
    model_name = Column(String, nullable=True)
    device = relationship('Device', back_populates='model')

class Device(Base):
    __tablename__ = 'device'
    id = Column(String, primary_key=True)

    model_id = Column(Integer, ForeignKey('device_model.id'),nullable=True)
    model = relationship('Device_model', back_populates='device', uselist = True)

    carrier_name = Column(Integer, ForeignKey('carrier.id'), nullable=True)
    carrier = relationship('Carrier', back_populates='device', uselist=True)

    platform = Column(String, nullable=False)
    pings = relationship('Pings', back_populates='device')


class Pings(Base):
    __tablename__ = 'pings'
    id = Column(Integer, primary_key=True)
    device_id = Column(String, ForeignKey('device.id'), nullable=False)
    location_at = Column(Integer, nullable=False)
    timestamp = Column(DateTime(timezone=True),nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    altitude = Column(Float, nullable=True)
    horizontal_accuracy = Column(Float, nullable=True)
    vertical_accuracy = Column(Float, nullable=True)
    heading = Column(String, nullable=True)
    speed = Column(Float, nullable=True)
    ipv_4 = Column(String, nullable=True)
    ipv_6 = Column(String, nullable=True)
    final_country = Column(String, nullable=True)
    user_agent = Column(String, nullable=True)
    background = Column(String, nullable=True)
    publisher_id = Column(String, nullable=True)
    wifi_ssid = Column(String, nullable=True)
    wifi_bssid = Column(String, nullable=True)

    venue_id = Column(Integer, ForeignKey('venue.id'), nullable=True)
    dwell_time = Column(Float,nullable=True)
    source = Column(Integer,nullable=False)

    device = relationship("Device", back_populates = 'pings')
    venue = relationship(Venue, back_populates = 'pings')

class FileManager():
    def __init__(self,wd):
        #check if files_to_process.csv exists
        self.wd = wd
        self.files_to_process_path = self.wd + 'files_to_process.csv'
        #self.files_to_process = pd.DataFrame(columns=['index','path','status'], dtype={'index':'int','path':'str','status':'int'})
        if os.path.exists(self.files_to_process_path):
            self.files_to_process = pd.read_csv(self.files_to_process_path, usecols=['index','path','status','nrow'],dtype={'index':'int','path':'str','status':'int','nrow':'int'})
        else:
            logging.ERROR("files_to_process.csv not found: required")

        self.file_errors_path = self.wd + 'error_files.csv'
        self.file_errors = pd.DataFrame({
                    'index': pd.Series([], dtype='int'),
                    'path': pd.Series([], dtype='str'),
                    'status': pd.Series([], dtype='int')})
        if os.path.exists(self.file_errors_path):
            self.file_errors = pd.read_csv(self.file_errors_path)

    def update_file_list(self):
        #write updated files_to_process_path
        self.files_to_process.to_csv(self.files_to_process_path,sep=",",header=True,index=False)
        #write list of files with errors
        self.file_errors.to_csv(self.file_errors_path,sep=",",header=True,index=False)