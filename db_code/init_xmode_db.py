import pandas as pd
import numpy as np
import psycopg2
import argparse
import utils
from utils import Base, Venue, Venue_category, Device, Carrier, Device_model, Pings
import logging
import pickle
from config import *
import time
import glob
import io
import csv, gzip
from datetime import datetime
from timezonefinder import TimezoneFinder
from pytz import timezone, utc
from pytz.exceptions import UnknownTimeZoneError

def init_venue_tables():

    logging.info("Initializing venue and venue category tables")
    logging.info(f"\tLoading: {aux_tables_dir + 'xmode_venue_masterlist.pickle'}" )

    t1 = time.perf_counter()

    with open(aux_tables_dir + 'xmode_venue_masterlist.pickle','rb') as f:
        venues_tab = pickle.load(f)

    #get unique categories
    v_cat_tab = {}
    c = 1
    for v in venues_tab:
        temp = venues_tab[v]
        for k in temp:
            if k not in v_cat_tab:
                v_cat_tab[k] = Venue_category(name=k)
                c = c + 1


    for v in venues_tab:
        temp_cats = venues_tab[v]
        venue = Venue(name = v)

        for l in temp_cats:
            if v_cat_tab[l]:
                venue.venue_category.append(v_cat_tab[l])
        session.add(venue)


    for k in v_cat_tab:
        session.add(v_cat_tab[k])

    #t = session.query(Venue_category.name).filter_by(name = 'Businesses and Services')
    session.commit()

    logging.info("Pushing to database")
    t2 = time.perf_counter()
    logging.info(f"\tComplete: {t2-t1:0.4f} seconds\n")

def init_device_tables():

    logging.info("Loading master list of devices")
    logging.info(f"\tLoading: {aux_tables_dir + 'xmode_tx_device_masterlist.pickle'}")

    with open(aux_tables_dir + 'xmode_tx_device_masterlist.pickle','rb') as f:
        device_tab = pickle.load(f)

    #construct set of unique device_models and carriers
    device_models = {}
    carriers = {}

    t1 = time.perf_counter()
    c1 = 0
    c2 = 1
    for d in device_tab:
        l = device_tab[d]
        temp_platform = l[0]
        temp_carrier = l[1]
        temp_model = l[2]
        if temp_carrier not in carriers:
            carriers[temp_carrier] = Carrier(carrier_name=temp_carrier)
            session.add(carriers[temp_carrier])
        if temp_model not in device_models:
            device_models[temp_model] = Device_model(model_name=temp_model)
            session.add(device_models[temp_model])



    session.commit()

    n = len(device_tab)
    for d in device_tab:
        l = device_tab[d]
        temp_platform = l[0]
        temp_carrier = l[1]
        temp_model = l[2]

        dev = Device(id=d, platform=temp_platform)
        dev.carrier.append(carriers[temp_carrier])
        dev.model.append(device_models[temp_model])
        session.add(dev)
        c1 = c1 + 1
        if c1 % 10000 == 0:
            print(f"\t{100*c1/n:0.4f} complete")
            session.commit()

    session.commit()
    t2 = time.perf_counter()
    logging.info(f"\tComplete: {t2-t1:0.4f} seconds\n")

def get_header_index(header):
    inds = {}
    try:
        inds['advertiser_id'] = header.index('advertiser_id')
        inds['location_at'] = header.index('location_at')
        inds['latitude'] = header.index('latitude')
        inds['longitude'] = header.index('longitude')
        inds['altitude'] = header.index('altitude')
        inds['horizontal_accuracy'] = header.index('horizontal_accuracy')
        inds['vertical_accuracy'] = header.index('vertical_accuracy')
        inds['heading'] = header.index('heading')
        inds['speed'] = header.index('speed')
        inds['ipv_4'] = header.index('ipv_4')
        inds['ipv_6'] = header.index('ipv_6')
        inds['final_country'] = header.index('final_country')
        inds['user_agent'] = header.index('user_agent')
        inds['background'] = header.index('background')
        inds['publisher_id'] = header.index('publisher_id')
        inds['wifi_ssid'] = header.index('wifi_ssid')
        inds['wifi_bssid'] = header.index("wifi_bssid")
        inds['venue_name'] = header.index('venue_name')
        inds['dwell_time'] = header.index('dwell_time')
    except:
        utils.ERROR("File does not contain expected header in first row")

    return(inds)

def get_venue_table():
    venue_tab = {}
    for v in session.query(Venue).order_by(Venue.id):
        venue_tab[v.name] = v.id
    return(venue_tab)



def init_ping_table3(fileManager, num_files):

    logging.info("Importing raw data")
    venue_tab = get_venue_table()

    # Open connection
    # The SQLAlchemy connection to the database
    conn = engine.connect()
    # A raw psycopg2 DB connection
    raw_conn = engine.raw_connection()
    # A cursor using the raw connection
    cur = raw_conn.cursor()
    cur.execute('SELECT version()')
    print(cur.fetchone()[0])


    file_tab = fileManager.files_to_process
    n = file_tab.shape[0]
    num_files_processed = 0
    for f in range(0,n):

        index = file_tab.iloc[f,0]
        inFile = file_tab.iloc[f,1]
        status = file_tab.iloc[f,2]
        try:
            if status == 0: #hasn't been processed yet
                rid_start = 0

                #get index range
                if index == 0:
                    #processing very first file
                    rid_start = 1
                else:
                    rid_start = file_tab.iloc[(f-1),3] + 1

                t1 = time.perf_counter()
                logging.info(f"Loading {inFile}")
                dat = pd.read_csv(
                    inFile,
                    usecols = [e for e in ping_dtypes],
                    dtype = ping_dtypes,
                    escapechar="\\"
                )

                # string formatting
                dat['wifi_ssid'] = dat['wifi_ssid'].str.replace('"', '')
                dat['wifi_ssid'] = dat['wifi_ssid'].str.strip()
                dat['wifi_ssid'] = dat['wifi_ssid'].replace('<unknown ssid>', np.nan)
                dat['wifi_bssid'] = dat['wifi_bssid'].replace('<unknown bssid>', np.nan)
                #dat.replace(r'^\s*$', np.nan, regex=True, inplace=True)


                dat = dat.fillna('Null')
                dat.insert(0, 'rid', range(rid_start, rid_start + len(dat)))
                dat.insert(len(dat.columns),'source',[index]*len(dat))
                #add additional datetime unaware
                dat.insert(3, 'timestamp_notz', pd.to_datetime(dat['location_at'], unit='s'))

                logging.info(f"\tindex: {index}\trid_start: {rid_start}\trid_end:{rid_start + len(dat)}")

                # venue id
                venue = dat['venue_name']
                v_id = []
                for v in venue:
                    if v == 'Null':
                        v_id.append('Null')
                    else:
                        if v in venue_tab:
                            v_id.append(str(venue_tab[v]))
                        else:
                            logging.error("Venue " + v + " not found in database")
                dat['venue_name'] = v_id

                #write to csv in memory
                output = io.StringIO()
                dat.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)

                t2 = time.perf_counter()
                logging.info(f"\tComplete: {t2-t1:0.4f} seconds\n")
                logging.info("\tPushing to db")

                t1 = time.perf_counter()
                #push to db
                cur.copy_from(output, 'pings', sep='\t', null='Null')
                # Commit inserts to DB
                raw_conn.commit()

                t2 = time.perf_counter()
                logging.info(f"\tComplete: {t2-t1:0.4f} seconds\n")

                num_files_processed = num_files_processed + 1
                #update status and nrow
                fileManager.files_to_process.iloc[f,2] = 1
                fileManager.files_to_process.iloc[f,3] = rid_start + len(dat)

                if num_files_processed == num_files:
                    break
        except psycopg2.DatabaseError as e:

            logging.info(f"DB ERROR")
            logging.info(f"\tFile: {inFile}")
            logging.info(f"\tadding record to {fileManager.file_errors_path}")
            if raw_conn:
                raw_conn.rollback()
            fileManager.file_errors = fileManager.file_errors.append(
                {'index':index, 'path':inFile,'status':0}, ignore_index=True
            )

            logging.info(f"\tremoving record from {fileManager.files_to_process_path}")
            ind = fileManager.files_to_process.index[f]
            fileManager.files_to_process=fileManager.files_to_process.drop(ind)

            fileManager.update_file_list()
            logging.ERROR(f'Error {e}')
        #end status
    fileManager.update_file_list()





def init_ping_table2(fileList):
    venue_tab = get_venue_table()
    tf = TimezoneFinder()

    ins = Pings.__table__.insert()

    # Open connection
    # The SQLAlchemy connection to the database
    conn = engine.connect()
    # A raw psycopg2 DB connection
    raw_conn = engine.raw_connection()
    # A cursor using the raw connection
    cur = raw_conn.cursor()
    cur.execute('SELECT version()')
    print(cur.fetchone()[0])


    t1 = time.perf_counter()
    # assumption: first row of data must be header
    inFile = fileList[0]
    inds = {}

    # Text output object
    output = io.StringIO()
    writer = csv.writer(output)

    data_to_insert = []
    c = 0
    with gzip.open(inFile, 'rt') as f:
        # csv_reader = csv.reader(f,escapechar='\\')
        csv_reader = csv.reader((x.replace('\0', '') for x in f), escapechar='\\')
        for line in csv_reader:
            if c == 0:
                inds = get_header_index(line)
            else:




                advertiser_id = line[inds['advertiser_id']]
                location_at = line[inds['location_at']]
                latitude = line[inds['latitude']]
                longitude = line[inds['longitude']]
                altitude = line[inds['altitude']]
                horizontal_accuracy = line[inds['horizontal_accuracy']]
                vertical_accuracy = line[inds['vertical_accuracy']]
                heading = line[inds['heading']]
                speed = line[inds['speed']]
                ipv_4 = line[inds['ipv_4']]
                ipv_6 = line[inds['ipv_6']]
                final_country = line[inds['final_country']]
                user_agent = line[inds['user_agent']]
                background = line[inds['background']]
                publisher_id = line[inds['publisher_id']]
                wifi_ssid = line[inds['wifi_ssid']]
                wifi_bssid = line[inds['wifi_bssid']]
                venue_name = line[inds['venue_name']]
                dwell_time = line[inds['dwell_time']]

                # only take records with lat,lng, and time data
                if latitude != '' or longitude != '' or location_at != '':
                    latitude = float(latitude)
                    longitude = float(longitude)
                    location_at = int(location_at)

                    try:
                        # time zone
                        #tz_name = tf.timezone_at(lng=longitude, lat=latitude)
                        #tz = timezone(tz_name)
                        timestamp = datetime.utcfromtimestamp(location_at)
                        #timestamp = timestamp.replace(tzinfo=tz)

                        # venue
                        # if there is a venue get its id from venue_tab
                        venue_id = 'Null'
                        if venue_name in venue_tab:
                            venue_id = venue_tab[venue_name]

                        # convert strings to floats
                        if altitude != '':
                            altitude = float(altitude)
                        else:
                            altitude = 'Null'

                        if horizontal_accuracy != '':
                            horizontal_accuracy = float(horizontal_accuracy)
                        else:
                            horizontal_accuracy = 'Null'

                        if vertical_accuracy != '':
                            vertical_accuracy = float(vertical_accuracy)
                        else:
                            vertical_accuracy = 'Null'

                        if speed != '':
                            speed = float(speed)
                        else:
                            speed = 'Null'

                        if dwell_time != '':
                            dwell_time = float(dwell_time)
                        else:
                            dwell_time = 'Null'

                        # convert empty strings to none
                        if heading == '':
                            heading = 'Null'
                        else:
                            heading = heading.replace('\t',' ').strip()

                        if ipv_4 == '':
                            ipv_4 = 'Null'
                        else:
                            ipv_4 = ipv_4.replace('\t',' ').strip()

                        if ipv_6 == '':
                            ipv_6 = 'Null'
                        else:
                            ipv_6 = ipv_6.replace('\t', ' ').strip()

                        if final_country == '':
                            final_country = 'Null'
                        else:
                            final_country = final_country.replace('\t',' ').strip()

                        if user_agent == '':
                            user_agent = 'Null'
                        else:
                            user_agent = user_agent.replace('\t', ' ').strip()

                        if background == '':
                            background = 'Null'
                        else:
                            background = background.replace('\t', ' ').strip()

                        if publisher_id == '':
                            publisher_id = 'Null'
                        else:
                            publisher_id = publisher_id.replace('\t', ' ').strip()

                        if wifi_ssid == '':
                            wifi_ssid = 'Null'
                        else:
                            wifi_ssid = wifi_ssid.replace('\t', ' ')
                            wifi_ssid = wifi_ssid.strip()

                        if wifi_bssid == '':
                            wifi_bssid = 'Null'
                        else:
                            wifi_bssid = wifi_bssid.replace('\t', ' ').strip()

                        temp_dat = [
                            c,
                            advertiser_id,
                            location_at,
                            timestamp,
                            latitude,
                            longitude,
                            altitude,
                            horizontal_accuracy,
                            vertical_accuracy,
                            heading,
                            speed,
                            ipv_4,
                            ipv_6,
                            final_country,
                            user_agent,
                            background,
                            publisher_id,
                            wifi_ssid,
                            wifi_bssid,
                            venue_id,
                            dwell_time
                        ]

                        out = str(temp_dat[0])
                        for i in range(1,len(temp_dat)-1):
                            out = out + "\t" + str(temp_dat[i])
                        out = out + '\t' + str(temp_dat[-1]) + "\n"
                        output.write(out)
                        #print(repr(out))
                        #writer.writerow(test)


                        test = 10
                    except UnknownTimeZoneError:
                        print("Time zone error")


                else:
                    utils.ERROR("Latitude/longitude empty")
                # get time with timezone aware

            # print(line)
            # commit in batches of 1000
            c = c + 1
            if c % 5000 == 0:
                print(str(c))
                #break

    # final commit
    output.seek(0)
    contents = output.getvalue()
    #print(repr(contents))
    test = cur.copy_from(output, 'pings', sep='\t', null='Null')
    # Commit inserts to DB
    raw_conn.commit()

    t2 = time.perf_counter()
    print(f"Time {t2-t1:0.4f} seconds")




def init_ping_table_temp(fileList):

    venue_tab = get_venue_table()
    tf = TimezoneFinder()

    t1 = time.perf_counter()
    #assumption: first row of data must be header
    inFile = fileList[0]
    inds = {}
    c = 0
    with gzip.open(inFile, 'rt') as f:
        # csv_reader = csv.reader(f,escapechar='\\')
        csv_reader = csv.reader((x.replace('\0', '') for x in f), escapechar='\\')
        for line in csv_reader:
            if c == 0:
                inds = get_header_index(line)
            else:
                advertiser_id = line[inds['advertiser_id']]
                location_at = line[inds['location_at']]
                latitude = line[inds['latitude']]
                longitude = line[inds['longitude']]
                altitude = line[inds['altitude']]
                horizontal_accuracy = line[inds['horizontal_accuracy']]
                vertical_accuracy = line[inds['vertical_accuracy']]
                heading = line[inds['heading']]
                speed = line[inds['speed']]
                ipv_4 = line[inds['ipv_4']]
                ipv_6 = line[inds['ipv_6']]
                final_country = line[inds['final_country']]
                user_agent = line[inds['user_agent']]
                background = line[inds['background']]
                publisher_id = line[inds['publisher_id']]
                wifi_ssid = line[inds['wifi_ssid']]
                wifi_bssid = line[inds['wifi_bssid']]
                venue_name = line[inds['venue_name']]
                dwell_time = line[inds['dwell_time']]

                #only take records with lat,lng, and time data
                if latitude != '' or longitude != '' or location_at != '':
                    latitude = float(latitude)
                    longitude = float(longitude)
                    location_at = int(location_at)

                    try:
                        #time zone
                        #tz_name = tf.timezone_at(lng=longitude,lat=latitude)
                        #tz = timezone(tz_name)
                        timestamp = datetime.utcfromtimestamp(location_at)
                        #timestamp = timestamp.replace(tzinfo=tz)


                        #venue
                        #if there is a venue get its id from venue_tab
                        venue_id = None
                        if venue_name in venue_tab:
                            venue_id = venue_tab[venue_name]


                        #convert strings to floats
                        if altitude != '':
                            altitude = float(altitude)
                        else:
                            altitude = None

                        if horizontal_accuracy != '':
                            horizontal_accuracy = float(horizontal_accuracy)
                        else:
                            horizontal_accuracy = None

                        if vertical_accuracy != '':
                            vertical_accuracy = float(vertical_accuracy)
                        else:
                            vertical_accuracy = None

                        if speed != '':
                            speed = float(speed)
                        else:
                            speed = None

                        if dwell_time != '':
                            dwell_time = float(dwell_time)
                        else:
                            dwell_time = None



                        #convert empty strings to none
                        if heading == '':
                            heading = None
                        if ipv_4 == '':
                            ipv_4 = None
                        if ipv_6 == '':
                            ipv_6 = None
                        if final_country == '':
                            final_country = None
                        if user_agent == '':
                            user_agent = None
                        if background == '':
                            background = None
                        if publisher_id == '':
                            publisher_id = None
                        if wifi_ssid == '':
                            wifi_ssid = None
                        if wifi_bssid == '':
                            wifi_bssid = None

                        #ceate ping object
                        ping = Pings(
                            device_id = advertiser_id,
                            location_at = location_at,
                            timestamp = timestamp,
                            latitude = latitude,
                            longitude = longitude,
                            altitude = altitude,
                            horizontal_accuracy = horizontal_accuracy,
                            vertical_accuracy = vertical_accuracy,
                            heading = heading,
                            speed = speed,
                            ipv_4 = ipv_4,
                            ipv_6 = ipv_6,
                            final_country = final_country,
                            user_agent = user_agent,
                            background = background,
                            publisher_id = publisher_id,
                            wifi_ssid = wifi_ssid,
                            wifi_bssid = wifi_bssid,
                            venue_id = venue_id,
                            dwell_time = dwell_time

                        )
                        session.add(ping)

                    except UnknownTimeZoneError:
                        print("Time zone error")


                else:
                    utils.ERROR("Latitude/longitude empty")
                #get time with timezone aware

            #print(line)
            #commit in batches of 1000
            c = c + 1
            if c % 5000 == 0:
                session.commit()
                print(str(c))
                #break

    #final commit
    session.commit()
    t2 = time.perf_counter()
    print(f"Time {t2-t1:0.4f} seconds")


def main():
    # initialize system level logging
    parser = argparse.ArgumentParser()
    parser.add_argument('--loglevel', type=str)
    parser.add_argument('--num_files', type=int)
    args = parser.parse_args(['--loglevel', 'info','--num_files','2'])
    utils.initLogger(args)

    wd = "/Users/jadrake/Documents/Misc/COVID/Mobility/xmode/"
    xmode_dir = wd + "filtered/"


    #file manager
    fileManager = utils.FileManager(wd + 'db_code/')
    num_files = args.num_files
    #files = glob.glob(xmode_dir + '/**/*.gz', recursive=True)

    #create table metadata
    Base.metadata.create_all(engine)

    #initialize tables
    #init_venue_tables()
    #init_device_tables()


    init_ping_table3(fileManager,num_files)




if __name__ == "__main__":
    main()