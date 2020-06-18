import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

# connect to postgres db and create session
engine = db.create_engine('postgresql+psycopg2://jadrake:tacc4ever@localhost/xmode', executemany_mode='batch',echo=False)

Session = sessionmaker(bind=engine)
session = Session()

aux_tables_dir = "/Users/jadrake/Documents/Misc/COVID/Mobility/xmode/db_code/tables/"

ping_dtypes = {
    'advertiser_id':'str',
    'location_at':'int',
    'latitude':'float',
    'longitude':'float',
    'altitude':'float',
    'horizontal_accuracy':'float',
    'vertical_accuracy':'float',
    'heading':'float',
    'speed':'float',
    'ipv_4':'str',
    'ipv_6':'str',
    'final_country':'str',
    'user_agent':'str',
    'background':'str',
    'publisher_id':'str',
    'wifi_ssid':'str',
    'wifi_bssid':'str',
    'venue_name':'str',
    'dwell_time':'str'
}