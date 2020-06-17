import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

# connect to postgres db and create session
engine = db.create_engine('postgresql://jadrake:tacc4ever@localhost/xmode')

Session = sessionmaker(bind=engine)
session = Session()

aux_tables_dir = "/Users/jadrake/Documents/Misc/COVID/Mobility/xmode/db_code/tables/"
