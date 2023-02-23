import csv
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import datetime
from sqlalchemy.exc import IntegrityError

engine = create_engine("mysql://fafutuka:%40Shinkafa123@134.122.126.5:3306/poll")
db = scoped_session(sessionmaker(bind=engine))

def main():
    count = 0
    f = open("/home/codegeek/Downloads/DGcompressed/DG/anambra/idemili-north.csv")
    reader = csv.reader(f)
    next(reader)
    tracker = open("tracker.txt", "r")
    totalUpload = int(tracker.readline())
    tracker.close()
    for i in range(totalUpload):
        next(reader)
    for state, lga, ward, pu, last_name, delim, vin, first_name, other_name, dob_day, dob_month, dob_year, gender, occupation, phone in reader:
        try:
            db.execute("INSERT INTO application_voter (state, lga, ward, pu, last_name, delim, vin, first_name, other_name, dob_day, dob_month, dob_year, gender, occupation, phone, active, create_date, deleted, last_modified) VALUES (:state, :lga, :ward, :pu, :last_name, :delim, :vin, :first_name, :other_name, :dob_day, :dob_month, :dob_year, :gender, :occupation, :phone, :active, :create_date, :deleted, :last_modified)",
                    {"state": state, "lga": lga, "ward": ward, "pu": pu, "last_name": vin, "delim": last_name, "vin": delim, "first_name": first_name, "other_name": other_name, "dob_day": dob_day, "dob_month": dob_month, "dob_year": dob_year, "gender": gender, "occupation": occupation, "phone": phone, "active": 1, "create_date": datetime.date.today(), "deleted": 0, "last_modified": datetime.date.today()})
        except IntegrityError as e:
            print('there is a duplicate, skipping this entry')
            continue
        count += 1
        totalUpload += 1      
        print(f"Added {vin} to database")
        if count == 1000:
            db.commit()
            tracker = open("tracker.txt", "w")
            tracker.write(str(totalUpload))
            tracker.close()
            count = 0
    db.commit()
    tracker = open("tracker.txt", "w")
    tracker.write(str(totalUpload))
    tracker.close()
if __name__ == "__main__":
    main()
