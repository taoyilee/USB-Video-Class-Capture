#!/usr/bin/env python
import argparse
import base64
import configparser
import datetime
import os
import time
from io import BytesIO
from time import sleep

import cv2
import mysql
from PIL import Image
from mysql.connector import connection

from app.utilities import current_timestamp


def main(write_image=None, mysql_config=(None, None, None, None), verbosity=0):
    if verbosity > 0:
        print(f"Writing Image is {write_image}")
        print(f"MySQL Config is {mysql_config}")

    cap = cv2.VideoCapture(1)

    output_dir = "captures"
    os.makedirs(output_dir, exist_ok=True)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1920)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 1200)

    if mysql_config is not None:
        (db_ip, user, password, database) = mysql_config
        database = "bb_" + database
        print(f"** {current_timestamp()[0]} Attemping to open a MySQL with IP {db_ip} and username: {user}")
        cnx = connection.MySQLConnection(user=user, password=password, host=db_ip)
        print(f"** {current_timestamp()[0]} Writing to MySQL with connection {cnx}")
        print(f"** {current_timestamp()[0]} Attemping to open a database called {database}")
        try:
            cnx.database = database
        except mysql.connector.errors.ProgrammingError:
            print(f"** {current_timestamp()[0]} DB {database} does not exist, creating a new one")
            cnx.cmd_query(f"CREATE DATABASE {database};")
        table_name = f"capture_{datetime.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d_%H_%M_%S')}"
        print(f"** {current_timestamp()[0]} Create a new table {table_name}")
        sql = f"CREATE TABLE `{database}`.`{table_name}` ( `key` INT NOT NULL AUTO_INCREMENT,"
        sql += "`timestamp` FLOAT NOT NULL,"
        sql += "`wall_time` VARCHAR(30) NULL,"
        sql += "`image_width` INT NOT NULL,"
        sql += "`image_height` INT NOT NULL,"
        sql += "`image_base64` LONGTEXT NULL, "
        sql += "PRIMARY KEY (`key`), "
        sql += "UNIQUE INDEX `key_UNIQUE` (`key` ASC));"
        cnx.cmd_query(sql)
        cnx.commit()

    while True:
        try:
            st, ts = current_timestamp()
            width = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
            height = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
            ret, frame = cap.read()
            if write_image:
                output_file = os.path.join(output_dir, f"frame_{current_timestamp()[0]}.png")
                print(f"** {current_timestamp()[0]} Writing CSV to {output_file}") if verbosity > 0 else None
                cv2.imwrite(output_file, frame) if write_image else None
            if write_db:
                buffered = BytesIO()
                img = Image.fromarray(frame)
                img.save(buffered, format="PNG")
                img_str = f"{str(base64.b64encode(buffered.getvalue()), 'utf-8')}"
                sql = f"INSERT INTO `{database}`.`{table_name}`"
                sql += " (`timestamp`, `wall_time`, `image_width`, `image_height`, `image_base64`)"
                sql += f" VALUES ('{ts}', '{st}', '{width}', '{height}', '{img_str}');"
                cnx.cmd_query(sql)
                cnx.commit()
            sleep(1 / 60)
        except KeyboardInterrupt:
            print(f"** {st} KeyboardInterrupt received, closing file.")
            cap.release()
            cnx.close() if write_db else None
            break


if __name__ == "__main__":
    config_file = "config.ini"
    if not os.path.isfile(config_file):
        raise FileNotFoundError(f"** {current_timestamp()[0]} \"{config.ini}\" must exist under {os.getcwd()}")

    cnf = configparser.ConfigParser()
    cnf.read(config_file)

    mysql_user = cnf["DEFAULT"].get("mysql_user")
    mysql_password = cnf["DEFAULT"].get("mysql_password")
    mysql_server = cnf["DEFAULT"].get("mysql_server")
    mysql_db = cnf["DEFAULT"].get("mysql_database")

    instance_name = cnf["DEFAULT"].get("instance_name")

    parser = argparse.ArgumentParser(description='Beagle Bone Blue Data Acquisition Wrapper')
    parser.add_argument('-c', help='Write Filesystem', action='store_true')
    parser.add_argument('-d', help='Write Database', action='store_true')
    parser.add_argument('--csv', help='Specific CSV File Name Write')
    parser.add_argument('--db-server-ip', help='Mysql Database Server IP')
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('-q', action='store_true', help='Quiet Mode')
    args = vars(parser.parse_args())
    write_csv = args['c']
    write_db = args['d']
    quiet = args['q']
    verbosity = args['verbose'] if not quiet else 0

    if not write_csv and not write_db:
        verbosity = 1 if verbosity is None else verbosity
    db_ip = None

    mysql_config = None
    if write_db:
        db_ip = args["db_server_ip"] if args["db_server_ip"] is not None else mysql_server
        mysql_config = (db_ip, mysql_user, mysql_password, mysql_db)
    main(write_image=args['c'], mysql_config=mysql_config, verbosity=verbosity)
