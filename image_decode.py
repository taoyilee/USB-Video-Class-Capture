import base64

import cv2
from PIL import Image
from mysql.connector import connection

cnx = connection.MySQLConnection(user="beaglebone", password="beaglebone", host="localhost")
cnx.database = "bb_capture"
cursor = cnx.cursor()

cursor.execute(
    "SELECT `image_width`, `image_height`, `image_base64` FROM bb_capture.capture_2018_03_23_08_30_59 WHERE `key` = 1")
for (width, height, data) in cursor:
    print(f"{width}, {height} {data[-10:-1]}")
    img_b = base64.b64decode(f"{data}")
    print(type(img_b))
    print(len(img_b))
    f = open('output_tmp.png', 'wb')
    f.write(img_b)
    f.close()
    img = Image.open('output_tmp.png')

    img.show()
    # img = Image.frombytes("RGB", (width, height), img_b, decoder_name="PNG")
