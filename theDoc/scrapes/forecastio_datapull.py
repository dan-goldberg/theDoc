#!/anaconda/envs/theDoc/bin/python

import forecastio
import numpy as np
import re
import sys
import os
import datetime
import time
import mysql.connector
import mysql
import math
from theDoc.utils import emailSend
from theDoc.database import mlb_analtablesupdate as mlbtab
from theDoc import settings


def forecastio_datapull(target_gids):



    cnx = mlbtab.mlb_connect()
    curA = cnx.cursor()

    query1 = ("""
            SELECT 
            g.gid,
            g.game_date,
            g.local_game_time,
            m.latitude,
            m.longitude,
            g.venue_w_chan_loc

            FROM pfx_game as g
            JOIN pfx_venue_w_loc_mapping as m
            ON g.venue_w_chan_loc = m.w_chan_loc

            WHERE g.gid IN ("""+target_gids+""")

    """)

    curA.execute(query1)

    games = np.array(curA.fetchall())

    curA.close()
    cnx.close()
    
    def scrapeWrite(file, fields, dic, line_end=True):
        n = 0
        for field in fields:
            if n == 0:
                file.write('"'+str(dic[field])+'"')
                n += 1
            else:
                file.write(',"'+str(dic[field])+'"')
        if line_end == True:
            file.write('\n')
        else:
            file.write(',')

    def cleanCSV(filename):

        f = open(filename,"r",encoding='utf-8')
        lines = f.readlines()
        f.close()

        f = open(filename,"w",encoding='utf-8')
        for line in lines:
            f.write(line.replace('""',''))
        f.close()

    def csvstore_delete(filename):
        try:
            os.remove(filename)
        except:
            print('    ! WARNING: not deleted - ',filename)


    def key_check(key,datadic,targetdic):
        if key in set(datadic.keys()):
            targetdic[key] = datadic[key]
        else:
            targetdic[key] = ''

    def sqltable_nulling(attrlist):

        colvar_string = ''
        nullif_string = ''
        for col in attrlist:
            colvar_string += "@v_"+col+", "
            nullif_string += col+" = NULLIF(@v_"+col+",''), "

        colvar_string=colvar_string[:-2]
        nullif_string=nullif_string[:-2]

        return colvar_string, nullif_string

    # In[60]:

    script_starttime = datetime.datetime.now()
    print('started -',script_starttime)

    data_storage_days = 0




    api_key = "f9ad5c1fb7320efc31a1db0ade0c27b8"

    game_attrlist = [
        "gid",
        "game_date",
        "local_game_time",
        "venue_w_chan_loc",
        "latitude",
        "longitude"
        ]

    weather_attrlist = [
        "temperature",
        "apparentTemperature",
        "humidity",
        "pressure",
        "dewPoint",
        "cloudCover",
        "uvIndex",
        "precipType",
        "windBearing",
        "windSpeed",

        "nearestStormBearing",
        "nearestStormDistance",
        "ozone",
        "precipIntensity",
        "precipProbability",
        "windGust"

    ]

    derived_attrlist = [
        "windVector_ns",
        "windVector_ew",
        "precipType_rain",
        "precipType_snow",
        "precipType_sleet"
    ]

    pull_date = datetime.datetime.now().date()

    game_columns_string = (', '.join(game_attrlist+weather_attrlist+derived_attrlist)+'\n')
    game_outfile=open("{}/forecastio_data_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pull_date)), "w+", encoding='utf-8')
    if os.stat(game_outfile.name).st_size==0: game_outfile.write(game_columns_string)                    

    resultmsg = ''

    for game in games:

        game_dic = {}
        game_dic["gid"] = game[0]
        game_dic["game_date"] = str(game[1])
        game_dic["local_game_time"] = game[2]
        game_dic["local_game_time"] = (datetime.datetime.min + game_dic["local_game_time"]).strftime('%H:%M')
        game_dic["latitude"] = game[3]
        game_dic["longitude"] = game[4]
        game_dic["venue_w_chan_loc"] = game[5]

        Date = game[1]
        Time = game[2]
        Time = (datetime.datetime.min+Time).time()
        DateTime = str(Date)+'T'+str(Time)

        Lat = game[3]
        Lng = game[4]

        url = "https://api.darksky.net/forecast/"+api_key+"/"+str(Lat)+","+str(Lng)+","+DateTime

        forecast = forecastio.manual(url)

        currently = forecast.json["currently"]

        for attr in weather_attrlist:
            key_check(attr,currently,game_dic)


        if game_dic["windSpeed"] != '' and game_dic["windBearing"] != '':
            game_dic["windVector_ns"] = math.cos(game_dic["windBearing"]*math.pi/180) * game_dic["windSpeed"]
            game_dic["windVector_ew"] = math.sin(game_dic["windBearing"]*math.pi/180) * game_dic["windSpeed"]
        else:
            game_dic["windVector_ns"] = ''
            game_dic["windVector_ew"] = ''

        if game_dic["precipType"] != '' or game_dic["precipIntensity"] == 0:
            game_dic["precipType_rain"] = 0
            game_dic["precipType_snow"] = 0
            game_dic["precipType_sleet"] = 0
        else:
            game_dic["precipType_rain"] = ''
            game_dic["precipType_snow"] = ''
            game_dic["precipType_sleet"] = ''
        if game_dic["precipType"] == 'snow':
            game_dic["precipType_snow"] = 1
        elif game_dic["precipType"] == 'sleet':
            game_dic["precipType_sleet"] = 1
        elif game_dic["precipType"] == 'rain' or game_dic["precipIntensity"] > 0:
            game_dic["precipType_rain"] = 1

        scrapeWrite(game_outfile, game_attrlist+weather_attrlist+derived_attrlist, game_dic, line_end=True)

    game_outfile.close()

    cleanCSV(game_outfile.name)

    affectedrows = 0

    colvar_string, nullif_string = sqltable_nulling(game_attrlist+weather_attrlist+derived_attrlist)

    try:
        cnx = mysql.connector.connect(user='dan', password='',
                                    host='127.0.0.1',
                                    database='mlb')
        cnx.get_warnings = True
        #print('status - Successfully connected to mySQL DB')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("error - Something is wrong with your user name or password")
            sys.exit()
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("error - Database does not exist")
            sys.exit()
        else:
            print(err)
            sys.exit()

    curA = cnx.cursor()

    insert_query1 = ("""
                    LOAD DATA LOCAL INFILE 
                    '"""+game_outfile.name+"""' 
                    INTO TABLE forecastio_weather
                    COLUMNS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES
                    ("""+colvar_string+""")
                    SET
                    """+nullif_string+"""
                    """)


    curA.fetchwarnings()

    curA.execute(insert_query1)
    cnx.commit()
    affectedrows1 = curA.rowcount
    print("    forecastio_weather affected rows = {}".format(affectedrows1))
    resultmsg1 = str("\n    forecastio_weather affected rows = {}".format(affectedrows1))+"\n"
    



    curA.close()
    cnx.close()

    pastday = datetime.datetime.now().date() - datetime.timedelta(days=data_storage_days)
    past_gamefile = "{}/forecastio_data_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))

    csvstore_delete(past_gamefile)

    #print('status - csv deleted')

    script_endtime = datetime.datetime.now()
    script_totaltime = script_endtime - script_starttime

    endmsg = '    success - '+str(datetime.datetime.now())+' script total runtime = '+str(script_totaltime)

    #print(endmsg)

    emailmsg = '    started ' + str(script_starttime)+'\n\n+'+resultmsg+'\n\n'+endmsg
    print(emailmsg)
    #emailSend.emailSend(msg=emailmsg ,subject=sys.argv[0]+' report')

    print('\n')
    
    return affectedrows1


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



