#!/anaconda/bin/python

'''
This scrpit scrapes MLB game results from previous day and inserts 
into pfx_action, pfx_atbat,pfx_pitch, pfx_runner, pfx_pickoff, 
pfx_miniscore, and pfx_basesav tables. The intention is to scrape all necessary raw 
game outcome data. Data csv's are kept for (data_storage_days) days then deleted.
'''

import re
import sys
from bs4 import BeautifulSoup, UnicodeDammit
from urllib.request import urlopen
import os
import datetime
import time
import mysql.connector
import numpy as np
import pandas as pd 
from io import StringIO 
from theDoc.utils import emailSend
from theDoc.scrapes.forecastio_datapull import forecastio_datapull
from theDoc.scrapes.betting_scrape_func import betting_scrape
from theDoc.inference.theDoc_inference_func import run_inference
from theDoc import settings

'''old_stdout = sys.stdout
output_text = StringIO()
sys.stdout = output_text'''

def collectXMLAttrs(parent, target):
    attrs_list = []
    pieces = parent.find_all(target)
    for piece in pieces:
        keys = piece.attrs.keys()
        for key in keys:
            attrs_list.append(key)
    return set(attrs_list)

def createSoup(url, max_errors):
    url_attempts = 0
    while url_attempts <= max_errors:
        try:        
            soup = BeautifulSoup(urlopen(url),"lxml")
            return soup
            break
        except:
            
            if url_attempts == max_errors:
                #print('error -',sys.exc_info()[0],url)
                #print('not found:',url)
                break
            else: 
                #print('error -',sys.exc_info()[0],url)
                url_attempts += 1
                time.sleep(30)
                continue

def scrapeXMLAttrs(parent, attributes):
    dic = {}
    for attribute in attributes:
        if attribute in parent.attrs: dic[attribute] = parent[attribute]
        else: dic[attribute] = ""
    return dic
    
def scrapeXMLContent(parent, xml_tags):
    dic = {}
    for xml_tag in xml_tags:
        if parent.find(xml_tag) is not None and parent.find(xml_tag).string is not None: dic[xml_tag] = parent.find(xml_tag).string
        else: dic[xml_tag] = ""
    return dic

def scrapeWrite(file, fields, dic, line_end=True):
    n = 0
    for field in fields:
        if n == 0:
            file.write('"'+dic[field]+'"')
            n += 1
        else:
            file.write(',"'+dic[field]+'"')
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


def insert_csv(filename,tablename,cursor,conn):
    insert_query = (
                "LOAD DATA LOCAL INFILE "
                "'"+filename+"' "
                "INTO TABLE "+tablename+" "
                "COLUMNS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
                )
    cursor.execute(insert_query)
    conn.commit()
    print(tablename+" affected rows = {}".format(cursor.rowcount))
    resultmsg = '\n'+str(tablename+" affected rows = {}".format(cursor.rowcount))+'\n'
    affectedrows = cursor.rowcount
    return resultmsg, affectedrows


def csvstore_delete(filename):
    try:
        os.remove(filename)
    except:
        pass
        
def bsCreateFile(url,max_errors):        
    url_attempts = 0
    while url_attempts <= max_errors:
        try:        
            return pd.read_csv(urlopen(url))
            break
        except:
            if url_attempts == max_errors:
                print('error -',sys.exc_info()[0],url)
                break
            else: 
                print('error -',sys.exc_info()[0],url)
                url_attempts += 1
                time.sleep(30)
                continue
                
def bs_cleanCSV(filename):

    f = open(filename,"r")
    lines = f.readlines()
    f.close()

    f = open(filename,"w")
    for line in lines:
        f.write(line.replace('"null"','NULL').replace('"0.0"','NULL').replace('"0.00"','NULL'))
    f.close()


# In[5]:

os.environ['TZ'] = 'America/New_york'

script_starttime = datetime.datetime.now()
print('    started -',script_starttime)

active_date = datetime.datetime.now().date()

try:
    cnx = mysql.connector.connect(user='dan', password='',
                                host='127.0.0.1',
                                database='mlb')
    #print('Successfully connected to mySQL DB')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
        sys.exit()
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
        sys.exit()
    else:
        print(err)
        sys.exit()
        
curA = cnx.cursor()

query = ("""
        SELECT 
        gid,
        game_date
        FROM
        pfx_game
        WHERE
        game_date = date('"""+str(active_date)+"""')
        AND gid NOT IN
        (
            SELECT p.gid FROM pfx_player AS p 
            WHERE p.game_date = date('"""+str(active_date)+"""')
        )
        """
    )

query_start = datetime.datetime.now()
#print('about to execute query: '+str(query_start))
try:
    curA.execute(query)
except:
    sys.exit()
cur_columns = curA.column_names

data = np.array(curA.fetchall())
#print('results fetched '+str(datetime.datetime.now())+' - total time: '+str(datetime.datetime.now()-query_start))


curA.close()
cnx.close()

#print('Success')

if len(data) == 0:
    sys.exit()


# In[10]:

data_storage_days = 2

game_attrs = [
                "gid"
                ,"game_date"
                ,"side"
                ]

player_attrs = [
                "id"
                ,"first"
                ,"last"
                ,"num"
                ,"boxname"
                ,"rl"
                ,"bats"
                ,"position"
                ,"current_position"
                ,"status"
                ,"team_abbrev"
                ,"team_id"
                ,"parent_team_abbrev"
                ,"parent_team_id"
                ,"bat_order"
                ,"game_position"
                ,"avg"
                ,"hr"
                ,"rbi"
                ,"wins"
                ,"losses"
                ,"era"
    ]


player_columns_string = (', '.join(game_attrs)+","+', '.join(player_attrs)+'\n')

player_outfile=open("{}/pfx_player_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(datetime.datetime.now())), "w+", encoding='utf-8')
if os.stat(player_outfile.name).st_size==0: player_outfile.write(player_columns_string)                    

player_outfile2=open("{}/pfx_player_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(active_date)), "a+", encoding='utf-8')
if os.stat(player_outfile2.name).st_size==0: player_outfile2.write(player_columns_string)                    

resultmsg = ''

active_gids = []

n = 0
for row in data:
    n += 1
    time.sleep(2)
    gid = row[0]
    game_date = str(row[1])

    game_dic = {}
    game_dic["gid"] = gid
    game_dic["game_date"] = game_date

    g_url = ("http://gd2.mlb.com/components/game/mlb/"
              +"year_"+str(active_date.year)
              +"/month_"+active_date.strftime('%m')
              +"/day_"+active_date.strftime('%d')
              +"/"+gid
             )
    
    p_url = g_url + "players.xml"
    
    players_soup = createSoup(p_url,2)
    
    if players_soup is None:
        continue
        
    print(gid)
    active_gids.append(gid)
    resultmsg += gid+'\n'
    
    teams = players_soup.find_all("team")
    for team in teams:
        
        game_dic["side"] = team["type"]
        
        players = team.find_all("player")
        for player in players:
            player_dic = scrapeXMLAttrs(player, player_attrs)
            
            scrapeWrite(player_outfile, game_attrs, game_dic, line_end=False)
            scrapeWrite(player_outfile, player_attrs, player_dic, line_end=True)
                
            scrapeWrite(player_outfile2, game_attrs, game_dic, line_end=False)
            scrapeWrite(player_outfile2, player_attrs, player_dic, line_end=True)
            
time.sleep(4)    
    
player_outfile.close()
cleanCSV(player_outfile.name)

player_outfile2.close()
cleanCSV(player_outfile2.name)

             
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
curA.fetchwarnings()

affectedrows = 0

resultmsg1, affectedrows1 = insert_csv(player_outfile.name,"pfx_player",curA,cnx)
resultmsg += resultmsg1
affectedrows += affectedrows1

curA.close()
cnx.close()
#print('status - mySQL DB connection closed')

if affectedrows > 0:
    active_gids_str = repr(active_gids)[1:-1]
    forecastio_weather_rows = forecastio_datapull(active_gids_str)
    resultmsg += '\n forecastio_weather inserted rows: '+str(forecastio_weather_rows)
    
    betting_scrape_rows = betting_scrape()
    resultmsg += '\n betting_scrape inserted rows: '+str(betting_scrape_rows)
    
    run_inference(active_gids_str)

csvstore_delete(player_outfile.name)

pastday = active_date - datetime.timedelta(days=data_storage_days)
past_playerfile = "{}/pfx_player_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))
csvstore_delete(past_playerfile)

#print('status - csv deleted')

script_endtime = datetime.datetime.now()
script_totaltime = script_endtime - script_starttime

endmsg = '    success -'+str(datetime.datetime.now())+'script total runtime ='+str(script_totaltime)

print(endmsg)

if affectedrows > 0:
    emailmsg = '    started ' + str(script_starttime)+'\n\n'+resultmsg+'\n\n'+endmsg
    emailSend.emailSend(msg=emailmsg ,subject=sys.argv[0]+' report')
    
print('\n')