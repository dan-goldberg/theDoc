#!/anaconda/bin/python

'''
This scrpit scrapes Sportsbookreview.com MLB betting lines for today's upcoming games,
and inserts the records into betting_scrape table.
Data csv's are kept for (data_storage_days) days then deleted.
'''

import re
import sys
from bs4 import BeautifulSoup, UnicodeDammit
import urllib
from urllib.request import urlopen
import os
import datetime
import time
import mysql.connector
import numpy as np
import pandas as pd 
from io import StringIO 
import emailSend
from staticVars import internetcheck_ip
import ssl

os.environ['TZ'] = 'America/New_york'

gcontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1,)  # Only for gangstars
gcontext.verify_mode = ssl.CERT_NONE




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
            soup = BeautifulSoup(urlopen(url, context=gcontext),"lxml")
            return soup
            break
        except urllib.error.URLError as error:
            
            if url_attempts == max_errors:
                print('error -',sys.exc_info()[0],url)
                print('too many errors - moving to next link')
                break
            else: 
                print('error -',sys.exc_info()[0],url)
                print(error.reason)
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
    print("    "+tablename+" affected rows = {}".format(cursor.rowcount))
    resultmsg = str('\n'+tablename+" affected rows = {}".format(cursor.rowcount))+'\n'
    affectedrows = cursor.rowcount
    return resultmsg, affectedrows

def csvstore_delete(filename):
    try:
        os.remove(filename)
    except:
        print('    ! WARNING: not deleted - ',filename)
        
    

def clean_betting(s):
    if re.match(r'^pk',s,re.I) != None:
        s = 0
    else:
        if s == "":
            pass
        elif s[-1] not in ['0','1','2','3','4','5','6','7','8','9']:
            if s[0] == '-':
                if s[1:-1] == '':
                    s = int(0)*(-1.0) - (0.5)
                else:
                    s = int(s[1:-1])*(-1.0) - (0.5)
            elif s[0] == '+':
                if s[1:-1] == '':
                    s = int(0)*(1.0) + (0.5)
                else:
                    s = int(s[1:-1])*(1.0) + (0.5)
            else:
                    s = int(s[0:-1])*(1.0) + (0.5)
        else:
            if s[0] == '-':
                s = int(s[1:])*(-1.0)
            elif s[0] == '+':
                s = int(s[1:])*(1.0)
            else:
                s = int(s[:])*(1.0)    
        return s


# In[36]:

script_starttime = datetime.datetime.now()
print('started -',script_starttime)

active_date = datetime.datetime.now().date()

data_storage_days = 2

outfile=open("/Users/dangoldberg/PFX_Scrapes/mlb_betting_odds___" + str(active_date) + ".csv", "w+", encoding='utf-8')
if os.stat(outfile.name).st_size==0: outfile.write('"away_team","home_team","game_date","game_time_et","sportsbook_id","spread_away_points","spread_away_odds","spread_home_points","spread_home_odds","money_away_odds","money_home_odds","total_points","total_over_odds","total_under_odds","spread_away_points_raw","spread_away_odds_raw","spread_home_points_raw","spread_home_odds_raw","money_away_odds_raw","money_home_odds_raw","total_points_raw","total_over_odds_raw","total_under_odds_raw"\n')
    
resultmsg = ''

base_url = "http://www.sportsbookreview.com/betting-odds/mlb-baseball/"
d_url = base_url+"?date="+str(active_date.year)+active_date.strftime('%m')+active_date.strftime('%d')

day_soup = createSoup(d_url, 2)

gamelinks_raw = day_soup.find_all("a", class_="eventLink")

for gamelink_raw in gamelinks_raw:
    time.sleep(10)

    g_url = gamelink_raw['href']

    game_soup = createSoup(g_url, 2) 

    if game_soup is None:
        continue
    
    teams_and_date = game_soup.find("h1", class_="teams")
    teams = teams_and_date.contents[0].string
    away_team = teams[:teams.find("vs")-1]
    home_team = teams[teams.find("vs")+3:]
    game_day_and_time = teams_and_date.find(class_="date").string
    game_time = game_day_and_time[game_day_and_time.find(str(active_date.year))+7:].strip()
    game_time = datetime.datetime.strptime(game_time[:-4],'%I:%M %p').strftime('%H:%M')
    # active_date already set

    eventGrid = game_soup.find_all("div", class_="event-grid event-category", attrs={"data-periodtypeid": "1"})[0]

    if eventGrid.find_all("div", class_="eventLine") == []:
        continue

    currentbooks = eventGrid["data-currentbooks"].split(",")

    for currentbook_num in currentbooks:

        book_id = int(currentbook_num)

        spread = eventGrid.find_all("div", class_="eventLine")[0].find("div", class_="el-div eventLine-book", attrs={"data-bookid" : currentbook_num.strip()}).find_all("b")

        if spread[0].string == None or spread[1].string == None:
            spread_away_points = ""
            spread_away_odds = ""
            spread_home_points = ""
            spread_home_odds = ""
        else:
            spread_away_points = spread[0].string[:spread[0].string.find("\xa0")]
            spread_away_odds = spread[0].string[spread[0].string.find("\xa0")+1:]
            spread_home_points = spread[1].string[:spread[1].string.find("\xa0")]
            spread_home_odds = spread[1].string[spread[1].string.find("\xa0")+1:]

        money = eventGrid.find_all("div", class_="eventLine")[1].find("div", class_="el-div eventLine-book", attrs={"data-bookid" : currentbook_num.strip()}).find_all("b")

        if money[0].string == None or money[1].string == None:
            money_away_odds = ""
            money_home_odds = ""
        else:
            money_away_odds = money[0].string
            money_home_odds = money[1].string

        total = eventGrid.find_all("div", class_="eventLine")[2].find("div", class_="el-div eventLine-book", attrs={"data-bookid" : currentbook_num.strip()}).find_all("b")

        if total[0].string == None or total[1].string == None:
            total_points = ""
            total_over = ""
            total_under = ""
        else:
            total_points = total[0].string[:total[0].string.find("\xa0")]
            total_over = total[0].string[total[0].string.find("\xa0")+1:]
            total_under = total[1].string[total[1].string.find("\xa0")+1:]

        outfile.write('"'+str(away_team.strip())
                            +'","'+str(home_team.strip())
                            +'","'+str(active_date)
                            +'","'+str(game_time.strip())
                            +'","'+str(book_id)
                            +'","'+str(clean_betting(spread_away_points.strip()))
                            +'","'+str(clean_betting(spread_away_odds.strip()))
                            +'","'+str(clean_betting(spread_home_points.strip()))
                            +'","'+str(clean_betting(spread_home_odds.strip()))
                            +'","'+str(clean_betting(money_away_odds.strip()))
                            +'","'+str(clean_betting(money_home_odds.strip()))
                            +'","'+str(clean_betting(total_points.strip()))
                            +'","'+str(clean_betting(total_over.strip()))
                            +'","'+str(clean_betting(total_under.strip()))
                            +'","'+str(spread_away_points.strip())
                            +'","'+str(spread_away_odds.strip())
                            +'","'+str(spread_home_points.strip())
                            +'","'+str(spread_home_odds.strip())
                            +'","'+str(money_away_odds.strip())
                            +'","'+str(money_home_odds.strip())
                            +'","'+str(total_points.strip())
                            +'","'+str(total_over.strip())
                            +'","'+str(total_under.strip())
                            +'"\n'
                            )

outfile.close()
cleanCSV(outfile.name)



             
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

toggle_most_recent_query=("""UPDATE betting_scrape SET most_recent = 'N' WHERE most_recent = 'Y' AND game_date = DATE(NOW())""")

curA.execute(toggle_most_recent_query)
cnx.commit()

affectedrows = 0

resultmsg1, affectedrows1 = insert_csv(outfile.name,"betting_scrape",curA,cnx)
resultmsg += resultmsg1
affectedrows += affectedrows1

dataclean_query=("""
                UPDATE 
                betting_scrape
                SET
                spread_away_points = CASE WHEN spread_away_points = 0 THEN NULL ELSE spread_away_points END,
                spread_away_odds = CASE WHEN spread_away_odds = 0 THEN NULL ELSE spread_away_odds END,
                spread_home_points = CASE WHEN spread_home_points = 0 THEN NULL ELSE spread_home_points END,
                spread_home_odds = CASE WHEN spread_home_odds = 0 THEN NULL ELSE spread_home_odds END,
                money_away_odds = CASE WHEN money_away_odds = 0 THEN NULL ELSE money_away_odds END,
                money_home_odds = CASE WHEN money_home_odds = 0 THEN NULL ELSE money_home_odds END,
                total_points = CASE WHEN total_points = 0 THEN NULL ELSE total_points END,
                total_over = CASE WHEN total_over = 0 THEN NULL ELSE total_over END,
                total_under = CASE WHEN total_under = 0 THEN NULL ELSE total_under END
                """)

curA.execute(dataclean_query)
cnx.commit()

mapupdate_query1=("""
                DROP TABLE IF EXISTS betting_pfx_map;
                """
                  )

curA.execute(mapupdate_query1)
cnx.commit()

mapupdate_query2=("""                  
                CREATE TABLE betting_pfx_map AS
                SELECT
                betting.betting_scrape_id,
                game.gid
                FROM
                (
                    SELECT b.betting_scrape_id, tba.name_translated as betting_away, tbh.name_translated as betting_home, DATE(b.game_date) as game_date_b, TIME(b.game_time_et) as game_time_et_b
                    FROM betting_scrape as b
                    LEFT JOIN teamname_translate as tba ON tba.source_table = 'betting_scrape' AND tba.name_original = b.team_away
                    LEFT JOIN teamname_translate as tbh ON tbh.source_table = 'betting_scrape' AND tbh.name_original = b.team_home
                ) as betting
                JOIN
                (
                    SELECT DISTINCT g.gid, tga.name_translated as game_away, tgh.name_translated as game_home, DATE(g.game_date) as game_date_g, TIME(g.game_time_et) as game_time_et_g
                    FROM pfx_game as g
                    LEFT JOIN teamname_translate as tga ON tga.source_table = 'pfx_game' AND tga.name_original = g.away_name_full
                    LEFT JOIN teamname_translate as tgh ON tgh.source_table = 'pfx_game' AND tgh.name_original = g.home_name_full
                ) as game
                ON 
                betting.betting_away = game.game_away AND 
                betting.betting_home = game.game_home AND
                DATE(betting.game_date_b) = DATE(game.game_date_g)
                AND TIME(betting.game_time_et_b) BETWEEN TIME(TIME(game.game_time_et_g) - interval '2' hour) AND TIME(TIME(game.game_time_et_g) + interval '2' hour)
                ;
                """
                )

curA.execute(mapupdate_query2)
cnx.commit()

curA.close()
cnx.close()
#print('status - mySQL DB connection closed')

pastday = active_date - datetime.timedelta(days=data_storage_days)
past_playerfile = "/Users/dangoldberg/PFX_Scrapes/mlb_betting_odds___"+str(pastday)+".csv"
csvstore_delete(past_playerfile)

        
script_endtime = datetime.datetime.now()
script_totaltime = script_endtime - script_starttime

endmsg = '    success - '+str(datetime.datetime.now())+' script total runtime = '+str(script_totaltime)

print(endmsg)

emailmsg = '    started ' + str(script_starttime)+'\n\n'+resultmsg+'\n\n'+endmsg
emailSend.emailSend(msg=emailmsg ,subject=sys.argv[0]+' report')
    
    
print('\n')
