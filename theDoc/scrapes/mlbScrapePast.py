#!/anaconda/bin/python

'''
This scrpit scrapes MLB game results from previous day and inserts 
into pfx_action, pfx_atbat,pfx_pitch, pfx_runner, pfx_pickoff, 
pfx_miniscore, and pfx_basesav tables. The intention is to scrape all necessary 
raw game outcome data. Data csv's are kept for (data_storage_days) days then deleted.
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
from theDoc import settings



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
                print('error -',sys.exc_info()[0],url)
                print('too many errors - moving to next link')
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
    print("    "+tablename+" affected rows = {}".format(cursor.rowcount))
    resultmsg = str(tablename+" affected rows = {}".format(cursor.rowcount))+'\n'
    affectedrows = cursor.rowcount
    return resultmsg, affectedrows

def csvstore_delete(filename):
    try:
        os.remove(filename)
    except:
        print('    ! WARNING: not deleted - ',filename)
        
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


# In[23]:

script_starttime = datetime.datetime.now()
print('started -',script_starttime)

active_date = datetime.datetime.now().date() - datetime.timedelta(days=1)
savant_date = datetime.datetime.now().date() - datetime.timedelta(days=2)

try:
    cnx = mysql.connector.connect(user='dan', password='',
                                host='127.0.0.1',
                                database='mlb')
    print('Successfully connected to mySQL DB')
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
        game_pk,
        game_date
        FROM
        pfx_game
        WHERE
        game_date = date('"""+str(active_date)+"""')
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


# In[24]:


data_storage_days = 2




game_attrs = [
                "gid"
                ,"game_pk"
                ,"game_date"
                ]

inning_attrs = [
                "inn_num"
                ,"inn_half" 
                ,"home_bat_fl"
]

action_attrs = [ 
                "b"
                ,"s"
                ,"o"
                ,"des"
                ,"des_es"
                ,"event"
                ,"event_es"
                ,"tfs"
                ,"tfs_zulu"
                ,"player"
                ,"pitch"
                ,"event_num"
                ,"home_team_runs"
                ,"away_team_runs"
                ,"play_guid"
    
]

atbat_attrs = [
                "num"
                ,"b"
                ,"s"
                ,"o"
                ,"start_tfs"
                ,"start_tfs_zulu"
                ,"batter"
                ,"stand"
                ,"b_height"
                ,"pitcher"
                ,"p_throws"
                ,"des"
                ,"des_es"
                ,"event_num"
                ,"event"
                ,"event_es"
                ,"event2"
                ,"event2_es"
                ,"score"
                ,"away_team_runs"
                ,"home_team_runs"
                ,"play_guid"

]

absub_attrs = ["ab_num"]

pitchpre_attrs = ["ab_pitch_num"]

pitch_attrs = [                     
                "des"
                ,"des_es"
                ,"id"
                ,"type"
                ,"tfs"
                ,"tfs_zulu"
                ,"x"
                ,"y"
                ,"event_num"
                ,"on_1b"
                ,"on_2b"
                ,"on_3b"
                ,"sv_id"
                ,"start_speed"
                ,"end_speed"
                ,"sz_top"
                ,"sz_bot"
                ,"pfx_x"
                ,"pfx_z"
                ,"px"
                ,"pz"
                ,"x0"
                ,"z0"
                ,"y0"
                ,"vx0"
                ,"vy0"
                ,"vz0"
                ,"ax"
                ,"ay"
                ,"az"
                ,"break_y"
                ,"break_angle"
                ,"break_length"
                ,"pitch_type"
                ,"type_confidence"
                ,"zone"
                ,"nasty"
                ,"spin_dir"
                ,"spin_rate"
                ,"cc"
                ,"mt"
                ,"play_guid"
    
]

runner_attrs = [
                "id"
                ,"start"
                ,"end"
                ,"event"
                ,"event_num"
                ,"score"
                ,"rbi"
                ,"earned"               
    
]

pickoff_attrs = [
                "des"
                ,"des_es"
                ,"event_num"
                ,"catcher"
                ,"play_guid"
    
                ]

ms_det_attrlist = [
                'gid',
                'game_date'
                ]

ms_game_attrlist = [
                'home_name_abbrev', 
                'away_name_abbrev',
                'home_code',
                'away_code',
                'home_file_code',
                'away_file_code',
                'home_team_id',
                'away_team_id',
                'home_games_back',
                'away_games_back',
                'home_games_back_wildcard',
                'away_games_back_wildcard',
                'venue_w_chan_loc',
                'gameday_sw',
                'double_header_sw',
                'gameday',
                'home_win',
                'home_loss',
                'away_win',
                'away_loss',
                'id'
                ]

ms_gamestatus_attrlist = [
                'status',
                'status_ind', 
                'delay_reason',
                'inning',
                'top_inning',
                'b',
                's',
                'o',
                'inning_state',
                'note',
                'is_perfect_game',
                'is_no_hitter'
                ]

ms_runs_attrlist = [
                'away',
                'home',
                'diff'
                ]

baseballsavant_attrs = [
                "game_pk"
                ,"pitch_id"
                ,"sv_id"
                ,"batter"
                ,"stand"
                ,"pitcher"
                ,"p_throws"
                ,"catcher"
                ,"umpire"
                ,"effective_speed"
                ,"release_spin_rate"
                ,"release_extension"
                ,"hc_x"
                ,"hc_y"
                ,"hit_distance_sc"
                ,"hit_speed"
                ,"hit_angle"
        ]


action_columns_string = (', '.join(game_attrs)+","+', '.join(inning_attrs)+","+', '.join(action_attrs)+'\n')
action_outfile=open("{}/pfx_action_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(active_date)), "w+", encoding='utf-8')
if os.stat(action_outfile.name).st_size==0: action_outfile.write(action_columns_string)                    

atbat_columns_string = (', '.join(game_attrs)+","+', '.join(inning_attrs)+","+', '.join(atbat_attrs)+'\n')
atbat_outfile=open("{}/pfx_atbat_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(active_date)), "w+", encoding='utf-8')
if os.stat(atbat_outfile.name).st_size==0: atbat_outfile.write(atbat_columns_string)    

pitch_columns_string = (', '.join(game_attrs)+","+', '.join(inning_attrs)+","+', '.join(absub_attrs)+","+', '.join(pitchpre_attrs)+","+', '.join(pitch_attrs)+'\n')
pitch_outfile=open("{}/pfx_pitch_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(active_date)), "w+", encoding='utf-8')
if os.stat(pitch_outfile.name).st_size==0: pitch_outfile.write(pitch_columns_string)    

runner_columns_string = (', '.join(game_attrs)+","+', '.join(inning_attrs)+","+', '.join(absub_attrs)+","+', '.join(runner_attrs)+'\n')
runner_outfile=open("{}/pfx_runner_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(active_date)), "w+", encoding='utf-8')
if os.stat(runner_outfile.name).st_size==0: runner_outfile.write(runner_columns_string)    

pickoff_columns_string = (', '.join(game_attrs)+","+', '.join(inning_attrs)+","+', '.join(absub_attrs)+","+', '.join(pickoff_attrs)+'\n')
pickoff_outfile=open("{}/pfx_pickoff_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(active_date)), "w+", encoding='utf-8')
if os.stat(pickoff_outfile.name).st_size==0: pickoff_outfile.write(pickoff_columns_string)

ms_columns_string = (', '.join(ms_det_attrlist)+"," +','+', '.join(ms_game_attrlist)+"," +','+ ', '.join(ms_gamestatus_attrlist)+"," +','+ ', '.join(ms_runs_attrlist) + '\n')
miniscore_outfile=open("{}/pfx_miniscore_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(active_date)), "w+", encoding='utf-8')
if os.stat(miniscore_outfile.name).st_size==0: miniscore_outfile.write(ms_columns_string)                    

resultmsg = ''

n = 0
for row in data:
    n += 1
    time.sleep(2)
    gid = row[0]
    game_date = str(row[2])
    game_pk = str(row[1])
    
    #print(n,gid)
    
    game_dic = {}
    game_dic["gid"] = gid
    game_dic["game_pk"] = game_pk
    game_dic["game_date"] = game_date

    g_url = ("http://gd2.mlb.com/components/game/mlb/"
              +"year_"+str(active_date.year)
              +"/month_"+active_date.strftime('%m')
              +"/day_"+active_date.strftime('%d')
              +"/"+gid
              )

    i_url = (g_url+"inning/inning_all.xml")

    inning_soup = createSoup(i_url, 3)
    
    if inning_soup is None:
        continue
        
    innings = inning_soup.find_all("inning")
    for inning in innings:
        time.sleep(1)
        inn_dic = {}
        inn_dic["inn_num"] = inning["num"]
        
        halves = inning.find_all({"top","bottom"})
        for half in halves:
            inn_dic["inn_half"] = half.name
            if inn_dic["inn_half"] == "top":
                inn_dic["home_bat_fl"] = "0"
            elif inn_dic["inn_half"] == "bottom":
                inn_dic["home_bat_fl"] = "1"
    
            actions = half.find_all("action")
            if actions == []:
                pass
            else:
                for action in actions:
                    action_dic = scrapeXMLAttrs(action, action_attrs)
                    scrapeWrite(action_outfile, game_attrs, game_dic, line_end=False)
                    scrapeWrite(action_outfile, inning_attrs, inn_dic, line_end=False)
                    scrapeWrite(action_outfile, action_attrs, action_dic, line_end=True)
                    
            atbats = half.find_all("atbat")
            for atbat in atbats:
                atbat_dic = scrapeXMLAttrs(atbat, atbat_attrs)
                scrapeWrite(atbat_outfile, game_attrs, game_dic, line_end=False)
                scrapeWrite(atbat_outfile, inning_attrs, inn_dic, line_end=False)
                scrapeWrite(atbat_outfile, atbat_attrs, atbat_dic, line_end=True)

                absub_dic = {}
                absub_dic["ab_num"] = atbat_dic["num"]

                pitchnum = 0

                pitches = atbat.find_all("pitch")
                for pitch in pitches:
                    pitch_dic = scrapeXMLAttrs(pitch, pitch_attrs)
                    pitchnum += 1 
                    pitchpre_dic = {}
                    pitchpre_dic["ab_pitch_num"] = str(pitchnum)
                    scrapeWrite(pitch_outfile, game_attrs, game_dic, line_end=False)
                    scrapeWrite(pitch_outfile, inning_attrs, inn_dic, line_end=False)
                    scrapeWrite(pitch_outfile, absub_attrs, absub_dic, line_end=False)
                    scrapeWrite(pitch_outfile, pitchpre_attrs, pitchpre_dic, line_end=False) 
                    scrapeWrite(pitch_outfile, pitch_attrs, pitch_dic, line_end=True)

                runners = atbat.find_all("runner")
                if runners == []:
                    pass
                else:  
                    for runner in runners:
                        runner_dic = scrapeXMLAttrs(runner, runner_attrs) 
                        scrapeWrite(runner_outfile, game_attrs, game_dic, line_end=False)
                        scrapeWrite(runner_outfile, inning_attrs, inn_dic, line_end=False)
                        scrapeWrite(runner_outfile, absub_attrs, absub_dic, line_end=False)
                        scrapeWrite(runner_outfile, runner_attrs, runner_dic, line_end=True)

                pickoffs = atbat.find_all("po")
                if pickoffs == []:
                    pass
                else:  
                    for pickoff in pickoffs:
                        pickoff_dic = scrapeXMLAttrs(pickoff, pickoff_attrs) 
                        scrapeWrite(pickoff_outfile, game_attrs, game_dic, line_end=False)
                        scrapeWrite(pickoff_outfile, inning_attrs, inn_dic, line_end=False)
                        scrapeWrite(pickoff_outfile, absub_attrs, absub_dic, line_end=False)
                        scrapeWrite(pickoff_outfile, pickoff_attrs, pickoff_dic, line_end=True)

                            
    ms_url = g_url+"miniscoreboard.xml"
    
    miniscore_soup = createSoup(ms_url,2)
    
    if miniscore_soup is None:
        continue
        
    ms_det_dic = {}
    ms_det_dic["gid"] = gid
    ms_det_dic["game_date"] = game_date
    scrapeWrite(miniscore_outfile, ms_det_attrlist, ms_det_dic, line_end=False)

    game = miniscore_soup.find("game")
    ms_game_dic = scrapeXMLAttrs(game, ms_game_attrlist)
    scrapeWrite(miniscore_outfile, ms_game_attrlist, ms_game_dic, line_end=False)

    game_status = game.find("game_status")
    ms_gamestatus_dic = scrapeXMLAttrs(game_status, ms_gamestatus_attrlist)
    scrapeWrite(miniscore_outfile, ms_gamestatus_attrlist, ms_gamestatus_dic, line_end=False)

    runs = game.find("r")
    ms_runs_dic = scrapeXMLAttrs(runs, ms_runs_attrlist)
    scrapeWrite(miniscore_outfile, ms_runs_attrlist, ms_runs_dic, line_end=True)

print('games scraped -',n)
    
time.sleep(4)    
    
action_outfile.close()
atbat_outfile.close()
pitch_outfile.close()
runner_outfile.close()
pickoff_outfile.close()
miniscore_outfile.close()    
    
cleanCSV(action_outfile.name)
cleanCSV(atbat_outfile.name)
cleanCSV(pitch_outfile.name)
cleanCSV(runner_outfile.name)
cleanCSV(pickoff_outfile.name)
cleanCSV(miniscore_outfile.name)

#&hfGT=R%7C

bs_url="https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfZ=&hfGT=R%7C&hfPR=&hfAB=&stadium=&hfBBT=&hfBBL=&hfC=&season=all&player_type=all&hfOuts=&pitcher_throws=&batter_stands=&start_speed_gt=&start_speed_lt=&perceived_speed_gt=&perceived_speed_lt=&spin_rate_gt=&spin_rate_lt=&exit_velocity_gt=&exit_velocity_lt=&launch_angle_gt=&launch_angle_lt=&distance_gt=&distance_lt=&batted_ball_angle_gt=&batted_ball_angle_lt="+"&game_date_gt="+savant_date.strftime("%Y%m%d")+"&game_date_lt="+savant_date.strftime("%Y%m%d")+"&team=&position=&hfRO=&home_road=&hfInn=&min_pitches=0&min_results=0&group_by=&sort_col=pitches&sort_order=desc&min_abs=0&xba_gt=&xba_lt=&px1=&px2=&pz1=&pz2=&type=all&"

bs_file = bsCreateFile(bs_url,3)

try:
    bs_df = bs_file[baseballsavant_attrs]
    bs_file_name = "{}/pfx_baseballsavant___{}.csv".format(settings.PFX_SCRAPE_PATH, str(active_date))
    bs_df.to_csv(bs_file_name, index=False, quoting=1)

    time.sleep(2)

    bs_cleanCSV(bs_file_name)
    
    savant_proceed_flag = 'Y'
except:
    savant_proceed_flag = 'N'


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

resultmsg1, affectedrows1 = insert_csv(action_outfile.name,"pfx_action",curA,cnx)
resultmsg += resultmsg1
affectedrows += affectedrows1
resultmsg1, affectedrows1 = insert_csv(atbat_outfile.name,"pfx_atbat",curA,cnx)
resultmsg += resultmsg1
affectedrows += affectedrows1
resultmsg1, affectedrows1 = insert_csv(pitch_outfile.name,"pfx_pitch",curA,cnx)
resultmsg += resultmsg1
affectedrows += affectedrows1
resultmsg1, affectedrows1 = insert_csv(runner_outfile.name,"pfx_runner",curA,cnx)
resultmsg += resultmsg1
affectedrows += affectedrows1
resultmsg1, affectedrows1 = insert_csv(pickoff_outfile.name,"pfx_pickoff",curA,cnx)
resultmsg += resultmsg1
affectedrows += affectedrows1
resultmsg1, affectedrows1 = insert_csv(miniscore_outfile.name,"pfx_miniscore",curA,cnx)
resultmsg += resultmsg1
affectedrows += affectedrows1
if savant_proceed_flag == 'Y':
    resultmsg1, affectedrows1 = insert_csv(bs_file_name,"pfx_basesav",curA,cnx)
    resultmsg += resultmsg1
    affectedrows += affectedrows1


curA.close()
cnx.close()
#print('status - mySQL DB connection closed')

pastday = active_date - datetime.timedelta(days=data_storage_days)

past_actionfile = "{}/pfx_action_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))
past_atbatfile = "{}/pfx_atbat_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))
past_pitchfile = "{}/pfx_pitch_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))
past_runnerfile = "{}/pfx_runner_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))
past_pickofffile = "{}/pfx_pickoff_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))
past_miniscorefile = "{}/pfx_miniscore_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))
past_baseballsavant = "{}/pfx_baseballsavant___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))

csvstore_delete(past_actionfile)
csvstore_delete(past_atbatfile)
csvstore_delete(past_pitchfile)
csvstore_delete(past_runnerfile)
csvstore_delete(past_pickofffile)
csvstore_delete(past_miniscorefile)
csvstore_delete(past_baseballsavant)



#print('status - csv deleted')

script_endtime = datetime.datetime.now()
script_totaltime = script_endtime - script_starttime

endmsg = '    success - '+str(datetime.datetime.now())+' script total runtime = '+str(script_totaltime)

print(endmsg)

emailmsg = '    started ' + str(script_starttime)+'\n\n'+resultmsg+'\n\n'+endmsg
emailSend.emailSend(msg=emailmsg ,subject=sys.argv[0]+' report')



import mlb_analtablesupdate as mlbtab

script_starttime = datetime.datetime.now()

def update_anal_table(tablename,func,dic):
    dic[tablename] = func()
    return tablename+' updated rows: '+str(dic[tablename])+'\n'

analupdates = {}

tablenames = [
    'analbase_atbat',
    'analbase_pitch',
    'anal_batter_counting',
    'anal_batter_rate',
    'anal_pitcher_counting_ab',
    'anal_pitcher_counting_p',
    'anal_pitcher_rate',
    'anal_team_counting_off',
    'anal_team_counting_def',
    'anal_team_rate'
]

tablefuncs = [
    mlbtab.analbase_atbat,
    mlbtab.analbase_pitch,
    mlbtab.anal_batter_counting,
    mlbtab.anal_batter_rate,
    mlbtab.anal_pitcher_counting_ab,
    mlbtab.anal_pitcher_counting_p,
    mlbtab.anal_pitcher_rate,
    mlbtab.anal_team_counting_off,
    mlbtab.anal_team_counting_def,
    mlbtab.anal_team_rate
]

resultmsg = ''

for tablename, tablefunc in zip(tablenames,tablefuncs):
    resultmsg += update_anal_table(tablename,tablefunc,analupdates)
    
script_endtime = datetime.datetime.now()
script_totaltime = script_endtime - script_starttime

endmsg = '    success - '+str(datetime.datetime.now())+' script total runtime = '+str(script_totaltime)
print(endmsg)

emailmsg = '    started ' + str(script_starttime)+'\n\n'+resultmsg+'\n\n'+endmsg
emailSend.emailSend(msg=emailmsg ,subject='analtables update report')

    
    
print('\n')
