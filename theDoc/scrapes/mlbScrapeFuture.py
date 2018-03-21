#!/anaconda/envs/theDoc/bin/python

'''
This scrpit scrapes basic game information for 
today's games and insert records into pfx_game and 
pfx_prob tables. The intention is to scrape all necessary 
pregame details, with the exception of starting lineups.
Data csv's are kept for (data_storage_days) days then deleted.
'''


import re
import sys
from bs4 import BeautifulSoup, UnicodeDammit
from urllib.request import urlopen
import os
import datetime
import time
import mysql.connector
import mysql
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
                print(    'error -',sys.exc_info()[0],url,'too many errors - moving to next link')
                break
            else: 
                #print('error -',sys.exc_info()[0],url)
                url_attempts += 1
                time.sleep(30)
                continue

def scrapeXMLAttrs(parent, attributes):
    dic = {}
    for attribute in attributes:
        if parent is not None and  attribute in parent.attrs: dic[attribute] = parent[attribute]
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

def csvstore_delete(filename):
    try:
        os.remove(filename)
    except:
        print('    ! WARNING: not deleted - ',filename)
        




# In[60]:

script_starttime = datetime.datetime.now()
print('started -',script_starttime)

data_storage_days = 2

game_attrlist = [
                "gid",
                "game_type",
                "game_pk",
                "game_date",
                "game_time_et", 
                "local_game_time",
                "gameday_sw",

                "home_time",
                "home_time_zone",
                "away_time",
                "away_time_zone",
    
                "venue_id",
                "venue",
                "location",
                "venue_w_chan_loc",

                "home_name_full",
                "home_team_id",
                "home_name_abbrev",
                "home_team_name",
                "home_team_city",
                "home_division_id",
                "home_file_code",
                "home_win",
                "home_loss",
                "home_league",
                "home_type",
                "home_league_id",
                "home_code",

                "away_name_full",
                "away_team_id",
                "away_name_abbrev",
                "away_team_name",
                "away_team_city",
                "away_division_id",
                "away_file_code",
                "away_win",
                "away_loss",
                "away_league",
                "away_type",
                "away_league_id",
                "away_code",

                "double_header_sw",
                "game_nbr",
                "status"
                ]
                
game_scrape_attrlist = [
                "gameday_link",
                "game_type",
                "game_pk",
                "time_date", 
                "gameday_sw",  
    
                "original_date",
    
                "time",
                "ampm",
                "time_zone",

                "home_time",
                "home_time_zone",
                "home_ampm",
                "away_time",
                "away_time_zone",
                "away_ampm",
    
                "venue_id",
                "venue",
                "location",
                "venue_w_chan_loc",

                "home_team_id",
                "home_name_abbrev",
                "home_team_name",
                "home_team_city",
                "home_division",
                "home_code",
                "home_win",
                "home_loss",
                "home_league_id",

                "away_team_id",
                "away_name_abbrev",
                "away_team_name",
                "away_team_city",
                "away_division",
                "away_code",
                "away_win",
                "away_loss",
                "away_league_id",
                "away_code",

                "double_header_sw",
                "game_nbr",
                "league",
                "status"
    
                ]                

prob_attrlist = [
                "id",
                "first_name",
                "last_name",
                "name_display_roster",
                "number",
                "throwinghand",
                "wins",
                "losses",
                "era",
                "so",
                "s_wins",
                "s_losses",
                "s_era",
                "s_so"
                ]

prob_columns = [
                "gid",
                "game_pk",
                "game_date",
                "game_status",
                
                "away_id",
                "away_first_name",
                "away_last_name",
                "away_name_display_roster",
                "away_number",
                "away_throwinghand",
                "away_wins",
                "away_losses",
                "away_era",
                "away_so",
                "away_s_wins",
                "away_s_losses",
                "away_s_era",
                "away_s_so",
    
                "home_id",
                "home_first_name",
                "home_last_name",
                "home_name_display_roster",
                "home_number",
                "home_throwinghand",
                "home_wins",
                "home_losses",
                "home_era",
                "home_so",
                "home_s_wins",
                "home_s_losses",
                "home_s_era",
                "home_s_so",
    
                
    
]

AL_div_dic = {"E":"201","C":"202","W":"200"}
NL_div_dic = {"E":"204","C":"205","W":"203"}


active_date = datetime.datetime.now().date() #- datetime.timedelta(days=1)

game_columns_string = (', '.join(game_attrlist)+'\n')
game_outfile=open("{}/pfx_game_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(active_date)), "w+", encoding='utf-8')
if os.stat(game_outfile.name).st_size==0: game_outfile.write(game_columns_string)                    

prob_columns_string = (', '.join(prob_columns)+'\n')
prob_outfile=open("{}/pfx_prob_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(active_date)), "w+", encoding='utf-8')
if os.stat(prob_outfile.name).st_size==0: prob_outfile.write(prob_columns_string)                    
    
resultmsg = ''

base_url = "http://gd2.mlb.com/components/game/mlb/"

d_url = base_url+"year_"+str(active_date.year)+"/month_"+active_date.strftime('%m')+"/day_"+active_date.strftime('%d')+"/"
#print(active_date)


time.sleep(1)
day_soup = createSoup(d_url, 5)
games = day_soup.find_all("a", href=re.compile("gid_.*"))
num_games = len(games)
print('    number of games - total -',num_games)
resultmsg += '    number of games - total - '+str(num_games)+'\n'
num_games = 0
for gamelink in games:
    
    gid = gamelink.get_text().strip()
    g_url = d_url+gid+"linescore.xml"
    #print(g_url)

    game_soup = createSoup(g_url,2)
    
    if game_soup is None:
        continue
    
    num_games += 1
    
    game_dic = {}
    
    game = game_soup.find("game")
    game_dic = scrapeXMLAttrs(game, game_scrape_attrlist)
    
    game_dic["gid"] = gid
    game_dic["game_date"] = str(active_date)
    
    if game_dic["home_ampm"] == 'PM' and datetime.datetime.strptime(game_dic["home_time"], '%H:%M').time().hour != 12:
        dt = datetime.datetime.combine(datetime.date.today(),datetime.datetime.strptime(game_dic["home_time"], '%H:%M').time()) + datetime.timedelta(hours=12)
        game_dic["home_time"] = dt.time().strftime('%H:%M')
    
    game_dic["game_time_et"] = game_dic["time"]
    time_dic = {"ET":0,"CT":1,"MT":2,"MST":3,"PT":3}
    homedt = datetime.datetime.combine(datetime.date.today(),datetime.datetime.strptime(game_dic["home_time"], '%H:%M').time())
    game_dic["game_time_et"] = str(homedt.hour + time_dic[game_dic["home_time_zone"]])+':'+str(homedt.time().strftime('%M'))
    
    game_dic["local_game_time"] = game_dic["home_time"]
    if game_dic["away_ampm"] == 'PM' and datetime.datetime.strptime(game_dic["away_time"], '%H:%M').time().hour != 12:
        dt = datetime.datetime.combine(datetime.date.today(),datetime.datetime.strptime(game_dic["away_time"], '%H:%M').time()) + datetime.timedelta(hours=12)
        game_dic["away_time"] = dt.time().strftime('%H:%M')
    
    if game_dic["away_team_name"] in game_dic["away_team_city"]:
        game_dic["away_name_full"] = game_dic["away_team_city"]
    else: 
        game_dic["away_name_full"] = game_dic["away_team_city"] + " " + game_dic["away_team_name"]
    game_dic["away_file_code"] = game_dic["away_code"]
    game_dic["away_type"] = 'away'
    if game_dic["league"] == 'NN' or game_dic["league"] == 'NA':
        game_dic["away_league"] = 'NL'
        if game_dic["away_division"] != "":
            game_dic["away_division_id"] = NL_div_dic[game_dic["away_division"]]
        else:
            game_dic["away_division_id"] = ""
    elif game_dic["league"] == 'AA' or game_dic["league"] == 'AN':
        game_dic["away_league"] = 'AL'
        if game_dic["away_division"] != "":
            game_dic["away_division_id"] = AL_div_dic[game_dic["away_division"]]
        else:
            game_dic["away_division_id"] = ""
    else:
        game_dic["away_league"] = game_dic["league"]
        game_dic["away_division_id"] = ""
        
    if game_dic["home_team_name"] in game_dic["home_team_city"]:
        game_dic["home_name_full"] = game_dic["home_team_city"]
    else: 
        game_dic["home_name_full"] = game_dic["home_team_city"] + " " + game_dic["home_team_name"]
    game_dic["home_file_code"] = game_dic["home_code"]
    game_dic["home_type"] = 'home'
    if game_dic["league"] == 'NN' or game_dic["league"] == 'AN':
        game_dic["home_league"] = 'NL'
        if game_dic["home_division"] != "":
            game_dic["home_division_id"] = NL_div_dic[game_dic["home_division"]]
        else:
            game_dic["home_division_id"] = ""
    elif game_dic["league"] == 'AA' or game_dic["league"] == 'NA':
        game_dic["home_league"] = 'AL'
        if game_dic["home_division"] != "":
            game_dic["home_division_id"] = AL_div_dic[game_dic["home_division"]]
        else:
            game_dic["home_division_id"] = ""
    else:
        game_dic["home_league"] = game_dic["league"]
        game_dic["home_division_id"] = ""
    
    scrapeWrite(game_outfile, game_attrlist, game_dic, line_end=True)
    
    
    prob_dic = {}
    prob_dic["gid"] = game_dic["gid"]
    prob_dic["game_pk"] = game_dic["game_pk"]
    prob_dic["game_date"] = game_dic["game_date"]
    prob_dic["game_status"] = game_dic["status"]
    prob_dic["away_score"] = ""
    prob_dic["home_score"] = ""
    
    prob_vals = {}
    
    for side in ['home','away']:
        prob = game_soup.find(side+"_probable_pitcher")
        prob_vals[side] = scrapeXMLAttrs(prob, prob_attrlist)
    
        for key in prob_vals[side].keys():
            prob_dic[side+"_"+key] = prob_vals[side][key]
            
        if prob_dic[side+"_throwinghand"] == "RHP":
            prob_dic[side+"_throwinghand"] = "R"
        elif prob_dic[side+"_throwinghand"] == "LHP":
            prob_dic[side+"_throwinghand"] = "L"
    
    scrapeWrite(prob_outfile, prob_columns, prob_dic, line_end=True)

print('    number of games - scraped -',num_games)    

game_outfile.close()
prob_outfile.close()

cleanCSV(game_outfile.name)
cleanCSV(prob_outfile.name)

#print('status - csv written and cleaned')

affectedrows = 0

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

insert_query1 = (
                "LOAD DATA LOCAL INFILE "
                "'"+game_outfile.name+"' "
                "INTO TABLE pfx_game "
                "COLUMNS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
                )

insert_query2 = (
                "LOAD DATA LOCAL INFILE "
                "'"+prob_outfile.name+"' "
                "INTO TABLE pfx_prob "
                "COLUMNS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
                )


curA.fetchwarnings()

curA.execute(insert_query1)
cnx.commit()
print("    pfx_game affected rows = {}".format(curA.rowcount))
resultmsg1 = str("\n    pfx_game affected rows = {}".format(curA.rowcount))+"\n"
affectedrows1 = curA.rowcount

resultmsg += resultmsg1
affectedrows += affectedrows1

curA.execute(insert_query2)
cnx.commit()
print("    pfx_prob affected rows = {}".format(curA.rowcount))
resultmsg1 = str("\n    pfx_prob affected rows = {}".format(curA.rowcount))+"\n"
affectedrows1 = curA.rowcount

resultmsg += resultmsg1
affectedrows += affectedrows1

curA.close()
cnx.close()
#print('status - mySQL DB connection closed')

pastday = datetime.datetime.now().date() - datetime.timedelta(days=data_storage_days)
past_gamefile = "{}/pfx_game_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))
past_probfile = "{}/pfx_prob_table___{}.csv".format(settings.PFX_SCRAPE_PATH, str(pastday))


csvstore_delete(past_gamefile)
csvstore_delete(past_probfile)

#print('status - csv deleted')

script_endtime = datetime.datetime.now()
script_totaltime = script_endtime - script_starttime

endmsg = '    success - '+str(datetime.datetime.now())+' script total runtime = '+str(script_totaltime)

print(endmsg)

emailmsg = '    started ' + str(script_starttime)+'\n\n'+resultmsg+'\n\n'+endmsg
emailSend.emailSend(msg=emailmsg ,subject=sys.argv[0]+' report')
    
    
print('\n')
