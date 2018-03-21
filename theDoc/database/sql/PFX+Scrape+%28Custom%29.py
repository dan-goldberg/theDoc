
# coding: utf-8

# In[57]:

'''
Copyright (C) 2017
Author: Dan Goldberg
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.'''


import re
import sys
from bs4 import BeautifulSoup, UnicodeDammit
from urllib.request import urlopen
import os
import datetime
import time


script_starttime = datetime.datetime.now()
print(script_starttime)

custom_startdate = datetime.date(2016,4,3)
custom_enddate = datetime.date(2016,10,2)

startdate = custom_startdate
enddate = custom_enddate

game_outfile=open("pfx_game_table___" + str(startdate) + "_" + str(enddate) + "___" + str(datetime.date.today()) + ".csv", "w+", encoding='utf-8')
probables_outfile =open("pfx_probables_table___" + str(startdate) + "_" + str(enddate) + "___" + str(datetime.date.today()) + ".csv", "w+", encoding='utf-8')
action_outfile=open("pfx_action_table___" + str(startdate) + "_" + str(enddate) + "___" + str(datetime.date.today()) + ".csv", "w+", encoding='utf-8')
atbat_outfile=open("pfx_atbat_table___" + str(startdate) + "_" + str(enddate) + "___" + str(datetime.date.today()) + ".csv", "w+", encoding='utf-8')
pitch_outfile=open("pfx_pitch_table___" + str(startdate) + "_" + str(enddate) + "___" + str(datetime.date.today()) + ".csv", "w+", encoding='utf-8')    
runner_outfile=open("pfx_runner_table___" + str(startdate) + "_" + str(enddate) + "___" + str(datetime.date.today()) + ".csv", "w+", encoding='utf-8')
pickoff_outfile =open("pfx_pickoff_table___" + str(startdate) + "_" + str(enddate) + "___" + str(datetime.date.today()) + ".csv", "w+", encoding='utf-8')


if os.stat(game_outfile.name).st_size==0: game_outfile.write("gid,game_type,game_pk,active_date,game_time_et,local_game_time,gameday_sw,stad_id,stad_name,stad_location,stad_state,venue_w_chan_loc,home_name_full,home_id,home_abbrev,home_name_brief,home_name,home_division_id,home_file_code,home_w,home_l,home_league,home_type,home_league_id,home_code,away_name_full,away_id,away_abbrev,away_name_brief,away_name,away_division_id,away_file_code,away_w,away_l,away_league,away_type,away_league_id,away_code\n")                    
else: sys.exit('game file already exists ; system exited')
if os.stat(probables_outfile.name).st_size==0: probables_outfile.write("gid,game_pk,active_date,away_player_id,away_usename,away_lastname,away_rosterdisplayname,away_number,away_throwinghand,away_wins,away_losses,away_era,away_so,away_std_wins,away_std_losses,away_std_era,away_std_so,home_player_id,home_usename,home_lastname,home_rosterdisplayname,home_number,home_throwinghand,home_wins,home_losses,home_era,home_so,home_std_wins,home_std_losses,home_std_era,home_std_so\n")                    
else: sys.exit('probables file already exists ; system exited')  
if os.stat(action_outfile.name).st_size==0: action_outfile.write("gid,game_pk,active_date,inn_num,inn_half,home_bat_fl,b,s,o,des,des_es,event,event_es,tfs,tfs_zulu,player,pitch,event_num,home_team_runs,away_team_runs,play_guid\n")
else: sys.exit('action file already exists ; system exited')
if os.stat(atbat_outfile.name).st_size==0: atbat_outfile.write("gid,game_pk,active_date,inn_num,inn_half,home_bat_fl,ab_num,b,s,o,start_tfs,start_tfs_zulu,batter,stand,b_height,pitcher,p_throws,des,des_es,event_num,event,event_es,event2,event2_es,score,away_team_runs,home_team_runs,play_guid\n")                                      
else: sys.exit('atbat file already exists ; system exited')  
if os.stat(pitch_outfile.name).st_size==0: pitch_outfile.write("gid,game_pk,active_date,inn_num,inn_half,home_bat_fl,ab_num,ab_pitch_num,des,des_es,id_,type_,tfs,tfs_zulu,x,y,event_num,on_1b,on_2b,on_3b,sv_id,start_speed,end_speed,sz_top,sz_bot,pfx_x,pfx_z,px,pz,x0,z0,y0,vx0,vy0,vz0,ax,ay,az,break_y,break_angle,break_length,pitch_type,type_confidence,zone,nasty,spin_dir,spin_rate,cc,mt,play_guid\n")
else: sys.exit('pitch file already exists ; system exited')  
if os.stat(runner_outfile.name).st_size==0: runner_outfile.write("gid,game_pk,active_date,inn_num,inn_half,home_bat_fl,ab_num,id_,start,end,event,event_num,score,rbi,earned\n")
else: sys.exit('runner file already exists ; system exited') 
if os.stat(pickoff_outfile.name).st_size==0: pickoff_outfile.write("gid,game_pk,active_date,inn_num,inn_half,home_bat_fl,ab_num,des,des_es,event_num,catcher,play_guid\n")
else: sys.exit('pickoff file already exists ; system exited') 

base_url = "http://gd2.mlb.com/components/game/mlb/"

delta = enddate - startdate

for i in range(delta.days+1):
    active_date = (startdate+datetime.timedelta(days=i))
    d_url = base_url+"year_"+str(active_date.year)+"/month_"+active_date.strftime('%m')+"/day_"+active_date.strftime('%d')+"/"
    print(active_date)
    
    time.sleep(0.1)
    d_url_attempts = 0
    while d_url_attempts <= 3:
        try:            
            day_soup = BeautifulSoup(urlopen(d_url))
            break
        except:
            print("Error excepted")
            print(sys.exc_info()[0])
            d_url_attempts += 1
            time.sleep(120)
            continue         

    games = day_soup.find_all("a", href=re.compile("gid_.*"))
    for game in games:

        gid = game.get_text().strip()
        g_url = d_url+gid
        print(g_url)

        time.sleep(0.1)
        g_url_attempts = 0
        while g_url_attempts <= 3:
            try:            
                game_soup = BeautifulSoup(urlopen(g_url),"lxml")
                break
            except:
                print("Error excepted")
                print(sys.exc_info()[0])
                g_url_attempts += 1
                time.sleep(120)
                continue    
        
        if game_soup.find("a", href="game.xml"):

            time.sleep(0.1)
            det_url_attempts = 0
            while det_url_attempts <= 3:
                try:            
                    detail_soup = BeautifulSoup(urlopen(g_url+"game.xml"), "lxml")
                    break
                except:
                    print("Error excepted")
                    print(sys.exc_info()[0])
                    det_url_attempts += 1
                    time.sleep(120)
                    continue   
            
            game_detail = detail_soup.find("game")
            teams = game_detail.find_all("team")
            stadium = game_detail.find("stadium")
            for team in teams:
                if team["type"] == "home":
                    if "name_full" in team.attrs: home_name_full = team["name_full"] 
                    else: home_name_full = ""
                    if "id" in team.attrs: home_id = team["id"] 
                    else: home_id = ""
                    if "abbrev" in team.attrs: home_abbrev = team["abbrev"] 
                    else: home_abbrev = ""
                    if "name_brief" in team.attrs: home_name_brief = team["name_brief"] 
                    else: home_name_brief = ""
                    if "name" in team.attrs: home_name = team["name"] 
                    else: home_name = ""
                    if "division_id" in team.attrs: home_division_id = team["division_id"] 
                    else: home_division_id = ""
                    if "file_code" in team.attrs: home_file_code = team["file_code"] 
                    else: home_file_code = ""
                    if "l" in team.attrs: home_l = team["l"] 
                    else: home_l = ""
                    if "w" in team.attrs: home_w = team["w"] 
                    else: home_w = ""
                    if "league" in team.attrs: home_league = team["league"] 
                    else: home_league = ""
                    if "type" in team.attrs: home_type = team["type"] 
                    else: home_type = ""
                    if "league_id" in team.attrs: home_league_id = team["league_id"] 
                    else: home_league_id = ""
                    if "code" in team.attrs: home_code = team["code"] 
                    else: home_code = ""

                elif team["type"] == "away":

                    if "name_full" in team.attrs: away_name_full = team["name_full"] 
                    else: away_name_full = ""
                    if "id" in team.attrs: away_id = team["id"] 
                    else: away_id = ""
                    if "abbrev" in team.attrs: away_abbrev = team["abbrev"] 
                    else: away_abbrev = ""
                    if "name_brief" in team.attrs: away_name_brief = team["name_brief"] 
                    else: away_name_brief = ""
                    if "name" in team.attrs: away_name = team["name"] 
                    else: away_name = ""
                    if "division_id" in team.attrs: away_division_id = team["division_id"] 
                    else: away_division_id = ""
                    if "file_code" in team.attrs: away_file_code = team["file_code"] 
                    else: away_file_code = ""
                    if "l" in team.attrs: away_l = team["l"] 
                    else: away_l = ""
                    if "w" in team.attrs: away_w = team["w"] 
                    else: away_w = ""
                    if "league" in team.attrs: away_league = team["league"] 
                    else: away_league = ""
                    if "type" in team.attrs: away_type = team["type"] 
                    else: away_type = ""
                    if "league_id" in team.attrs: away_league_id = team["league_id"] 
                    else: away_league_id = ""
                    if "code" in team.attrs: away_code = team["code"] 
                    else: away_code = ""

            if "type" in game_detail.attrs: game_type = game_detail["type"] 
            else: game_type = ""
            if "game_pk" in game_detail.attrs: game_pk = game_detail["game_pk"] 
            else: game_pk = ""
            if "game_time_et" in game_detail.attrs: game_time_et = game_detail["game_time_et"] 
            else: game_time_et = ""
            if "local_game_time" in game_detail.attrs: local_game_time = game_detail["local_game_time"] 
            else: local_game_time = ""
            if "gameday_sw" in game_detail.attrs: gameday_sw = game_detail["gameday_sw"] 
            else: gameday_sw = ""

            if "id" in stadium.attrs: stad_id = stadium["id"] 
            else: stad_id = ""
            if "venue_w_chan_loc" in stadium.attrs: venue_w_chan_loc = stadium["venue_w_chan_loc"] 
            else: venue_w_chan_loc = ""
            if "location" in stadium.attrs: stad_location = stadium["location"] 
            else: stad_location = ""
            if "name" in stadium.attrs: stad_name = stadium["name"] 
            else: stad_name = ""

            if 1==1:
                game_outfile.write(gid
                                    +","+game_type
                                    +","+game_pk
                                    +","+str(active_date)
                                    +","+game_time_et
                                    +","+local_game_time
                                    +","+gameday_sw

                                    +","+stad_id
                                    +","+stad_name
                                    +","+stad_location 
                                    +","+venue_w_chan_loc  

                                    +","+home_name_full
                                    +","+home_id
                                    +","+home_abbrev
                                    +","+home_name_brief
                                    +","+home_name
                                    +","+home_division_id
                                    +","+home_file_code
                                    +","+home_w
                                    +","+home_l
                                    +","+home_league
                                    +","+home_type
                                    +","+home_league_id
                                    +","+home_code

                                    +","+away_name_full
                                    +","+away_id
                                    +","+away_abbrev
                                    +","+away_name_brief
                                    +","+away_name
                                    +","+away_division_id
                                    +","+away_file_code
                                    +","+away_w
                                    +","+away_l
                                    +","+away_league
                                    +","+away_type
                                    +","+away_league_id
                                    +","+away_code
                                    +"\n"
                                    )

            time.sleep(0.1)
            gc_url_attempts = 0
            while gc_url_attempts <= 3:
                try:            
                    gamecenter_soup = BeautifulSoup(urlopen(g_url+"gamecenter.xml"))
                    break
                except:
                    print("Error excepted")
                    print(sys.exc_info()[0])
                    gc_url_attempts += 1
                    time.sleep(120)
                    continue   
            
            probables = gamecenter_soup.find("probables")
            
            home = probables.find("home")
            away = probables.find("away")
                
            if home.find("player_id") != []: home_player_id = home.player_id.string 
            else: home_player_id = ""
            if home.find("usename") != []: home_usename = home.usename.string 
            else: home_usename = ""
            if home.find("lastname") != []: home_lastname = home.lastname.string 
            else: home_lastname = ""
            if home.find("rosterdisplayname") != []: home_rosterdisplayname = home.rosterdisplayname.string 
            else: home_rosterdisplayname = ""
            if home.find("number") != []: home_number = home.number.string 
            else: home_number = ""
            if home.find("throwinghand") != []: home_throwinghand = home.throwinghand.string 
            else: home_throwinghand = ""
            if home.find("wins") != []: home_wins = home.wins.string 
            else: home_wins = ""
            if home.find("losses") != []: home_losses = home.losses.string 
            else: home_losses = ""
            if home.find("era") != []: home_era = home.era.string 
            else: home_era = ""
            if home.find("so") != []: home_so = home.so.string 
            else: home_so = ""
            if home.find("std_wins") != []: home_std_wins = home.std_wins.string 
            else: home_std_wins = ""
            if home.find("std_losses") != []: home_std_losses = home.std_losses.string 
            else: home_std_losses = ""
            if home.find("std_era") != []: home_std_era = home.std_era.string 
            else: home_std_era = ""
            if home.find("std_so") != []: home_std_so = home.std_so.string 
            else: home_std_so = ""

            if away.find("player_id") != []: away_player_id = away.player_id.string 
            else: away_player_id = ""
            if away.find("usename") != []: away_usename = away.usename.string 
            else: away_usename = ""
            if away.find("lastname") != []: away_lastname = away.lastname.string 
            else: away_lastname = ""
            if away.find("rosterdisplayname") != []: away_rosterdisplayname = away.rosterdisplayname.string 
            else: away_rosterdisplayname = ""
            if away.find("number") != []: away_number = away.number.string 
            else: away_number = ""
            if away.find("throwinghand") != []: away_throwinghand = away.throwinghand.string 
            else: away_throwinghand = ""
            if away.find("wins") != []: away_wins = away.wins.string 
            else: away_wins = ""
            if away.find("losses") != []: away_losses = away.losses.string 
            else: away_losses = ""
            if away.find("era") != []: away_era = away.era.string 
            else: away_era = ""
            if away.find("so") != []: away_so = away.so.string 
            else: away_so = ""
            if away.find("std_wins") != []: away_std_wins = away.std_wins.string 
            else: away_std_wins = ""
            if away.find("std_losses") != []: away_std_losses = away.std_losses.string 
            else: away_std_losses = ""
            if away.find("std_era") != []: away_std_era = away.std_era.string 
            else: away_std_era = ""
            if away.find("std_so") != []: away_std_so = away.std_so.string 
            else: away_std_so = ""

            if 1==1:
                probables_outfile.write(gid
                                            +","+game_pk
                                            +","+str(active_date)

                                            +","+away_player_id
                                            +","+away_usename
                                            +","+away_lastname
                                            +","+away_rosterdisplayname
                                            +","+away_number
                                            +","+away_throwinghand
                                            +","+away_wins
                                            +","+away_losses
                                            +","+away_era
                                            +","+away_so
                                            +","+away_std_wins
                                            +","+away_std_losses
                                            +","+away_std_era
                                            +","+away_std_so

                                            +","+home_player_id
                                            +","+home_usename
                                            +","+home_lastname
                                            +","+home_rosterdisplayname
                                            +","+home_number
                                            +","+home_throwinghand
                                            +","+home_wins
                                            +","+home_losses
                                            +","+home_era
                                            +","+home_so
                                            +","+home_std_wins
                                            +","+home_std_losses
                                            +","+home_std_era
                                            +","+home_std_so
                                            +"\n"
                                            )
            
            time.sleep(0.1)
            inn_url_attempts = 0
            while inn_url_attempts <= 3:
                try:            
                    inning_soup = BeautifulSoup(urlopen(g_url+"inning/inning_all.xml"))
                    break
                except:
                    print("Error excepted")
                    print(sys.exc_info()[0])
                    inn_url_attempts += 1
                    time.sleep(120)
                    continue   
                    
            innings = inning_soup.find_all("inning")
            for inning in innings:
                inn_num = inning["num"]

                halves = inning.find_all({"top","bottom"})
                for half in halves:
                    inn_half = half.name
                    if inn_half == "top":
                        home_bat_fl = "0"
                    elif inn_half == "bottom":
                        home_bat_fl = "1"

                    actions = half.find_all("action")
                    if actions == []:
                        pass
                    else:
                        for action in actions:

                            if "b" in action.attrs: b = action["b"] 
                            else: b = ""
                            if "s" in action.attrs: s = action["s"] 
                            else: s = ""
                            if "o" in action.attrs: o = action["o"] 
                            else: o = ""
                            if "des" in action.attrs: des = action["des"] 
                            else: des = ""
                            if "des_es" in action.attrs: des_es = action["des_es"] 
                            else: des_es = ""
                            if "event" in action.attrs: event = action["event"] 
                            else: event = ""
                            if "event_es" in action.attrs: event_es = action["event_es"] 
                            else: event_es = ""
                            if "tfs" in action.attrs: tfs = action["tfs"] 
                            else: tfs = ""
                            if "tfs_zulu" in action.attrs: tfs_zulu = action["tfs_zulu"] 
                            else: tfs_zulu = ""
                            if "player" in action.attrs: player = action["player"] 
                            else: player = ""
                            if "pitch" in action.attrs: pitch = action["pitch"] 
                            else: pitch = ""
                            if "event_num" in action.attrs: event_num = action["event_num"] 
                            else: event_num = ""
                            if "home_team_runs" in action.attrs: home_team_runs = action["home_team_runs"] 
                            else: home_team_runs = ""
                            if "away_team_runs" in action.attrs: away_team_runs = action["away_team_runs"] 
                            else: away_team_runs = ""
                            if "play_guid" in action.attrs: play_guid = action["play_guid"] 
                            else: play_guid = ""


                            action_outfile.write(gid
                                                 +","+game_pk
                                                 +","+str(active_date)                                         
                                                 +","+inn_num
                                                 +","+inn_half
                                                 +","+home_bat_fl
                                                 +","+b
                                                 +","+s
                                                 +","+o
                                                 +","+des
                                                 +","+des_es
                                                 +","+event
                                                 +","+event_es
                                                 +","+tfs
                                                 +","+tfs_zulu
                                                 +","+player
                                                 +","+pitch
                                                 +","+event_num
                                                 +","+home_team_runs
                                                 +","+away_team_runs
                                                 +","+play_guid
                                                 +"\n"
                                                 )


                    atbats = half.find_all("atbat")
                    for atbat in atbats:

                        if "num" in atbat.attrs: ab_num = atbat["num"] 
                        else: ab_num = ""
                        if "b" in atbat.attrs: b = atbat["b"] 
                        else: b = ""
                        if "s" in atbat.attrs: s = atbat["s"]
                        else: s = ""
                        if "o" in atbat.attrs: o = atbat["o"] 
                        else: o = ""
                        if "start_tfs" in atbat.attrs: start_tfs = atbat["start_tfs"] 
                        else: start_tfs = ""
                        if "start_tfs_zulu" in atbat.attrs: start_tfs_zulu = atbat["start_tfs_zulu"] 
                        else: start_tfs_zulu = ""
                        if "batter" in atbat.attrs: batter = atbat["batter"] 
                        else: batter = ""
                        if "stand" in atbat.attrs: stand = atbat["stand"] 
                        else: stand = ""
                        if "b_height" in atbat.attrs: b_height = atbat["b_height"] 
                        else: b_height = ""
                        if "pitcher" in atbat.attrs: pitcher = atbat["pitcher"]
                        else: pitcher = ""
                        if "p_throws" in atbat.attrs: p_throws = atbat["p_throws"] 
                        else: p_throws = ""
                        if "des" in atbat.attrs: des = atbat["des"] 
                        else: des = ""
                        if "des_es" in atbat.attrs: des_es = atbat["des_es"] 
                        else: des_es = ""
                        if "event_num" in atbat.attrs: event_num = atbat["event_num"] 
                        else: event_num = ""
                        if "event" in atbat.attrs: event = atbat["event"] 
                        else: event = ""
                        if "event_es" in atbat.attrs: event_es = atbat["event_es"] 
                        else: event_es = ""
                        if "event2" in atbat.attrs: event2 = atbat["event2"] 
                        else: event2 = ""
                        if "event2_es" in atbat.attrs: event2_es = atbat["event2_es"] 
                        else: event2_es = ""
                        if "score" in atbat.attrs: score = atbat["score"] 
                        else: score = ""
                        if "away_team_runs" in atbat.attrs: away_team_runs = atbat["away_team_runs"] 
                        else: away_team_runs = ""
                        if "home_team_runs" in atbat.attrs: home_team_runs = atbat["home_team_runs"] 
                        else: home_team_runs = ""
                        if "play_guid" in atbat.attrs: play_guid = atbat["play_guid"] 
                        else: play_guid = ""

                        atbat_outfile.write(gid
                                            +","+game_pk
                                            +","+str(active_date)
                                            +","+inn_num
                                            +","+inn_half
                                            +","+home_bat_fl
                                            +","+ab_num
                                            +","+b
                                            +","+s
                                            +","+o
                                            +","+start_tfs
                                            +","+start_tfs_zulu
                                            +","+batter
                                            +","+stand
                                            +","+b_height
                                            +","+pitcher
                                            +","+p_throws
                                            +","+des
                                            +","+des_es
                                            +","+event_num
                                            +","+event
                                            +","+event_es
                                            +","+event2
                                            +","+event2_es
                                            +","+score
                                            +","+away_team_runs
                                            +","+home_team_runs
                                            +","+play_guid
                                            +"\n"
                                            )

                        ab_pitch_num = 0
                        pitches = atbat.find_all("pitch")
                        for pitch in pitches:

                            ab_pitch_num += 1

                            if "des" in pitch.attrs: des = pitch["des"] 
                            else: des = ""
                            if "des_es" in pitch.attrs: des_es = pitch["des_es"]
                            else: des_es = ""
                            if "id" in pitch.attrs: id_ = pitch["id"] 
                            else: id_ = ""
                            if "type" in pitch.attrs: type_ = pitch["type"] 
                            else: type_ = ""
                            if "tfs" in pitch.attrs: tfs = pitch["tfs"] 
                            else: tfs = ""
                            if "tfs_zulu" in pitch.attrs: tfs_zulu = pitch["tfs_zulu"] 
                            else: tfs_zulu = ""
                            if "x" in pitch.attrs: x = pitch["x"] 
                            else: x = ""
                            if "y" in pitch.attrs: y = pitch["y"] 
                            else: y = ""
                            if "event_num" in pitch.attrs: event_num = pitch["event_num"] 
                            else: event_num = ""
                            if "on_1b" in pitch.attrs: on_1b = pitch["on_1b"]
                            else: on_1b = ""
                            if "on_2b" in pitch.attrs: on_2b = pitch["on_2b"] 
                            else: on_2b = ""
                            if "on_3b" in pitch.attrs: on_3b = pitch["on_3b"] 
                            else: on_3b = ""
                            if "sv_id" in pitch.attrs: sv_id = pitch["sv_id"] 
                            else: sv_id = ""
                            if "start_speed" in pitch.attrs: start_speed = pitch["start_speed"] 
                            else: start_speed = ""
                            if "end_speed" in pitch.attrs: end_speed = pitch["end_speed"] 
                            else: end_speed = ""
                            if "sz_top" in pitch.attrs: sz_top = pitch["sz_top"] 
                            else: sz_top = ""
                            if "sz_bot" in pitch.attrs: sz_bot = pitch["sz_bot"] 
                            else: sz_bot = ""
                            if "pfx_x" in pitch.attrs: pfx_x = pitch["pfx_x"] 
                            else: pfx_x = ""
                            if "pfx_z" in pitch.attrs: pfx_z = pitch["pfx_z"]
                            else: pfx_z = ""
                            if "px" in pitch.attrs: px = pitch["px"] 
                            else: px = ""
                            if "pz" in pitch.attrs: pz = pitch["pz"] 
                            else: pz = ""
                            if "x0" in pitch.attrs: x0 = pitch["x0"] 
                            else: x0 = ""
                            if "z0" in pitch.attrs: z0 = pitch["z0"] 
                            else: z0 = ""
                            if "y0" in pitch.attrs: y0 = pitch["y0"] 
                            else: y0 = ""
                            if "vx0" in pitch.attrs: vx0 = pitch["vx0"] 
                            else: vx0 = ""
                            if "vy0" in pitch.attrs: vy0 = pitch["vy0"]
                            else: vy0 = ""
                            if "vz0" in pitch.attrs: vz0 = pitch["vz0"] 
                            else: vz0 = ""
                            if "ax" in pitch.attrs: ax = pitch["ax"] 
                            else: ax = ""
                            if "ay" in pitch.attrs: ay = pitch["ay"] 
                            else: ay = ""
                            if "az" in pitch.attrs: az = pitch["az"] 
                            else: az = ""
                            if "break_y" in pitch.attrs: break_y = pitch["break_y"] 
                            else: break_y = ""
                            if "break_angle" in pitch.attrs: break_angle = pitch["break_angle"] 
                            else: break_angle = ""
                            if "break_length" in pitch.attrs: break_length = pitch["break_length"] 
                            else: break_length = ""
                            if "pitch_type" in pitch.attrs: pitch_type = pitch["pitch_type"] 
                            else: pitch_type = ""
                            if "type_confidence" in pitch.attrs: type_confidence = pitch["type_confidence"] 
                            else: type_confidence = ""
                            if "zone" in pitch.attrs: zone = pitch["zone"] 
                            else: zone = ""
                            if "nasty" in pitch.attrs: nasty = pitch["nasty"] 
                            else: nasty = ""
                            if "spin_dir" in pitch.attrs: spin_dir = pitch["spin_dir"] 
                            else: spin_dir = ""
                            if "spin_rate" in pitch.attrs: spin_rate = pitch["spin_rate"] 
                            else: spin_rate = ""
                            if "cc" in pitch.attrs: cc = pitch["cc"] 
                            else: cc = ""
                            if "mt" in pitch.attrs: mt = pitch["mt"] 
                            else: mt = ""
                            if "play_guid" in pitch.attrs: play_guid = pitch["play_guid"] 
                            else: play_guid = ""

                            pitch_outfile.write(gid
                                                +","+game_pk
                                                +","+str(active_date)
                                                +","+inn_num
                                                +","+inn_half
                                                +","+home_bat_fl
                                                +","+ab_num
                                                +","+str(ab_pitch_num)                           
                                                +","+des
                                                +","+des_es
                                                +","+id_
                                                +","+type_
                                                +","+tfs
                                                +","+tfs_zulu
                                                +","+x
                                                +","+y
                                                +","+event_num
                                                +","+on_1b
                                                +","+on_2b
                                                +","+on_3b
                                                +","+sv_id
                                                +","+start_speed
                                                +","+end_speed
                                                +","+sz_top
                                                +","+sz_bot
                                                +","+pfx_x
                                                +","+pfx_z
                                                +","+px
                                                +","+pz
                                                +","+x0
                                                +","+z0
                                                +","+y0
                                                +","+vx0
                                                +","+vy0
                                                +","+vz0
                                                +","+ax
                                                +","+ay
                                                +","+az
                                                +","+break_y
                                                +","+break_angle
                                                +","+break_length
                                                +","+pitch_type
                                                +","+type_confidence
                                                +","+zone
                                                +","+nasty
                                                +","+spin_dir
                                                +","+spin_rate
                                                +","+cc
                                                +","+mt
                                                +","+play_guid
                                                +"\n"
                                                )


                        runners = atbat.find_all("runner")
                        if runners == []:
                            pass
                        else:  
                            for runner in runners:

                                if "id" in runner.attrs: id_ = runner["id"] 
                                else: id_ = ""
                                if "start" in runner.attrs: start = runner["start"] 
                                else: start = ""
                                if "end" in runner.attrs: end = runner["end"] 
                                else: end = ""
                                if "event" in runner.attrs: event = runner["event"] 
                                else: event = ""
                                if "event_num" in runner.attrs: event_num = runner["event_num"] 
                                else: event_num = ""
                                if "score" in runner.attrs: score = runner["score"] 
                                else: score = ""
                                if "rbi" in runner.attrs: rbi = runner["rbi"] 
                                else: rbi = ""
                                if "earned" in runner.attrs: earned = runner["earned"] 
                                else: earned = ""

                                runner_outfile.write(gid
                                                        +","+game_pk
                                                        +","+str(active_date)
                                                        +","+inn_num
                                                        +","+inn_half
                                                        +","+home_bat_fl
                                                        +","+ab_num
                                                        +","+id_
                                                        +","+start
                                                        +","+end
                                                        +","+event
                                                        +","+event_num
                                                        +","+score
                                                        +","+rbi
                                                        +","+earned
                                                        +"\n"
                                                     )


                        pickoffs = atbat.find_all("po")
                        if pickoffs == []:
                            pass
                        else:  
                            pickoffs = atbat.find_all("po")
                            for pickoff in pickoffs:

                                if "des" in pickoff.attrs: des = pickoff["des"] 
                                else: des = ""
                                if "des_es" in pickoff.attrs: des_es = pickoff["des_es"] 
                                else: des_es = ""
                                if "event_num" in pickoff.attrs: event_num = pickoff["event_num"] 
                                else: event_num = ""
                                if "catcher" in pickoff.attrs: catcher = pickoff["catcher"] 
                                else: catcher = ""
                                if "play_guid" in pickoff.attrs: play_guid = pickoff["play_guid"] 
                                else: play_guid = ""

                                pickoff_outfile.write(gid
                                                        +","+game_pk
                                                        +","+str(active_date)
                                                        +","+inn_num
                                                        +","+inn_half
                                                        +","+home_bat_fl
                                                        +","+ab_num
                                                        +","+des
                                                        +","+des_es
                                                        +","+event_num
                                                        +","+catcher
                                                        +","+play_guid
                                                        +"\n"
                                                      )


script_endtime = datetime.datetime.now()
script_totaltime = script_endtime - script_starttime

print('\n script total runtime = '+ str(script_totaltime))
print('\n success ' + str(datetime.datetime.now()))


# In[ ]:

items = []

for n in range(1000):
    
    events = test_soup.find_all(event_num = str(n))
    for event in events:
        items.append(event.name)
        
s=set(items)
print(s)


# In[ ]:

runner_allitems = []
pickoff_allitems = []
pitch_allitems = []
atbat_allitems = []
action_allitems = []

exp_days = ["http://gd2.mlb.com/components/game/mlb/year_2016/month_06/day_11/",
           "http://gd2.mlb.com/components/game/mlb/year_2016/month_08/day_19/",
           "http://gd2.mlb.com/components/game/mlb/year_2015/month_06/day_29/",
           "http://gd2.mlb.com/components/game/mlb/year_2014/month_07/day_02/",
           "http://gd2.mlb.com/components/game/mlb/year_2016/month_04/day_25/",
           "http://gd2.mlb.com/components/game/mlb/year_2013/month_09/day_21/",
           "http://gd2.mlb.com/components/game/mlb/year_2016/month_05/day_08/"]

for expday in exp_days:
    
    test_day_url = expday
    test_day_soup = BeautifulSoup(urlopen(test_day_url))

    for game in test_day_soup.find_all("a", href=re.compile("gid_.*")):
            g = game.get_text().strip()
            test_game_url = test_day_url+g+"inning/inning_all.xml"
            print(test_game_url)
            time.sleep(0.1)
            test_game_soup = BeautifulSoup(urlopen(test_game_url),"lxml")

            pieces = test_game_soup.find_all("pitch")
            for piece in pieces:
                items = (list(piece.attrs.keys()))
                for item in items:
                    pitch_allitems.append(item)
            
            pieces = test_game_soup.find_all("atbat")
            for piece in pieces:
                items = (list(piece.attrs.keys()))
                for item in items:
                    atbat_allitems.append(item)           
            
            pieces = test_game_soup.find_all("action")
            for piece in pieces:
                items = (list(piece.attrs.keys()))
                for item in items:
                    action_allitems.append(item)
            
            pieces = test_game_soup.find_all("runner")
            for piece in pieces:
                items = (list(piece.attrs.keys()))
                for item in items:
                    runner_allitems.append(item)

            pieces = test_game_soup.find_all("po")
            for piece in pieces:
                items = (list(piece.attrs.keys()))
                for item in items:
                    pickoff_allitems.append(item)
    
print(set(pitch_allitems))
print(set(atbat_allitems))
print(set(action_allitems))
print(set(pickoff_allitems))
print(set(runner_allitems))


# In[30]:

arr = ['player_id', 
 'usename',
'lastname',
'rosterdisplayname',  
'number', 
'throwinghand',
'wins', 
'losses', 
'era',  
'so',  
'std_wins',
'std_losses', 
'std_era', 
'std_so']

for side in ['home','away']:
    
    cleanarr = []
    
    for ar in arr:
        cleanarr.append('if '+side+'.find("'+ar+'") != []: '+side+"_"+ar+' = '+ar+'.string else: '+side+"_"+ar+' = ""')

    for cleanar in cleanarr:
        print(cleanar)
    
    
    writearr = []

    for ar in arr:
        writearr.append('+","+'+side+"_"+ar)

    for cleanwritear in writearr:
        print(cleanwritear)


# In[3]:

import re
import sys
from bs4 import BeautifulSoup, UnicodeDammit
from urllib.request import urlopen
import os
import datetime
import time


game_allitems = []

exp_days = ["http://gd2.mlb.com/components/game/mlb/year_2014/month_05/day_14/",
           "http://gd2.mlb.com/components/game/mlb/year_2016/month_08/day_19/"]

for expday in exp_days:
    
    test_day_url = expday
    test_day_soup = BeautifulSoup(urlopen(test_day_url))

    for game in test_day_soup.find_all("a", href=re.compile("gid_.*")):
            g = game.get_text().strip()
            test_game_url = test_day_url+g+"gamecenter.xml"
            print(test_game_url)
            time.sleep(0.1)
            test_game_soup = BeautifulSoup(urlopen(test_game_url),"lxml")

            pieces = test_game_soup.find("probables")
            for piece in pieces.contents:
                
            
    
print(set(game_allitems))


# In[26]:

homeset = []
awayset = []

for piece in pieces:
    homes = piece.find("home")
    aways = piece.find("away")
    for home in homes:
        homeset.append(home.name)
    for away in aways:
        awayset.append(away.name)
        
print(set(homeset),set(awayset))


# In[53]:

game_pk


# In[ ]:

ga

