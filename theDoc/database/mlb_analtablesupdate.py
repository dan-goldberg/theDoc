
# coding: utf-8

# In[ ]:

import mysql.connector
import datetime
import numpy as np



# In[ ]:

def mlb_connect():
    
    try:
        cnx = mysql.connector.connect(user='dan', password='',
                                    host='127.0.0.1',
                                    database='mlb')
        print('    Successfully connected to mySQL DB')
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
            
    return cnx


# In[ ]:

def get_missingdates(table):
    
    cnx = mlb_connect()

    curA = cnx.cursor(buffered=True)
    
    if table == "anal_team_counting_off":
        col = "anal_game_date_off"
    elif table == "anal_team_counting_def":
        col = "anal_game_date_def"
    elif table == "analbase_atbat" or table == "analbase_pitch":
        col = "game_date"
    else:
        col = "anal_game_date"

    query1 = (
        """SELECT DISTINCT 
        DATE(game_date - interval '1' day)
        FROM
        pfx_game
        WHERE game_type = 'R'
        AND game_date NOT IN (SELECT """+col+""" FROM """+table+""")
        ;"""
        )

    curA.execute(query1)

    dates = np.array(curA.fetchall())
    
    return dates, cnx, curA


# In[ ]:

def analbase_atbat():

    print('update starting: analbase_atbat')
    
    starttime = datetime.datetime.now()
    
    dates, cnx, curA = get_missingdates("analbase_atbat") 
    
    affectedrows = 0

    query_analbase_atbat = ("""

    INSERT INTO analbase_atbat

    SELECT DISTINCT 

    (@runsscored := CASE
        when ab.des LIKE '%scores%scores%scores%scores%'
            or ab.des LIKE '%homers%scores%scores%scores%'
            or ab.des LIKE '%grand slam%scores%scores%scores%' 
            or ab.des LIKE '%hits an inside-the-park home run%scores%scores%scores%' 
        then 4
        when ab.des LIKE '%scores%scores%scores%' 
            or ab.des LIKE '%homers%scores%scores%' 
            or ab.des LIKE '%hits an inside-the-park home run%scores%scores%' 
        then 3
        when ab.des LIKE '%scores%scores%' 
            or ab.des LIKE '%homers%scores%' 
            or ab.des LIKE '%hits an inside-the-park home run%scores%' 
        then 2
        when ab.des LIKE '%scores%' 
            or ab.des LIKE '%homers%' 
            or ab.des LIKE '%hits an inside-the-park home run%' 
            or (ab.event_ = 'Grounded Into DP' AND ab.score = 'T')
        then 1
        else 0
    END) as runsscored,

    ##### ids
    ab.gid,
    ab.game_pk,
    ab.ab_num,
    batter,
    pitcher,
    ab.event_num,
    ##### opportunities
    ab.game_date,
    ab.inn_num,
    ab.home_bat_fl,
    des,
    stand as bat_side,
    p_throws,
    CASE WHEN stand = p_throws THEN 1 ELSE 0 END as p_same_hand,
    o - CASE 
        WHEN (ab.event_ LIKE '% Out' AND ab.event_ <> 'Runner Out') OR ab.event_ LIKE '%out' THEN 1
        WHEN ab.event_ LIKE 'Double Play' OR ab.event_ LIKE '% DP' THEN 2
        WHEN ab.event_ LIKE 'Triple Play' THEN 3
        ELSE 0
    END - CASE WHEN event2 LIKE '%Out' THEN 1 ELSE 0 END
    as o_sit,
    CASE 
        WHEN ab.home_bat_fl = 1 THEN (ab.home_teamruns - @runsscored) - ab.away_teamruns
        ELSE (ab.away_teamruns - @runsscored)- ab.home_teamruns
    END as team_lead,

    CASE WHEN r1.id_ IS NOT NULL THEN 1 ELSE 0 END as r1b_fl,
    CASE WHEN r2.id_ IS NOT NULL THEN 1 ELSE 0 END as r2b_fl,
    CASE WHEN r3.id_ IS NOT NULL THEN 1 ELSE 0 END as r3b_fl,


    ##### events


    CASE 
        WHEN (ab.event_ LIKE '% Out' AND ab.event_ <> 'Runner Out') OR ab.event_ LIKE '%out' THEN 1
        WHEN ab.event_ LIKE 'Double Play' OR ab.event_ LIKE '% DP' THEN 2
        WHEN ab.event_ LIKE 'Triple Play' THEN 3
        ELSE 0
    END as outsmade,
    CASE WHEN ab.event_ = 'Single' THEN 1 ELSE 0 END as _1b_fl,
    CASE WHEN ab.event_ = 'Double' THEN 1 ELSE 0 END as _2b_fl,
    CASE WHEN ab.event_ = 'Triple' THEN 1 ELSE 0 END as _3b_fl,
    CASE WHEN ab.event_ = 'Home Run' OR ab.des LIKE '%home run%' OR ab.des LIKE '%grand slam%' THEN 1 ELSE 0 END as _hr_fl,
    CASE WHEN ab.event_ = 'Walk' OR ab.event_ = 'Intent Walk' THEN 1 ELSE 0 END as _bb_fl,
    CASE WHEN ab.event_ = 'Strikeout' THEN 1 ELSE 0 END as _k_fl,
    CASE WHEN ab.des LIKE '%strikes out%' THEN 1 ELSE 0 END as _kswing_fl,
    CASE WHEN ab.des LIKE '%called out on strikes%' THEN 1 ELSE 0 END as _klook_fl,
    CASE WHEN ab.event_ = 'Hit By Pitch' THEN 1 ELSE 0 END as _hbp_fl,
    CASE WHEN ab.event_ IN ('Sac Bunt','Sac Fly','Sac Fly DP','Sacrifice Bunt DP') THEN 1 ELSE 0 END as _sac_fl,
    CASE WHEN ab.event_ = 'Field Error' THEN 1 ELSE 0 END as _e_fl,
    CASE WHEN 
        des LIKE '%ground%' OR 
        ab.des LIKE '%reaches on a throwing error%' OR
        ab.event_ LIKE '%Bunt Groundout%'
    THEN 1 ELSE 0 END as groundball_fl,
    CASE WHEN 
        des LIKE '%line drive%' OR
        ab.des LIKE '%lines%'
    THEN 1 ELSE 0 END as linedrive_fl,
    CASE WHEN ab.score = 'T' THEN 1 ELSE 0 END as _score_fl,
    b,
    s,
    case when o = 3 THEN 1 ELSE 0 END as endinn_fl,
    @runnerbasesadv := CASE 
        WHEN r1.end_base = '2B' THEN 1
        WHEN r1.end_base = '3B' THEN 2
        WHEN r1.end_base = '' AND r1.score = 'T' THEN 3
        WHEN r1.end_base = '' AND r1.score = '' THEN -1
        WHEN r1.end_base IS NULL THEN 0
    END 
    +
    CASE 
        WHEN r2.end_base = '3B' THEN 1
        WHEN r2.end_base = '' AND r2.score = 'T' THEN 2
        WHEN r2.end_base = '' AND r2.score = '' THEN -2
        WHEN r2.end_base IS NULL THEN 0
    END 
    +
    CASE 
        WHEN r3.end_base = '' AND r3.score = 'T' THEN 1
        WHEN r3.end_base = '' AND r3.score = '' THEN -3
        WHEN r3.end_base IS NULL THEN 0
    END
    as runnerbasesadv,
    @selfbasesadv := CASE
        WHEN r0.end_base = '1B' THEN 1 
        WHEN r0.end_base = '2B' THEN 2
        WHEN r0.end_base = '3B' THEN 3
        WHEN r0.end_base = '' AND r0.score = 'T' THEN 4
        ELSE 0
    END
    as selfbasesadv,
    @runnerbasesadv + @selfbasesadv as totbasesadv,
    CASE WHEN ab.home_bat_fl = 0 THEN (ab.pitcher = prob.home_player_id) ELSE (ab.pitcher = prob.away_player_id) END AS sp_fl,

    NULL,
    NULL


    FROM pfx_atbat as ab
    LEFT JOIN pfx_runner as r0
    ON r0.gid = ab.gid AND r0.ab_num = ab.ab_num AND (r0.event_num = ab.event_num OR r0.event_ = ab.event_) AND r0.start_base = ''
    LEFT JOIN pfx_runner as r1
    ON r1.gid = ab.gid AND r1.ab_num = ab.ab_num AND (r1.event_num = ab.event_num OR r1.event_ = ab.event_) AND r1.start_base = '1B'
    LEFT JOIN pfx_runner as r2
    ON r2.gid = ab.gid AND r2.ab_num = ab.ab_num AND (r2.event_num = ab.event_num OR r2.event_ = ab.event_) AND r2.start_base = '2B'
    LEFT JOIN pfx_runner as r3
    ON r3.gid = ab.gid AND r3.ab_num = ab.ab_num AND (r3.event_num = ab.event_num OR r3.event_ = ab.event_) AND r3.start_base = '3B'
    LEFT JOIN pfx_prob as prob
    ON prob.gid = ab.gid

    WHERE ab.gid IN (SELECT pfx_game.gid FROM pfx_game WHERE pfx_game.gid NOT IN (SELECT DISTINCT analbase_atbat.gid FROM analbase_atbat) )

    """)

    curA.execute(query_analbase_atbat)
    cnx.commit()    

    affectedrows += curA.rowcount

    print('   ',str(datetime.datetime.now() - starttime))

    curA.close()
    cnx.close()
    print('    mySQL DB connection closed')
    print('table updated: analbase_atbat')
    
    return affectedrows


# In[ ]:

def analbase_pitch():
    
    print('update starting: analbase_pitch')
    
    starttime = datetime.datetime.now()
    
    dates, cnx, curA = get_missingdates("analbase_pitch")
    
    affectedrows = 0
    
    query_analbase_pitch = ("""
    INSERT INTO analbase_pitch

    SELECT DISTINCT

    (@runsscored := CASE
        when ab.des LIKE '%scores%scores%scores%scores%'
            or ab.des LIKE '%homers%scores%scores%scores%'
            or ab.des LIKE '%grand slam%scores%scores%scores%' 
            or ab.des LIKE '%hits an inside-the-park home run%scores%scores%scores%' 
        then 4
        when ab.des LIKE '%scores%scores%scores%' 
            or ab.des LIKE '%homers%scores%scores%' 
            or ab.des LIKE '%hits an inside-the-park home run%scores%scores%' 
        then 3
        when ab.des LIKE '%scores%scores%' 
            or ab.des LIKE '%homers%scores%' 
            or ab.des LIKE '%hits an inside-the-park home run%scores%' 
        then 2
        when ab.des LIKE '%scores%' 
            or ab.des LIKE '%homers%' 
            or ab.des LIKE '%hits an inside-the-park home run%' 
            or (ab.event_ = 'Grounded Into DP' AND ab.score = 'T')
        then 1
        else 0
    END) as runsscored_p,

    ##### ids
    p.gid,
    p.game_pk,
    p.id_ as pitch_id,
    p.ab_num,
    p.event_num,
    ##### opportunities
    p.game_date,
    p.inn_num,
    p.home_bat_fl,
    p.des,
    p.ab_pitch_num,

    ab.stand as bat_side,
    ab.p_throws,
    CASE WHEN ab.stand = ab.p_throws THEN 1 ELSE 0 END as p_same_hand,
    o - CASE 
        WHEN (ab.event_ LIKE '% Out' AND ab.event_ <> 'Runner Out') OR ab.event_ LIKE '%out' THEN 1
        WHEN ab.event_ LIKE 'Double Play' OR ab.event_ LIKE '% DP' THEN 2
        WHEN ab.event_ LIKE 'Triple Play' THEN 3
        ELSE 0
    END - CASE WHEN ab.event2 LIKE '%Out' THEN 1 ELSE 0 END
    as o_sit_p,
    CASE 
        WHEN ab.home_bat_fl = 1 THEN (ab.home_teamruns - @runsscored) - ab.away_teamruns
        ELSE (ab.away_teamruns - @runsscored)- ab.home_teamruns
    END as team_lead_p,

    CASE WHEN p.on_1b IS NOT NULL THEN 1 ELSE 0 END as r1b_fl,
    CASE WHEN p.on_2b IS NOT NULL THEN 1 ELSE 0 END as r2b_fl,
    CASE WHEN p.on_3b IS NOT NULL THEN 1 ELSE 0 END as r3b_fl,

    ##### outcomes

    CASE WHEN p.des IN ('Ball','Ball In Dirt','Intent Ball','Pitchout') THEN 1 ELSE 0 END as ball_fl,
    CASE WHEN p.des LIKE '%Strike%' OR p.des LIKE 'Foul%' OR p.des LIKE 'In play%' THEN 1 ELSE 0 END as strike_fl,
    CASE WHEN p.des IN ('Hit By Pitch') THEN 1 ELSE 0 END as hbp_fl,
    CASE WHEN p.des IN ('Called Strike') THEN 1 ELSE 0 END as calledstrike_fl,
    CASE WHEN p.des IN ('Swinging Strike','Swinging Strike (Blocked)','Missed Bunt','Swinging Pitchout') THEN 1 ELSE 0 END as swingmissstrike_fl,
    CASE WHEN p.des LIKE 'Foul%' THEN 1 ELSE 0 END as foulstrike_fl,
    CASE WHEN p.des LIKE 'In play%' THEN 1 ELSE 0 END as inplay_fl,
    CASE WHEN p.des IN ('Swinging Strike','Swinging Strike (Blocked)','Missed Bunt','Swinging Pitchout') OR p.des LIKE 'Foul%' OR p.des LIKE 'In play%' THEN 1 ELSE 0 END as swing_fl,
    CASE WHEN p.des IN ('Ball','Ball In Dirt','Intent Ball','Pitchout','Hit By Pitch','Called Strike') THEN 1 ELSE 0 END as take_fl,
    CASE WHEN p.ab_pitch_num = 1 AND p.type_ = 'S' THEN 1 ELSE 0 END as firstpstrike_fl,
    CASE WHEN p.ab_pitch_num = 1 AND p.type_ != 'X' THEN 1 ELSE 0 END as firstpnotinplay_fl,
    CASE WHEN p.ab_pitch_num = 2 AND p.type_ = 'S' THEN 1 ELSE 0 END as secondpstrike_fl,

    p.pitch_type,
    p.spin_rate,
    p.start_speed,
    p.end_speed,
    p.zone,
    p.nasty,

    CASE WHEN ( px BETWEEN (@xmin := -0.708) AND (@xmax := 0.708) AND (pz BETWEEN (@zmin := p.sz_bot) AND @zmin + (2/12) OR pz BETWEEN (@zmax := p.sz_top) - (2/12) AND @zmax) ) OR ( (px BETWEEN @xmin AND @xmin + (2/12) OR px BETWEEN @xmax - (2/12) AND @xmax) AND pz BETWEEN @zmin AND @zmax ) THEN 1 ELSE 0 END as zoneedge_in2,
    CASE WHEN ( px BETWEEN @xmin AND @xmax AND (pz BETWEEN @zmin - (2/12) AND @zmin OR pz BETWEEN @zmax AND @zmax + (2/12)) ) OR ( (px BETWEEN @xmin - (2/12) AND @xmin OR px BETWEEN @xmax AND @xmax + (2/12)) AND pz BETWEEN @zmin AND @zmax ) THEN 1 ELSE 0 END as zoneedge_out2,
    CASE WHEN ( px BETWEEN @xmin AND @xmax AND (pz BETWEEN @zmin AND @zmin + (4/12) OR pz BETWEEN @zmax - (4/12) AND @zmax) ) OR ( (px BETWEEN @xmin AND @xmin + (4/12) OR px BETWEEN @xmax - (4/12) AND @xmax) AND pz BETWEEN @zmin AND @zmax ) THEN 1 ELSE 0 END as zoneedge_in4,
    CASE WHEN ( px BETWEEN @xmin AND @xmax AND (pz BETWEEN @zmin - (4/12) AND @zmin OR pz BETWEEN @zmax AND @zmax + (4/12)) ) OR ( (px BETWEEN @xmin - (4/12) AND @xmin OR px BETWEEN @xmax AND @xmax + (4/12)) AND pz BETWEEN @zmin AND @zmax ) THEN 1 ELSE 0 END as zoneedge_out4,
    CASE WHEN ( px BETWEEN @xmin AND @xmax AND pz BETWEEN @zmin AND @zmax AND (px <= @xmin + (2/12) OR px >= @xmax - (2/12)) AND (pz <= @zmin + (2/12) OR pz >= @zmax - (2/12)) ) THEN 1 ELSE 0 END as zonecorn_in2,
    CASE WHEN ( ( px BETWEEN @xmin - (2/12) AND @xmin + (2/12) AND pz BETWEEN @zmin - (2/12) AND @zmin + (2/12) ) OR ( px BETWEEN @xmax - (2/12) AND @xmax + (2/12) AND pz BETWEEN @zmin - (2/12) AND @zmin + (2/12) ) OR ( px BETWEEN @xmin - (2/12) AND @xmin + (2/12) AND pz BETWEEN @zmax - (2/12) AND @zmax + (2/12) ) OR ( px BETWEEN @xmax - (2/12) AND @xmax + (2/12) AND pz BETWEEN @zmax - (2/12) AND @zmax + (2/12) ) ) AND px NOT BETWEEN @xmin AND @xmax AND pz NOT BETWEEN @zmin AND @zmax THEN 1 ELSE 0 END as zonecorn_out2,
    CASE WHEN ( px BETWEEN @xmin AND @xmax AND pz BETWEEN @zmin AND @zmax AND (px <= @xmin + (4/12) OR px >= @xmax - (4/12)) AND (pz <= @zmin + (4/12) OR pz >= @zmax - (4/12)) ) THEN 1 ELSE 0 END as zonecorn_in4,
    CASE WHEN ( ( px BETWEEN @xmin - (4/12) AND @xmin + (4/12) AND pz BETWEEN @zmin - (4/12) AND @zmin + (4/12) ) OR ( px BETWEEN @xmax - (4/12) AND @xmax + (4/12) AND pz BETWEEN @zmin - (4/12) AND @zmin + (4/12) ) OR ( px BETWEEN @xmin - (4/12) AND @xmin + (4/12) AND pz BETWEEN @zmax - (4/12) AND @zmax + (4/12) ) OR ( px BETWEEN @xmax - (4/12) AND @xmax + (4/12) AND pz BETWEEN @zmax - (4/12) AND @zmax + (4/12) ) ) AND px NOT BETWEEN @xmin AND @xmax AND pz NOT BETWEEN @zmin AND @zmax THEN 1 ELSE 0 END as zonecorn_out4,
    CASE WHEN (px BETWEEN @xmin + (@xmax-@xmin)/2 - (1.5/12) AND @xmin + (@xmax-@xmin)/2 + (1.5/12) AND pz BETWEEN @zmin + (@zmax-@zmin)/2 - (1.5/12) AND @zmin + (@zmax-@zmin)/2 + (1.5/12)) THEN 1 ELSE 0 END as zone_mid3,
    CASE WHEN (px BETWEEN @xmin + (@xmax-@xmin)/2 - (3/12) AND @xmin + (@xmax-@xmin)/2 + (3/12) AND pz BETWEEN @zmin + (@zmax-@zmin)/2 - (3/12) AND @zmin + (@zmax-@zmin)/2 + (3/12)) THEN 1 ELSE 0 END as zone_mid6,
    CASE WHEN ( (px NOT BETWEEN @xmin - (4/12) AND @xmax + (4/12) ) OR ( pz NOT BETWEEN @zmin - (4/12) AND @zmax + (4/12) ) ) THEN 1 ELSE 0 END as zone_bigmiss4,

    CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN end_speed END as fastball_endspeed,
    CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN spin_rate END as fastball_spinrate,
    CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN spin_dir END as fastball_spindir,
    CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN pfx_x END as fastball_pfx_x,
    CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN pfx_z END as fastball_pfx_z,
    CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z,2)) END as fastball_mnorm,
    CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z-9,2)) END as fastball_adjmnorm,

    CASE WHEN p.pitch_type IN ('CU','KC') THEN end_speed END as curveball_endspeed,
    CASE WHEN p.pitch_type IN ('CU','KC') THEN spin_rate END as curveball_spinrate,
    CASE WHEN p.pitch_type IN ('CU','KC') THEN spin_dir END as curveball_spindir,
    CASE WHEN p.pitch_type IN ('CU','KC') THEN pfx_x END as curveball_pfx_x,
    CASE WHEN p.pitch_type IN ('CU','KC') THEN pfx_z END as curveball_pfx_z,
    CASE WHEN p.pitch_type IN ('CU','KC') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z,2)) END as curveball_mnorm,
    CASE WHEN p.pitch_type IN ('CU','KC') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z-9,2)) END as curveball_adjmnorm,

    CASE WHEN p.pitch_type IN ('SL','SC') THEN end_speed END as slider_endspeed,
    CASE WHEN p.pitch_type IN ('SL','SC') THEN spin_rate END as slider_spinrate,
    CASE WHEN p.pitch_type IN ('SL','SC') THEN spin_dir END as slider_spindir,
    CASE WHEN p.pitch_type IN ('SL','SC') THEN pfx_x END as slider_pfx_x,
    CASE WHEN p.pitch_type IN ('SL','SC') THEN pfx_z END as slider_pfx_z,
    CASE WHEN p.pitch_type IN ('SL','SC') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z,2)) END as slider_mnorm,
    CASE WHEN p.pitch_type IN ('SL','SC') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z-9,2)) END as slider_adjmnorm,

    CASE WHEN p.pitch_type IN ('CH','SI') THEN end_speed END as changeup_endspeed,
    CASE WHEN p.pitch_type IN ('CH','SI') THEN spin_rate END as changeup_spinrate,
    CASE WHEN p.pitch_type IN ('CH','SI') THEN spin_dir END as changeup_spindir,
    CASE WHEN p.pitch_type IN ('CH','SI') THEN pfx_x END as changeup_pfx_x,
    CASE WHEN p.pitch_type IN ('CH','SI') THEN pfx_z END as changeup_pfx_z,
    CASE WHEN p.pitch_type IN ('CH','SI') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z,2)) END as changeup_mnorm,
    CASE WHEN p.pitch_type IN ('CH','SI') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z-9,2)) END as changeup_adjmnorm,

    NULL,
    NULL

    FROM pfx_pitch AS p
    LEFT JOIN pfx_atbat AS ab
    ON p.gid = ab.gid AND p.ab_num = ab.ab_num
    LEFT JOIN pfx_game AS g
    ON p.gid = g.gid

    WHERE p.gid IN (SELECT pfx_game.gid FROM pfx_game WHERE pfx_game.gid NOT IN (SELECT DISTINCT analbase_pitch.gid FROM analbase_pitch) )
    AND ab.gid IN (SELECT pfx_game.gid FROM pfx_game WHERE pfx_game.gid NOT IN (SELECT DISTINCT analbase_pitch.gid FROM analbase_pitch) )
    ;""")

    
    curA.execute(query_analbase_pitch)
    cnx.commit()    
    
    affectedrows += curA.rowcount

    print('   ',str(datetime.datetime.now() - starttime))

    curA.close()
    cnx.close()
    print('    mySQL DB connection closed')
    print('table updated: analbase_pitch')
    
    return affectedrows


# In[ ]:

def anal_batter_counting():
    
    print('update starting: anal_batter_counting')
    
    dates, cnx, curA = get_missingdates("anal_batter_counting")
    
    affectedrows = 0    
    
    for date in dates:

        starttime = datetime.datetime.now()

        activedate = date[0]

        query2 = "SET @analdate := '"+str(activedate)+"';"
        curA.execute(query2)

        query3 = """

            INSERT INTO anal_batter_counting

            SELECT DISTINCT
            @analdate + interval '1' day as anal_game_date,
            a.batter,
            p.pname,

            ###########  STD  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as pa_std,

            @pa_out := COUNT(CASE WHEN @logic_std = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std,
            @outsmade := SUM(CASE WHEN @logic_std = 1 THEN a.outsmade ELSE 0 END) as outsmade_std,
            @hits := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std,
            @onbases := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std,
            @singles := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std,
            @doubles := COUNT(CASE WHEN @logic_std = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std,
            @triples := COUNT(CASE WHEN @logic_std = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std,
            @homeruns := COUNT(CASE WHEN @logic_std = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std,
            @walks := COUNT(CASE WHEN @logic_std = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std,
            @k := COUNT(CASE WHEN @logic_std = 1 AND a._k_fl > 0 THEN 1 END) as k_std,
            @klook := COUNT(CASE WHEN @logic_std = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std,
            @kswing := COUNT(CASE WHEN @logic_std = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std,
            @hbp := COUNT(CASE WHEN @logic_std = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std,
            @sacs := COUNT(CASE WHEN @logic_std = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_std,
            @groundballs := COUNT(CASE WHEN @logic_std = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std,
            @linedrives := COUNT(CASE WHEN @logic_std = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std,
            @runnerbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std,
            @selfbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std,
            @totbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std,

            ###########  last60  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 THEN a.gid END) as gp_last60,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60,

            @pa_out := COUNT(CASE WHEN @logic_last60 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60,
            @outsmade := SUM(CASE WHEN @logic_last60 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60,
            @hits := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60,
            @onbases := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60,
            @singles := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last60,
            @doubles := COUNT(CASE WHEN @logic_last60 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last60,
            @triples := COUNT(CASE WHEN @logic_last60 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last60,
            @homeruns := COUNT(CASE WHEN @logic_last60 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last60,
            @walks := COUNT(CASE WHEN @logic_last60 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last60,
            @k := COUNT(CASE WHEN @logic_last60 = 1 AND a._k_fl > 0 THEN 1 END) as k_last60,
            @klook := COUNT(CASE WHEN @logic_last60 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last60,
            @kswing := COUNT(CASE WHEN @logic_last60 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last60,
            @hbp := COUNT(CASE WHEN @logic_last60 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last60,
            @sacs := COUNT(CASE WHEN @logic_last60 = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_last60,
            @groundballs := COUNT(CASE WHEN @logic_last60 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last60,
            @linedrives := COUNT(CASE WHEN @logic_last60 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last60,
            @runnerbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last60,
            @selfbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last60,
            @totbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60,

            ###########  last30  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last30 := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate) = 1 THEN a.gid END) as gp_last30,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30,

            @pa_out := COUNT(CASE WHEN @logic_last30 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30,
            @outsmade := SUM(CASE WHEN @logic_last30 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30,
            @hits := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30,
            @onbases := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30,
            @singles := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last30,
            @doubles := COUNT(CASE WHEN @logic_last30 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last30,
            @triples := COUNT(CASE WHEN @logic_last30 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last30,
            @homeruns := COUNT(CASE WHEN @logic_last30 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last30,
            @walks := COUNT(CASE WHEN @logic_last30 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last30,
            @k := COUNT(CASE WHEN @logic_last30 = 1 AND a._k_fl > 0 THEN 1 END) as k_last30,
            @klook := COUNT(CASE WHEN @logic_last30 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last30,
            @kswing := COUNT(CASE WHEN @logic_last30 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last30,
            @hbp := COUNT(CASE WHEN @logic_last30 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last30,
            @sacs := COUNT(CASE WHEN @logic_last30 = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_last30,
            @groundballs := COUNT(CASE WHEN @logic_last30 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last30,
            @linedrives := COUNT(CASE WHEN @logic_last30 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last30,
            @runnerbasesadv := SUM(CASE WHEN @logic_last30 = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last30,
            @selfbasesadv := SUM(CASE WHEN @logic_last30 = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last30,
            @totbasesadv := SUM(CASE WHEN @logic_last30 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30,

            ###########  last10  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last10 := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate) = 1 THEN a.gid END) as gp_last10,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10,

            @pa_out := COUNT(CASE WHEN @logic_last10 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10,
            @outsmade := SUM(CASE WHEN @logic_last10 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10,
            @hits := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10,
            @onbases := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10,
            @singles := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last10,
            @doubles := COUNT(CASE WHEN @logic_last10 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last10,
            @triples := COUNT(CASE WHEN @logic_last10 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last10,
            @homeruns := COUNT(CASE WHEN @logic_last10 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last10,
            @walks := COUNT(CASE WHEN @logic_last10 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last10,
            @k := COUNT(CASE WHEN @logic_last10 = 1 AND a._k_fl > 0 THEN 1 END) as k_last10,
            @klook := COUNT(CASE WHEN @logic_last10 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last10,
            @kswing := COUNT(CASE WHEN @logic_last10 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last10,
            @hbp := COUNT(CASE WHEN @logic_last10 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last10,
            @sacs := COUNT(CASE WHEN @logic_last10 = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_last10,
            @groundballs := COUNT(CASE WHEN @logic_last10 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last10,
            @linedrives := COUNT(CASE WHEN @logic_last10 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last10,
            @runnerbasesadv := SUM(CASE WHEN @logic_last10 = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last10,
            @selfbasesadv := SUM(CASE WHEN @logic_last10 = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last10,
            @totbasesadv := SUM(CASE WHEN @logic_last10 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10,

            ###########  std2  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std2 := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std2,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as pa_std2,

            @pa_out := COUNT(CASE WHEN @logic_std2 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std2,
            @outsmade := SUM(CASE WHEN @logic_std2 = 1 THEN a.outsmade ELSE 0 END) as outsmade_std2,
            @hits := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std2,
            @onbases := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std2,
            @singles := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std2,
            @doubles := COUNT(CASE WHEN @logic_std2 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std2,
            @triples := COUNT(CASE WHEN @logic_std2 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std2,
            @homeruns := COUNT(CASE WHEN @logic_std2 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std2,
            @walks := COUNT(CASE WHEN @logic_std2 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std2,
            @k := COUNT(CASE WHEN @logic_std2 = 1 AND a._k_fl > 0 THEN 1 END) as k_std2,
            @klook := COUNT(CASE WHEN @logic_std2 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std2,
            @kswing := COUNT(CASE WHEN @logic_std2 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std2,
            @hbp := COUNT(CASE WHEN @logic_std2 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std2,
            @sacs := COUNT(CASE WHEN @logic_std2 = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_std2,
            @groundballs := COUNT(CASE WHEN @logic_std2 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std2,
            @linedrives := COUNT(CASE WHEN @logic_std2 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std2,
            @runnerbasesadv := SUM(CASE WHEN @logic_std2 = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std2,
            @selfbasesadv := SUM(CASE WHEN @logic_std2 = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std2,
            @totbasesadv := SUM(CASE WHEN @logic_std2 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std2,

            ###########  std3  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std3 := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std3,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as pa_std3,

            @pa_out := COUNT(CASE WHEN @logic_std3 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std3,
            @outsmade := SUM(CASE WHEN @logic_std3 = 1 THEN a.outsmade ELSE 0 END) as outsmade_std3,
            @hits := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std3,
            @onbases := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std3,
            @singles := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std3,
            @doubles := COUNT(CASE WHEN @logic_std3 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std3,
            @triples := COUNT(CASE WHEN @logic_std3 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std3,
            @homeruns := COUNT(CASE WHEN @logic_std3 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std3,
            @walks := COUNT(CASE WHEN @logic_std3 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std3,
            @k := COUNT(CASE WHEN @logic_std3 = 1 AND a._k_fl > 0 THEN 1 END) as k_std3,
            @klook := COUNT(CASE WHEN @logic_std3 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std3,
            @kswing := COUNT(CASE WHEN @logic_std3 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std3,
            @hbp := COUNT(CASE WHEN @logic_std3 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std3,
            @sacs := COUNT(CASE WHEN @logic_std3 = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_std3,
            @groundballs := COUNT(CASE WHEN @logic_std3 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std3,
            @linedrives := COUNT(CASE WHEN @logic_std3 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std3,
            @runnerbasesadv := SUM(CASE WHEN @logic_std3 = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std3,
            @selfbasesadv := SUM(CASE WHEN @logic_std3 = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std3,
            @totbasesadv := SUM(CASE WHEN @logic_std3 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std3,

            ###########  std_rh  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std_rh := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_std_rh,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_rh,

            @pa_out := COUNT(CASE WHEN @logic_std_rh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_rh,
            @outsmade := SUM(CASE WHEN @logic_std_rh = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_rh,
            @hits := COUNT(CASE WHEN @logic_std_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_rh,
            @onbases := COUNT(CASE WHEN @logic_std_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_rh,
            @singles := COUNT(CASE WHEN @logic_std_rh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std_rh,
            @doubles := COUNT(CASE WHEN @logic_std_rh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std_rh,
            @triples := COUNT(CASE WHEN @logic_std_rh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std_rh,
            @homeruns := COUNT(CASE WHEN @logic_std_rh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std_rh,
            @walks := COUNT(CASE WHEN @logic_std_rh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std_rh,
            @k := COUNT(CASE WHEN @logic_std_rh = 1 AND a._k_fl > 0 THEN 1 END) as k_std_rh,
            @klook := COUNT(CASE WHEN @logic_std_rh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std_rh,
            @kswing := COUNT(CASE WHEN @logic_std_rh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std_rh,
            @hbp := COUNT(CASE WHEN @logic_std_rh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std_rh,
            @sacs := COUNT(CASE WHEN @logic_std_rh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_std_rh,
            @groundballs := COUNT(CASE WHEN @logic_std_rh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std_rh,
            @linedrives := COUNT(CASE WHEN @logic_std_rh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std_rh,
            @runnerbasesadv := SUM(CASE WHEN @logic_std_rh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std_rh,
            @selfbasesadv := SUM(CASE WHEN @logic_std_rh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std_rh,
            @totbasesadv := SUM(CASE WHEN @logic_std_rh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_rh,

            ###########  last60_rh  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last60_rh := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_last60_rh,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_rh,

            @pa_out := COUNT(CASE WHEN @logic_last60_rh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_rh,
            @outsmade := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_rh,
            @hits := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_rh,
            @onbases := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_rh,
            @singles := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last60_rh,
            @doubles := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last60_rh,
            @triples := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last60_rh,
            @homeruns := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last60_rh,
            @walks := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last60_rh,
            @k := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._k_fl > 0 THEN 1 END) as k_last60_rh,
            @klook := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last60_rh,
            @kswing := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last60_rh,
            @hbp := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last60_rh,
            @sacs := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_last60_rh,
            @groundballs := COUNT(CASE WHEN @logic_last60_rh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last60_rh,
            @linedrives := COUNT(CASE WHEN @logic_last60_rh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last60_rh,
            @runnerbasesadv := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last60_rh,
            @selfbasesadv := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last60_rh,
            @totbasesadv := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_rh,

            ###########  last30_rh  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last30_rh := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_last30_rh,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_rh,

            @pa_out := COUNT(CASE WHEN @logic_last30_rh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_rh,
            @outsmade := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_rh,
            @hits := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_rh,
            @onbases := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_rh,
            @singles := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last30_rh,
            @doubles := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last30_rh,
            @triples := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last30_rh,
            @homeruns := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last30_rh,
            @walks := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last30_rh,
            @k := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._k_fl > 0 THEN 1 END) as k_last30_rh,
            @klook := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last30_rh,
            @kswing := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last30_rh,
            @hbp := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last30_rh,
            @sacs := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_last30_rh,
            @groundballs := COUNT(CASE WHEN @logic_last30_rh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last30_rh,
            @linedrives := COUNT(CASE WHEN @logic_last30_rh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last30_rh,
            @runnerbasesadv := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last30_rh,
            @selfbasesadv := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last30_rh,
            @totbasesadv := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_rh,

            ###########  last10_rh  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last10_rh := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_last10_rh,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_rh,

            @pa_out := COUNT(CASE WHEN @logic_last10_rh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_rh,
            @outsmade := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_rh,
            @hits := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_rh,
            @onbases := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_rh,
            @singles := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last10_rh,
            @doubles := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last10_rh,
            @triples := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last10_rh,
            @homeruns := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last10_rh,
            @walks := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last10_rh,
            @k := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._k_fl > 0 THEN 1 END) as k_last10_rh,
            @klook := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last10_rh,
            @kswing := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last10_rh,
            @hbp := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last10_rh,
            @sacs := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_last10_rh,
            @groundballs := COUNT(CASE WHEN @logic_last10_rh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last10_rh,
            @linedrives := COUNT(CASE WHEN @logic_last10_rh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last10_rh,
            @runnerbasesadv := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last10_rh,
            @selfbasesadv := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last10_rh,
            @totbasesadv := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_rh,

            ###########  std2_rh  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std2_rh := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_std2_rh,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as pa_std2_rh,

            @pa_out := COUNT(CASE WHEN @logic_std2_rh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std2_rh,
            @outsmade := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.outsmade ELSE 0 END) as outsmade_std2_rh,
            @hits := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std2_rh,
            @onbases := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std2_rh,
            @singles := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std2_rh,
            @doubles := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std2_rh,
            @triples := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std2_rh,
            @homeruns := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std2_rh,
            @walks := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std2_rh,
            @k := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._k_fl > 0 THEN 1 END) as k_std2_rh,
            @klook := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std2_rh,
            @kswing := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std2_rh,
            @hbp := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std2_rh,
            @sacs := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_std2_rh,
            @groundballs := COUNT(CASE WHEN @logic_std2_rh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std2_rh,
            @linedrives := COUNT(CASE WHEN @logic_std2_rh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std2_rh,
            @runnerbasesadv := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std2_rh,
            @selfbasesadv := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std2_rh,
            @totbasesadv := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std2_rh,

            ###########  std3_rh   ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std3_rh  := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_std3_rh ,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as pa_std3_rh ,

            @pa_out := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std3_rh ,
            @outsmade := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.outsmade ELSE 0 END) as outsmade_std3_rh ,
            @hits := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std3_rh ,
            @onbases := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std3_rh ,
            @singles := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std3_rh ,
            @doubles := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std3_rh ,
            @triples := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std3_rh ,
            @homeruns := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std3_rh ,
            @walks := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std3_rh ,
            @k := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._k_fl > 0 THEN 1 END) as k_std3_rh ,
            @klook := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std3_rh ,
            @kswing := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std3_rh ,
            @hbp := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std3_rh ,
            @sacs := COUNT(CASE WHEN @logic_std3_rh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_std3_rh,
            @groundballs := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std3_rh ,
            @linedrives := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std3_rh ,
            @runnerbasesadv := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std3_rh ,
            @selfbasesadv := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std3_rh ,
            @totbasesadv := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std3_rh ,

            ###########  std_lh  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std_lh := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_std_lh,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_lh,

            @pa_out := COUNT(CASE WHEN @logic_std_lh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_lh,
            @outsmade := SUM(CASE WHEN @logic_std_lh = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_lh,
            @hits := COUNT(CASE WHEN @logic_std_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_lh,
            @onbases := COUNT(CASE WHEN @logic_std_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_lh,
            @singles := COUNT(CASE WHEN @logic_std_lh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std_lh,
            @doubles := COUNT(CASE WHEN @logic_std_lh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std_lh,
            @triples := COUNT(CASE WHEN @logic_std_lh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std_lh,
            @homeruns := COUNT(CASE WHEN @logic_std_lh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std_lh,
            @walks := COUNT(CASE WHEN @logic_std_lh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std_lh,
            @k := COUNT(CASE WHEN @logic_std_lh = 1 AND a._k_fl > 0 THEN 1 END) as k_std_lh,
            @klook := COUNT(CASE WHEN @logic_std_lh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std_lh,
            @kswing := COUNT(CASE WHEN @logic_std_lh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std_lh,
            @hbp := COUNT(CASE WHEN @logic_std_lh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std_lh,
            @sacs := COUNT(CASE WHEN @logic_std_lh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_std_lh,
            @groundballs := COUNT(CASE WHEN @logic_std_lh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std_lh,
            @linedrives := COUNT(CASE WHEN @logic_std_lh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std_lh,
            @runnerbasesadv := SUM(CASE WHEN @logic_std_lh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std_lh,
            @selfbasesadv := SUM(CASE WHEN @logic_std_lh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std_lh,
            @totbasesadv := SUM(CASE WHEN @logic_std_lh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_lh,

            ###########  last60_lh  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last60_lh := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_last60_lh,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_lh,

            @pa_out := COUNT(CASE WHEN @logic_last60_lh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_lh,
            @outsmade := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_lh,
            @hits := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_lh,
            @onbases := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_lh,
            @singles := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last60_lh,
            @doubles := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last60_lh,
            @triples := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last60_lh,
            @homeruns := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last60_lh,
            @walks := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last60_lh,
            @k := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._k_fl > 0 THEN 1 END) as k_last60_lh,
            @klook := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last60_lh,
            @kswing := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last60_lh,
            @hbp := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last60_lh,
            @sacs := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_last60_lh,
            @groundballs := COUNT(CASE WHEN @logic_last60_lh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last60_lh,
            @linedrives := COUNT(CASE WHEN @logic_last60_lh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last60_lh,
            @runnerbasesadv := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last60_lh,
            @selfbasesadv := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last60_lh,
            @totbasesadv := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_lh,

            ###########  last30_lh  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last30_lh := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_last30_lh,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_lh,

            @pa_out := COUNT(CASE WHEN @logic_last30_lh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_lh,
            @outsmade := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_lh,
            @hits := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_lh,
            @onbases := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_lh,
            @singles := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last30_lh,
            @doubles := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last30_lh,
            @triples := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last30_lh,
            @homeruns := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last30_lh,
            @walks := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last30_lh,
            @k := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._k_fl > 0 THEN 1 END) as k_last30_lh,
            @klook := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last30_lh,
            @kswing := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last30_lh,
            @hbp := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last30_lh,
            @sacs := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_last30_lh,
            @groundballs := COUNT(CASE WHEN @logic_last30_lh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last30_lh,
            @linedrives := COUNT(CASE WHEN @logic_last30_lh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last30_lh,
            @runnerbasesadv := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last30_lh,
            @selfbasesadv := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last30_lh,
            @totbasesadv := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_lh,

            ###########  last10_lh  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last10_lh := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_last10_lh,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_lh,

            @pa_out := COUNT(CASE WHEN @logic_last10_lh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_lh,
            @outsmade := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_lh,
            @hits := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_lh,
            @onbases := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_lh,
            @singles := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last10_lh,
            @doubles := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last10_lh,
            @triples := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last10_lh,
            @homeruns := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last10_lh,
            @walks := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last10_lh,
            @k := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._k_fl > 0 THEN 1 END) as k_last10_lh,
            @klook := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last10_lh,
            @kswing := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last10_lh,
            @hbp := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last10_lh,
            @sacs := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_last10_lh,
            @groundballs := COUNT(CASE WHEN @logic_last10_lh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last10_lh,
            @linedrives := COUNT(CASE WHEN @logic_last10_lh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last10_lh,
            @runnerbasesadv := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last10_lh,
            @selfbasesadv := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last10_lh,
            @totbasesadv := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_lh,

            ###########  std2_lh  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std2_lh := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_std2_lh,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as pa_std2_lh,

            @pa_out := COUNT(CASE WHEN @logic_std2_lh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std2_lh,
            @outsmade := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.outsmade ELSE 0 END) as outsmade_std2_lh,
            @hits := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std2_lh,
            @onbases := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std2_lh,
            @singles := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std2_lh,
            @doubles := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std2_lh,
            @triples := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std2_lh,
            @homeruns := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std2_lh,
            @walks := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std2_lh,
            @k := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._k_fl > 0 THEN 1 END) as k_std2_lh,
            @klook := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std2_lh,
            @kswing := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std2_lh,
            @hbp := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std2_lh,
            @sacs := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_std2_lh,
            @groundballs := COUNT(CASE WHEN @logic_std2_lh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std2_lh,
            @linedrives := COUNT(CASE WHEN @logic_std2_lh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std2_lh,
            @runnerbasesadv := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std2_lh,
            @selfbasesadv := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std2_lh,
            @totbasesadv := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std2_lh,

            ###########  std3_lh   ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std3_lh  := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_std3_lh ,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as pa_std3_lh ,

            @pa_out := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std3_lh ,
            @outsmade := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.outsmade ELSE 0 END) as outsmade_std3_lh ,
            @hits := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std3_lh ,
            @onbases := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std3_lh ,
            @singles := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std3_lh ,
            @doubles := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std3_lh ,
            @triples := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std3_lh ,
            @homeruns := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std3_lh ,
            @walks := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std3_lh ,
            @k := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._k_fl > 0 THEN 1 END) as k_std3_lh ,
            @klook := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std3_lh ,
            @kswing := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std3_lh ,
            @hbp := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std3_lh ,
            @sacs := COUNT(CASE WHEN @logic_std3_lh = 1 AND a._sac_fl > 0 THEN 1 END) as sacs_std3_lh,
            @groundballs := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std3_lh ,
            @linedrives := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std3_lh ,
            @runnerbasesadv := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std3_lh ,
            @selfbasesadv := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std3_lh ,
            @totbasesadv := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std3_lh,

            -- -- -- --

            NULL, -- for createddate
            NULL, -- for lastmodifieddate
            NULL -- for autoincrement PK

            -- -- -- -- 

            FROM
            analbase_atbat as a
            JOIN pfx_game as g ON g.gid = a.gid
            left join
            (SELECT distinct id_, concat(first_,' ',last_) as pname FROM pfx_player
            WHERE player_record_id IN (SELECT max(player_record_id) FROM pfx_player group by id_)
            ) as p
            ON p.id_ = a.batter

            WHERE g.game_type = 'R'
            AND YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) 
            AND a.game_date <= @analdate

            GROUP BY
            @analdate,
            a.batter

            HAVING COUNT(DISTINCT CASE WHEN (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN concat(a.gid,a.ab_num) END) > 0

            ORDER BY totbasesadv_std desc
            ;

        """
        curA.fetchwarnings()
        curA.execute(query3)
        cnx.commit()

    
    
    
    
    
    
    
        affectedrows += curA.rowcount

        print('   ',activedate, str(datetime.datetime.now() - starttime))

    curA.close()
    cnx.close()
    print('    mySQL DB connection closed')
    print('table updated: anal_batter_counting')
    
    return affectedrows


# In[ ]:

def anal_batter_rate():
    
    print('update starting: anal_batter_rate')
    
    dates, cnx, curA = get_missingdates("anal_batter_rate")
    
    affectedrows = 0  

    for date in dates:
    
        starttime = datetime.datetime.now()

        activedate = date[0]

        query2 = "SET @analdate := '"+str(activedate)+"';"
        curA.execute(query2)
        
        
        query3 = ("""
        INSERT INTO anal_batter_rate 
        SELECT DISTINCT

        anal_game_date,
        batter,
        pname,

        ##### std ######

        @outsmade_pa := outsmade_std/pa_std as outsmade_pa_std,
        @hits_pa := hits_std/pa_std as hits_pa_std,
        @avg_ := hits_std/(pa_std - walks_std - hbp_std - sacs_std) as avg_std,
        @obp := onbases_std/pa_std as obp_std,
        @sba_pa := selfbasesadv_std/pa_std as sba_pa_std,
        @sba_o := selfbasesadv_std/outsmade_std as sba_o_std,
        @rba_pa := runnerbasesadv_std/pa_std as rba_pa_std,
        @rba_o := runnerbasesadv_std/outsmade_std as rba_o_std,
        @tba_pa := totbasesadv_std/pa_std as tba_pa_std,
        @tba_o := totbasesadv_std/outsmade_std as tba_o_std,
        @k_pa := k_std/pa_std as k_pa_std,
        @klook_pa := klook_std/pa_std as klook_pa_std,
        @kswing_pa := kswing_std/pa_std as kswing_pa_std,
        @walk_pa := walks_std/pa_std as walk_pa_std,
        @singles_pa := singles_std/pa_std as singles_pa_std,
        @doubles_pa := doubles_std/pa_std as doubles_pa_std,
        @triples_pa := triples_std/pa_std as triples_pa_std,
        @homeruns_pa := homeruns_std/pa_std as homeruns_pa_std,
        @sacs_pa := sacs_std/pa_std as sacs_pa_std,
        @groundballs_pa := groundballs_std/pa_std as groundballs_pa_std,
        @linedrives_pa := linedrives_std/pa_std as linedrives_pa_std,

        ##### last60 ######

        @outsmade_pa := outsmade_last60/pa_last60 as outsmade_pa_last60,
        @hits_pa := hits_last60/pa_last60 as hits_pa_last60,
        @avg_ := hits_last60/(pa_last60 - walks_last60 - hbp_last60 - sacs_last60) as avg_last60,
        @obp := onbases_last60/pa_last60 as obp_last60,
        @sba_pa := selfbasesadv_last60/pa_last60 as sba_pa_last60,
        @sba_o := selfbasesadv_last60/outsmade_last60 as sba_o_last60,
        @rba_pa := runnerbasesadv_last60/pa_last60 as rba_pa_last60,
        @rba_o := runnerbasesadv_last60/outsmade_last60 as rba_o_last60,
        @tba_pa := totbasesadv_last60/pa_last60 as tba_pa_last60,
        @tba_o := totbasesadv_last60/outsmade_last60 as tba_o_last60,
        @k_pa := k_last60/pa_last60 as k_pa_last60,
        @klook_pa := klook_last60/pa_last60 as klook_pa_last60,
        @kswing_pa := kswing_last60/pa_last60 as kswing_pa_last60,
        @walk_pa := walks_last60/pa_last60 as walk_pa_last60,
        @singles_pa := singles_last60/pa_last60 as singles_pa_last60,
        @doubles_pa := doubles_last60/pa_last60 as doubles_pa_last60,
        @triples_pa := triples_last60/pa_last60 as triples_pa_last60,
        @homeruns_pa := homeruns_last60/pa_last60 as homeruns_pa_last60,
        @sacs_pa := sacs_last60/pa_last60 as sacs_pa_last60,
        @groundballs_pa := groundballs_last60/pa_last60 as groundballs_pa_last60,
        @linedrives_pa := linedrives_last60/pa_last60 as linedrives_pa_last60,

        ##### last30 ######

        @outsmade_pa := outsmade_last30/pa_last30 as outsmade_pa_last30,
        @hits_pa := hits_last30/pa_last30 as hits_pa_last30,
        @avg_ := hits_last30/(pa_last30 - walks_last30 - hbp_last30 - sacs_last30) as avg_last30,
        @obp := onbases_last30/pa_last30 as obp_last30,
        @sba_pa := selfbasesadv_last30/pa_last30 as sba_pa_last30,
        @sba_o := selfbasesadv_last30/outsmade_last30 as sba_o_last30,
        @rba_pa := runnerbasesadv_last30/pa_last30 as rba_pa_last30,
        @rba_o := runnerbasesadv_last30/outsmade_last30 as rba_o_last30,
        @tba_pa := totbasesadv_last30/pa_last30 as tba_pa_last30,
        @tba_o := totbasesadv_last30/outsmade_last30 as tba_o_last30,
        @k_pa := k_last30/pa_last30 as k_pa_last30,
        @klook_pa := klook_last30/pa_last30 as klook_pa_last30,
        @kswing_pa := kswing_last30/pa_last30 as kswing_pa_last30,
        @walk_pa := walks_last30/pa_last30 as walk_pa_last30,
        @singles_pa := singles_last30/pa_last30 as singles_pa_last30,
        @doubles_pa := doubles_last30/pa_last30 as doubles_pa_last30,
        @triples_pa := triples_last30/pa_last30 as triples_pa_last30,
        @homeruns_pa := homeruns_last30/pa_last30 as homeruns_pa_last30,
        @sacs_pa := sacs_last30/pa_last30 as sacs_pa_last30,
        @groundballs_pa := groundballs_last30/pa_last30 as groundballs_pa_last30,
        @linedrives_pa := linedrives_last30/pa_last30 as linedrives_pa_last30,

        ##### last10 ######

        @outsmade_pa := outsmade_last10/pa_last10 as outsmade_pa_last10,
        @hits_pa := hits_last10/pa_last10 as hits_pa_last10,
        @avg_ := hits_last10/(pa_last10 - walks_last10 - hbp_last10 - sacs_last10) as avg_last10,
        @obp := onbases_last10/pa_last10 as obp_last10,
        @sba_pa := selfbasesadv_last10/pa_last10 as sba_pa_last10,
        @sba_o := selfbasesadv_last10/outsmade_last10 as sba_o_last10,
        @rba_pa := runnerbasesadv_last10/pa_last10 as rba_pa_last10,
        @rba_o := runnerbasesadv_last10/outsmade_last10 as rba_o_last10,
        @tba_pa := totbasesadv_last10/pa_last10 as tba_pa_last10,
        @tba_o := totbasesadv_last10/outsmade_last10 as tba_o_last10,
        @k_pa := k_last10/pa_last10 as k_pa_last10,
        @klook_pa := klook_last10/pa_last10 as klook_pa_last10,
        @kswing_pa := kswing_last10/pa_last10 as kswing_pa_last10,
        @walk_pa := walks_last10/pa_last10 as walk_pa_last10,
        @singles_pa := singles_last10/pa_last10 as singles_pa_last10,
        @doubles_pa := doubles_last10/pa_last10 as doubles_pa_last10,
        @triples_pa := triples_last10/pa_last10 as triples_pa_last10,
        @homeruns_pa := homeruns_last10/pa_last10 as homeruns_pa_last10,
        @sacs_pa := sacs_last10/pa_last10 as sacs_pa_last10,
        @groundballs_pa := groundballs_last10/pa_last10 as groundballs_pa_last10,
        @linedrives_pa := linedrives_last10/pa_last10 as linedrives_pa_last10,

        ##### std2 ######

        @outsmade_pa := outsmade_std2/pa_std2 as outsmade_pa_std2,
        @hits_pa := hits_std2/pa_std2 as hits_pa_std2,
        @avg_ := hits_std2/(pa_std2 - walks_std2 - hbp_std2 - sacs_std2) as avg_std2,
        @obp := onbases_std2/pa_std2 as obp_std2,
        @sba_pa := selfbasesadv_std2/pa_std2 as sba_pa_std2,
        @sba_o := selfbasesadv_std2/outsmade_std2 as sba_o_std2,
        @rba_pa := runnerbasesadv_std2/pa_std2 as rba_pa_std2,
        @rba_o := runnerbasesadv_std2/outsmade_std2 as rba_o_std2,
        @tba_pa := totbasesadv_std2/pa_std2 as tba_pa_std2,
        @tba_o := totbasesadv_std2/outsmade_std2 as tba_o_std2,
        @k_pa := k_std2/pa_std2 as k_pa_std2,
        @klook_pa := klook_std2/pa_std2 as klook_pa_std2,
        @kswing_pa := kswing_std2/pa_std2 as kswing_pa_std2,
        @walk_pa := walks_std2/pa_std2 as walk_pa_std2,
        @singles_pa := singles_std2/pa_std2 as singles_pa_std2,
        @doubles_pa := doubles_std2/pa_std2 as doubles_pa_std2,
        @triples_pa := triples_std2/pa_std2 as triples_pa_std2,
        @homeruns_pa := homeruns_std2/pa_std2 as homeruns_pa_std2,
        @sacs_pa := sacs_std2/pa_std2 as sacs_pa_std2,
        @groundballs_pa := groundballs_std2/pa_std2 as groundballs_pa_std2,
        @linedrives_pa := linedrives_std2/pa_std2 as linedrives_pa_std2,

        ##### std3 ######

        @outsmade_pa := outsmade_std3/pa_std3 as outsmade_pa_std3,
        @hits_pa := hits_std3/pa_std3 as hits_pa_std3,
        @avg_ := hits_std3/(pa_std3 - walks_std3 - hbp_std3 - sacs_std3) as avg_std3,
        @obp := onbases_std3/pa_std3 as obp_std3,
        @sba_pa := selfbasesadv_std3/pa_std3 as sba_pa_std3,
        @sba_o := selfbasesadv_std3/outsmade_std3 as sba_o_std3,
        @rba_pa := runnerbasesadv_std3/pa_std3 as rba_pa_std3,
        @rba_o := runnerbasesadv_std3/outsmade_std3 as rba_o_std3,
        @tba_pa := totbasesadv_std3/pa_std3 as tba_pa_std3,
        @tba_o := totbasesadv_std3/outsmade_std3 as tba_o_std3,
        @k_pa := k_std3/pa_std3 as k_pa_std3,
        @klook_pa := klook_std3/pa_std3 as klook_pa_std3,
        @kswing_pa := kswing_std3/pa_std3 as kswing_pa_std3,
        @walk_pa := walks_std3/pa_std3 as walk_pa_std3,
        @singles_pa := singles_std3/pa_std3 as singles_pa_std3,
        @doubles_pa := doubles_std3/pa_std3 as doubles_pa_std3,
        @triples_pa := triples_std3/pa_std3 as triples_pa_std3,
        @homeruns_pa := homeruns_std3/pa_std3 as homeruns_pa_std3,
        @sacs_pa := sacs_std3/pa_std3 as sacs_pa_std3,
        @groundballs_pa := groundballs_std3/pa_std3 as groundballs_pa_std3,
        @linedrives_pa := linedrives_std3/pa_std3 as linedrives_pa_std3,

        ##### std_rh ######

        @outsmade_pa := outsmade_std_rh/pa_std_rh as outsmade_pa_std_rh,
        @hits_pa := hits_std_rh/pa_std_rh as hits_pa_std_rh,
        @avg_ := hits_std_rh/(pa_std_rh - walks_std_rh - hbp_std_rh - sacs_std_rh) as avg_std_rh,
        @obp := onbases_std_rh/pa_std_rh as obp_std_rh,
        @sba_pa := selfbasesadv_std_rh/pa_std_rh as sba_pa_std_rh,
        @sba_o := selfbasesadv_std_rh/outsmade_std_rh as sba_o_std_rh,
        @rba_pa := runnerbasesadv_std_rh/pa_std_rh as rba_pa_std_rh,
        @rba_o := runnerbasesadv_std_rh/outsmade_std_rh as rba_o_std_rh,
        @tba_pa := totbasesadv_std_rh/pa_std_rh as tba_pa_std_rh,
        @tba_o := totbasesadv_std_rh/outsmade_std_rh as tba_o_std_rh,
        @k_pa := k_std_rh/pa_std_rh as k_pa_std_rh,
        @klook_pa := klook_std_rh/pa_std_rh as klook_pa_std_rh,
        @kswing_pa := kswing_std_rh/pa_std_rh as kswing_pa_std_rh,
        @walk_pa := walks_std_rh/pa_std_rh as walk_pa_std_rh,
        @singles_pa := singles_std_rh/pa_std_rh as singles_pa_std_rh,
        @doubles_pa := doubles_std_rh/pa_std_rh as doubles_pa_std_rh,
        @triples_pa := triples_std_rh/pa_std_rh as triples_pa_std_rh,
        @homeruns_pa := homeruns_std_rh/pa_std_rh as homeruns_pa_std_rh,
        @sacs_pa := sacs_std_rh/pa_std_rh as sacs_pa_std_rh,
        @groundballs_pa := groundballs_std_rh/pa_std_rh as groundballs_pa_std_rh,
        @linedrives_pa := linedrives_std_rh/pa_std_rh as linedrives_pa_std_rh,

        ##### last60_rh ######

        @outsmade_pa := outsmade_last60_rh/pa_last60_rh as outsmade_pa_last60_rh,
        @hits_pa := hits_last60_rh/pa_last60_rh as hits_pa_last60_rh,
        @avg_ := hits_last60_rh/(pa_last60_rh - walks_last60_rh - hbp_last60_rh - sacs_last60_rh) as avg_last60_rh,
        @obp := onbases_last60_rh/pa_last60_rh as obp_last60_rh,
        @sba_pa := selfbasesadv_last60_rh/pa_last60_rh as sba_pa_last60_rh,
        @sba_o := selfbasesadv_last60_rh/outsmade_last60_rh as sba_o_last60_rh,
        @rba_pa := runnerbasesadv_last60_rh/pa_last60_rh as rba_pa_last60_rh,
        @rba_o := runnerbasesadv_last60_rh/outsmade_last60_rh as rba_o_last60_rh,
        @tba_pa := totbasesadv_last60_rh/pa_last60_rh as tba_pa_last60_rh,
        @tba_o := totbasesadv_last60_rh/outsmade_last60_rh as tba_o_last60_rh,
        @k_pa := k_last60_rh/pa_last60_rh as k_pa_last60_rh,
        @klook_pa := klook_last60_rh/pa_last60_rh as klook_pa_last60_rh,
        @kswing_pa := kswing_last60_rh/pa_last60_rh as kswing_pa_last60_rh,
        @walk_pa := walks_last60_rh/pa_last60_rh as walk_pa_last60_rh,
        @singles_pa := singles_last60_rh/pa_last60_rh as singles_pa_last60_rh,
        @doubles_pa := doubles_last60_rh/pa_last60_rh as doubles_pa_last60_rh,
        @triples_pa := triples_last60_rh/pa_last60_rh as triples_pa_last60_rh,
        @homeruns_pa := homeruns_last60_rh/pa_last60_rh as homeruns_pa_last60_rh,
        @sacs_pa := sacs_last60_rh/pa_last60_rh as sacs_pa_last60_rh,
        @groundballs_pa := groundballs_last60_rh/pa_last60_rh as groundballs_pa_last60_rh,
        @linedrives_pa := linedrives_last60_rh/pa_last60_rh as linedrives_pa_last60_rh,

        ##### last30_rh ######

        @outsmade_pa := outsmade_last30_rh/pa_last30_rh as outsmade_pa_last30_rh,
        @hits_pa := hits_last30_rh/pa_last30_rh as hits_pa_last30_rh,
        @avg_ := hits_last30_rh/(pa_last30_rh - walks_last30_rh - hbp_last30_rh - sacs_last30_rh) as avg_last30_rh,
        @obp := onbases_last30_rh/pa_last30_rh as obp_last30_rh,
        @sba_pa := selfbasesadv_last30_rh/pa_last30_rh as sba_pa_last30_rh,
        @sba_o := selfbasesadv_last30_rh/outsmade_last30_rh as sba_o_last30_rh,
        @rba_pa := runnerbasesadv_last30_rh/pa_last30_rh as rba_pa_last30_rh,
        @rba_o := runnerbasesadv_last30_rh/outsmade_last30_rh as rba_o_last30_rh,
        @tba_pa := totbasesadv_last30_rh/pa_last30_rh as tba_pa_last30_rh,
        @tba_o := totbasesadv_last30_rh/outsmade_last30_rh as tba_o_last30_rh,
        @k_pa := k_last30_rh/pa_last30_rh as k_pa_last30_rh,
        @klook_pa := klook_last30_rh/pa_last30_rh as klook_pa_last30_rh,
        @kswing_pa := kswing_last30_rh/pa_last30_rh as kswing_pa_last30_rh,
        @walk_pa := walks_last30_rh/pa_last30_rh as walk_pa_last30_rh,
        @singles_pa := singles_last30_rh/pa_last30_rh as singles_pa_last30_rh,
        @doubles_pa := doubles_last30_rh/pa_last30_rh as doubles_pa_last30_rh,
        @triples_pa := triples_last30_rh/pa_last30_rh as triples_pa_last30_rh,
        @homeruns_pa := homeruns_last30_rh/pa_last30_rh as homeruns_pa_last30_rh,
        @sacs_pa := sacs_last30_rh/pa_last30_rh as sacs_pa_last30_rh,
        @groundballs_pa := groundballs_last30_rh/pa_last30_rh as groundballs_pa_last30_rh,
        @linedrives_pa := linedrives_last30_rh/pa_last30_rh as linedrives_pa_last30_rh,

        ##### last10_rh ######

        @outsmade_pa := outsmade_last10_rh/pa_last10_rh as outsmade_pa_last10_rh,
        @hits_pa := hits_last10_rh/pa_last10_rh as hits_pa_last10_rh,
        @avg_ := hits_last10_rh/(pa_last10_rh - walks_last10_rh - hbp_last10_rh - sacs_last10_rh) as avg_last10_rh,
        @obp := onbases_last10_rh/pa_last10_rh as obp_last10_rh,
        @sba_pa := selfbasesadv_last10_rh/pa_last10_rh as sba_pa_last10_rh,
        @sba_o := selfbasesadv_last10_rh/outsmade_last10_rh as sba_o_last10_rh,
        @rba_pa := runnerbasesadv_last10_rh/pa_last10_rh as rba_pa_last10_rh,
        @rba_o := runnerbasesadv_last10_rh/outsmade_last10_rh as rba_o_last10_rh,
        @tba_pa := totbasesadv_last10_rh/pa_last10_rh as tba_pa_last10_rh,
        @tba_o := totbasesadv_last10_rh/outsmade_last10_rh as tba_o_last10_rh,
        @k_pa := k_last10_rh/pa_last10_rh as k_pa_last10_rh,
        @klook_pa := klook_last10_rh/pa_last10_rh as klook_pa_last10_rh,
        @kswing_pa := kswing_last10_rh/pa_last10_rh as kswing_pa_last10_rh,
        @walk_pa := walks_last10_rh/pa_last10_rh as walk_pa_last10_rh,
        @singles_pa := singles_last10_rh/pa_last10_rh as singles_pa_last10_rh,
        @doubles_pa := doubles_last10_rh/pa_last10_rh as doubles_pa_last10_rh,
        @triples_pa := triples_last10_rh/pa_last10_rh as triples_pa_last10_rh,
        @homeruns_pa := homeruns_last10_rh/pa_last10_rh as homeruns_pa_last10_rh,
        @sacs_pa := sacs_last10_rh/pa_last10_rh as sacs_pa_last10_rh,
        @groundballs_pa := groundballs_last10_rh/pa_last10_rh as groundballs_pa_last10_rh,
        @linedrives_pa := linedrives_last10_rh/pa_last10_rh as linedrives_pa_last10_rh,

        ##### std2_rh ######

        @outsmade_pa := outsmade_std2_rh/pa_std2_rh as outsmade_pa_std2_rh,
        @hits_pa := hits_std2_rh/pa_std2_rh as hits_pa_std2_rh,
        @avg_ := hits_std2_rh/(pa_std2_rh - walks_std2_rh - hbp_std2_rh - sacs_std2_rh) as avg_std2_rh,
        @obp := onbases_std2_rh/pa_std2_rh as obp_std2_rh,
        @sba_pa := selfbasesadv_std2_rh/pa_std2_rh as sba_pa_std2_rh,
        @sba_o := selfbasesadv_std2_rh/outsmade_std2_rh as sba_o_std2_rh,
        @rba_pa := runnerbasesadv_std2_rh/pa_std2_rh as rba_pa_std2_rh,
        @rba_o := runnerbasesadv_std2_rh/outsmade_std2_rh as rba_o_std2_rh,
        @tba_pa := totbasesadv_std2_rh/pa_std2_rh as tba_pa_std2_rh,
        @tba_o := totbasesadv_std2_rh/outsmade_std2_rh as tba_o_std2_rh,
        @k_pa := k_std2_rh/pa_std2_rh as k_pa_std2_rh,
        @klook_pa := klook_std2_rh/pa_std2_rh as klook_pa_std2_rh,
        @kswing_pa := kswing_std2_rh/pa_std2_rh as kswing_pa_std2_rh,
        @walk_pa := walks_std2_rh/pa_std2_rh as walk_pa_std2_rh,
        @singles_pa := singles_std2_rh/pa_std2_rh as singles_pa_std2_rh,
        @doubles_pa := doubles_std2_rh/pa_std2_rh as doubles_pa_std2_rh,
        @triples_pa := triples_std2_rh/pa_std2_rh as triples_pa_std2_rh,
        @homeruns_pa := homeruns_std2_rh/pa_std2_rh as homeruns_pa_std2_rh,
        @sacs_pa := sacs_std2_rh/pa_std2_rh as sacs_pa_std2_rh,
        @groundballs_pa := groundballs_std2_rh/pa_std2_rh as groundballs_pa_std2_rh,
        @linedrives_pa := linedrives_std2_rh/pa_std2_rh as linedrives_pa_std2_rh,

        ##### std3_rh ######

        @outsmade_pa := outsmade_std3_rh/pa_std3_rh as outsmade_pa_std3_rh,
        @hits_pa := hits_std3_rh/pa_std3_rh as hits_pa_std3_rh,
        @avg_ := hits_std3_rh/(pa_std3_rh - walks_std3_rh - hbp_std3_rh - sacs_std3_rh) as avg_std3_rh,
        @obp := onbases_std3_rh/pa_std3_rh as obp_std3_rh,
        @sba_pa := selfbasesadv_std3_rh/pa_std3_rh as sba_pa_std3_rh,
        @sba_o := selfbasesadv_std3_rh/outsmade_std3_rh as sba_o_std3_rh,
        @rba_pa := runnerbasesadv_std3_rh/pa_std3_rh as rba_pa_std3_rh,
        @rba_o := runnerbasesadv_std3_rh/outsmade_std3_rh as rba_o_std3_rh,
        @tba_pa := totbasesadv_std3_rh/pa_std3_rh as tba_pa_std3_rh,
        @tba_o := totbasesadv_std3_rh/outsmade_std3_rh as tba_o_std3_rh,
        @k_pa := k_std3_rh/pa_std3_rh as k_pa_std3_rh,
        @klook_pa := klook_std3_rh/pa_std3_rh as klook_pa_std3_rh,
        @kswing_pa := kswing_std3_rh/pa_std3_rh as kswing_pa_std3_rh,
        @walk_pa := walks_std3_rh/pa_std3_rh as walk_pa_std3_rh,
        @singles_pa := singles_std3_rh/pa_std3_rh as singles_pa_std3_rh,
        @doubles_pa := doubles_std3_rh/pa_std3_rh as doubles_pa_std3_rh,
        @triples_pa := triples_std3_rh/pa_std3_rh as triples_pa_std3_rh,
        @homeruns_pa := homeruns_std3_rh/pa_std3_rh as homeruns_pa_std3_rh,
        @sacs_pa := sacs_std3_rh/pa_std3_rh as sacs_pa_std3_rh,
        @groundballs_pa := groundballs_std3_rh/pa_std3_rh as groundballs_pa_std3_rh,
        @linedrives_pa := linedrives_std3_rh/pa_std3_rh as linedrives_pa_std3_rh,

        ##### std_lh ######

        @outsmade_pa := outsmade_std_lh/pa_std_lh as outsmade_pa_std_lh,
        @hits_pa := hits_std_lh/pa_std_lh as hits_pa_std_lh,
        @avg_ := hits_std_lh/(pa_std_lh - walks_std_lh - hbp_std_lh - sacs_std_lh) as avg_std_lh,
        @obp := onbases_std_lh/pa_std_lh as obp_std_lh,
        @sba_pa := selfbasesadv_std_lh/pa_std_lh as sba_pa_std_lh,
        @sba_o := selfbasesadv_std_lh/outsmade_std_lh as sba_o_std_lh,
        @rba_pa := runnerbasesadv_std_lh/pa_std_lh as rba_pa_std_lh,
        @rba_o := runnerbasesadv_std_lh/outsmade_std_lh as rba_o_std_lh,
        @tba_pa := totbasesadv_std_lh/pa_std_lh as tba_pa_std_lh,
        @tba_o := totbasesadv_std_lh/outsmade_std_lh as tba_o_std_lh,
        @k_pa := k_std_lh/pa_std_lh as k_pa_std_lh,
        @klook_pa := klook_std_lh/pa_std_lh as klook_pa_std_lh,
        @kswing_pa := kswing_std_lh/pa_std_lh as kswing_pa_std_lh,
        @walk_pa := walks_std_lh/pa_std_lh as walk_pa_std_lh,
        @singles_pa := singles_std_lh/pa_std_lh as singles_pa_std_lh,
        @doubles_pa := doubles_std_lh/pa_std_lh as doubles_pa_std_lh,
        @triples_pa := triples_std_lh/pa_std_lh as triples_pa_std_lh,
        @homeruns_pa := homeruns_std_lh/pa_std_lh as homeruns_pa_std_lh,
        @sacs_pa := sacs_std_lh/pa_std_lh as sacs_pa_std_lh,
        @groundballs_pa := groundballs_std_lh/pa_std_lh as groundballs_pa_std_lh,
        @linedrives_pa := linedrives_std_lh/pa_std_lh as linedrives_pa_std_lh,

        ##### last60_lh ######

        @outsmade_pa := outsmade_last60_lh/pa_last60_lh as outsmade_pa_last60_lh,
        @hits_pa := hits_last60_lh/pa_last60_lh as hits_pa_last60_lh,
        @avg_ := hits_last60_lh/(pa_last60_lh - walks_last60_lh - hbp_last60_lh - sacs_last60_lh) as avg_last60_lh,
        @obp := onbases_last60_lh/pa_last60_lh as obp_last60_lh,
        @sba_pa := selfbasesadv_last60_lh/pa_last60_lh as sba_pa_last60_lh,
        @sba_o := selfbasesadv_last60_lh/outsmade_last60_lh as sba_o_last60_lh,
        @rba_pa := runnerbasesadv_last60_lh/pa_last60_lh as rba_pa_last60_lh,
        @rba_o := runnerbasesadv_last60_lh/outsmade_last60_lh as rba_o_last60_lh,
        @tba_pa := totbasesadv_last60_lh/pa_last60_lh as tba_pa_last60_lh,
        @tba_o := totbasesadv_last60_lh/outsmade_last60_lh as tba_o_last60_lh,
        @k_pa := k_last60_lh/pa_last60_lh as k_pa_last60_lh,
        @klook_pa := klook_last60_lh/pa_last60_lh as klook_pa_last60_lh,
        @kswing_pa := kswing_last60_lh/pa_last60_lh as kswing_pa_last60_lh,
        @walk_pa := walks_last60_lh/pa_last60_lh as walk_pa_last60_lh,
        @singles_pa := singles_last60_lh/pa_last60_lh as singles_pa_last60_lh,
        @doubles_pa := doubles_last60_lh/pa_last60_lh as doubles_pa_last60_lh,
        @triples_pa := triples_last60_lh/pa_last60_lh as triples_pa_last60_lh,
        @homeruns_pa := homeruns_last60_lh/pa_last60_lh as homeruns_pa_last60_lh,
        @sacs_pa := sacs_last60_lh/pa_last60_lh as sacs_pa_last60_lh,
        @groundballs_pa := groundballs_last60_lh/pa_last60_lh as groundballs_pa_last60_lh,
        @linedrives_pa := linedrives_last60_lh/pa_last60_lh as linedrives_pa_last60_lh,

        ##### last30_lh ######

        @outsmade_pa := outsmade_last30_lh/pa_last30_lh as outsmade_pa_last30_lh,
        @hits_pa := hits_last30_lh/pa_last30_lh as hits_pa_last30_lh,
        @avg_ := hits_last30_lh/(pa_last30_lh - walks_last30_lh - hbp_last30_lh - sacs_last30_lh) as avg_last30_lh,
        @obp := onbases_last30_lh/pa_last30_lh as obp_last30_lh,
        @sba_pa := selfbasesadv_last30_lh/pa_last30_lh as sba_pa_last30_lh,
        @sba_o := selfbasesadv_last30_lh/outsmade_last30_lh as sba_o_last30_lh,
        @rba_pa := runnerbasesadv_last30_lh/pa_last30_lh as rba_pa_last30_lh,
        @rba_o := runnerbasesadv_last30_lh/outsmade_last30_lh as rba_o_last30_lh,
        @tba_pa := totbasesadv_last30_lh/pa_last30_lh as tba_pa_last30_lh,
        @tba_o := totbasesadv_last30_lh/outsmade_last30_lh as tba_o_last30_lh,
        @k_pa := k_last30_lh/pa_last30_lh as k_pa_last30_lh,
        @klook_pa := klook_last30_lh/pa_last30_lh as klook_pa_last30_lh,
        @kswing_pa := kswing_last30_lh/pa_last30_lh as kswing_pa_last30_lh,
        @walk_pa := walks_last30_lh/pa_last30_lh as walk_pa_last30_lh,
        @singles_pa := singles_last30_lh/pa_last30_lh as singles_pa_last30_lh,
        @doubles_pa := doubles_last30_lh/pa_last30_lh as doubles_pa_last30_lh,
        @triples_pa := triples_last30_lh/pa_last30_lh as triples_pa_last30_lh,
        @homeruns_pa := homeruns_last30_lh/pa_last30_lh as homeruns_pa_last30_lh,
        @sacs_pa := sacs_last30_lh/pa_last30_lh as sacs_pa_last30_lh,
        @groundballs_pa := groundballs_last30_lh/pa_last30_lh as groundballs_pa_last30_lh,
        @linedrives_pa := linedrives_last30_lh/pa_last30_lh as linedrives_pa_last30_lh,

        ##### last10_lh ######

        @outsmade_pa := outsmade_last10_lh/pa_last10_lh as outsmade_pa_last10_lh,
        @hits_pa := hits_last10_lh/pa_last10_lh as hits_pa_last10_lh,
        @avg_ := hits_last10_lh/(pa_last10_lh - walks_last10_lh - hbp_last10_lh - sacs_last10_lh) as avg_last10_lh,
        @obp := onbases_last10_lh/pa_last10_lh as obp_last10_lh,
        @sba_pa := selfbasesadv_last10_lh/pa_last10_lh as sba_pa_last10_lh,
        @sba_o := selfbasesadv_last10_lh/outsmade_last10_lh as sba_o_last10_lh,
        @rba_pa := runnerbasesadv_last10_lh/pa_last10_lh as rba_pa_last10_lh,
        @rba_o := runnerbasesadv_last10_lh/outsmade_last10_lh as rba_o_last10_lh,
        @tba_pa := totbasesadv_last10_lh/pa_last10_lh as tba_pa_last10_lh,
        @tba_o := totbasesadv_last10_lh/outsmade_last10_lh as tba_o_last10_lh,
        @k_pa := k_last10_lh/pa_last10_lh as k_pa_last10_lh,
        @klook_pa := klook_last10_lh/pa_last10_lh as klook_pa_last10_lh,
        @kswing_pa := kswing_last10_lh/pa_last10_lh as kswing_pa_last10_lh,
        @walk_pa := walks_last10_lh/pa_last10_lh as walk_pa_last10_lh,
        @singles_pa := singles_last10_lh/pa_last10_lh as singles_pa_last10_lh,
        @doubles_pa := doubles_last10_lh/pa_last10_lh as doubles_pa_last10_lh,
        @triples_pa := triples_last10_lh/pa_last10_lh as triples_pa_last10_lh,
        @homeruns_pa := homeruns_last10_lh/pa_last10_lh as homeruns_pa_last10_lh,
        @sacs_pa := sacs_last10_lh/pa_last10_lh as sacs_pa_last10_lh,
        @groundballs_pa := groundballs_last10_lh/pa_last10_lh as groundballs_pa_last10_lh,
        @linedrives_pa := linedrives_last10_lh/pa_last10_lh as linedrives_pa_last10_lh,

        ##### std2_lh ######

        @outsmade_pa := outsmade_std2_lh/pa_std2_lh as outsmade_pa_std2_lh,
        @hits_pa := hits_std2_lh/pa_std2_lh as hits_pa_std2_lh,
        @avg_ := hits_std2_lh/(pa_std2_lh - walks_std2_lh - hbp_std2_lh - sacs_std2_lh) as avg_std2_lh,
        @obp := onbases_std2_lh/pa_std2_lh as obp_std2_lh,
        @sba_pa := selfbasesadv_std2_lh/pa_std2_lh as sba_pa_std2_lh,
        @sba_o := selfbasesadv_std2_lh/outsmade_std2_lh as sba_o_std2_lh,
        @rba_pa := runnerbasesadv_std2_lh/pa_std2_lh as rba_pa_std2_lh,
        @rba_o := runnerbasesadv_std2_lh/outsmade_std2_lh as rba_o_std2_lh,
        @tba_pa := totbasesadv_std2_lh/pa_std2_lh as tba_pa_std2_lh,
        @tba_o := totbasesadv_std2_lh/outsmade_std2_lh as tba_o_std2_lh,
        @k_pa := k_std2_lh/pa_std2_lh as k_pa_std2_lh,
        @klook_pa := klook_std2_lh/pa_std2_lh as klook_pa_std2_lh,
        @kswing_pa := kswing_std2_lh/pa_std2_lh as kswing_pa_std2_lh,
        @walk_pa := walks_std2_lh/pa_std2_lh as walk_pa_std2_lh,
        @singles_pa := singles_std2_lh/pa_std2_lh as singles_pa_std2_lh,
        @doubles_pa := doubles_std2_lh/pa_std2_lh as doubles_pa_std2_lh,
        @triples_pa := triples_std2_lh/pa_std2_lh as triples_pa_std2_lh,
        @homeruns_pa := homeruns_std2_lh/pa_std2_lh as homeruns_pa_std2_lh,
        @sacs_pa := sacs_std2_lh/pa_std2_lh as sacs_pa_std2_lh,
        @groundballs_pa := groundballs_std2_lh/pa_std2_lh as groundballs_pa_std2_lh,
        @linedrives_pa := linedrives_std2_lh/pa_std2_lh as linedrives_pa_std2_lh,

        ##### std3_lh ######

        @outsmade_pa := outsmade_std3_lh/pa_std3_lh as outsmade_pa_std3_lh,
        @hits_pa := hits_std3_lh/pa_std3_lh as hits_pa_std3_lh,
        @avg_ := hits_std3_lh/(pa_std3_lh - walks_std3_lh - hbp_std3_lh - sacs_std3_lh) as avg_std3_lh,
        @obp := onbases_std3_lh/pa_std3_lh as obp_std3_lh,
        @sba_pa := selfbasesadv_std3_lh/pa_std3_lh as sba_pa_std3_lh,
        @sba_o := selfbasesadv_std3_lh/outsmade_std3_lh as sba_o_std3_lh,
        @rba_pa := runnerbasesadv_std3_lh/pa_std3_lh as rba_pa_std3_lh,
        @rba_o := runnerbasesadv_std3_lh/outsmade_std3_lh as rba_o_std3_lh,
        @tba_pa := totbasesadv_std3_lh/pa_std3_lh as tba_pa_std3_lh,
        @tba_o := totbasesadv_std3_lh/outsmade_std3_lh as tba_o_std3_lh,
        @k_pa := k_std3_lh/pa_std3_lh as k_pa_std3_lh,
        @klook_pa := klook_std3_lh/pa_std3_lh as klook_pa_std3_lh,
        @kswing_pa := kswing_std3_lh/pa_std3_lh as kswing_pa_std3_lh,
        @walk_pa := walks_std3_lh/pa_std3_lh as walk_pa_std3_lh,
        @singles_pa := singles_std3_lh/pa_std3_lh as singles_pa_std3_lh,
        @doubles_pa := doubles_std3_lh/pa_std3_lh as doubles_pa_std3_lh,
        @triples_pa := triples_std3_lh/pa_std3_lh as triples_pa_std3_lh,
        @homeruns_pa := homeruns_std3_lh/pa_std3_lh as homeruns_pa_std3_lh,
        @sacs_pa := sacs_std3_lh/pa_std3_lh as sacs_pa_std3_lh,
        @groundballs_pa := groundballs_std3_lh/pa_std3_lh as groundballs_pa_std3_lh,
        @linedrives_pa := linedrives_std3_lh/pa_std3_lh as linedrives_pa_std3_lh,
        
        NULL,
        NULL,
        NULL
    

        FROM anal_batter_counting
        WHERE anal_game_date = @analdate + interval '1' day

        """)   

        curA.execute(query3)
        cnx.commit()

    
        affectedrows += curA.rowcount

        print('   ',activedate, str(datetime.datetime.now() - starttime))

    curA.close()
    cnx.close()
    print('    mySQL DB connection closed')
    print('table updated: anal_batter_rate')
    
    return affectedrows


# In[ ]:

def anal_pitcher_counting_ab():
    
    print('update starting: anal_pitcher_counting_ab')
    
    dates, cnx, curA = get_missingdates("anal_pitcher_counting_ab")
    
    affectedrows = 0    
    
    for date in dates:

        starttime = datetime.datetime.now()

        activedate = date[0]

        query2 = "SET @analdate := '"+str(activedate)+"';"
        curA.execute(query2)

        query3 = """
            INSERT INTO anal_pitcher_counting_ab
            SELECT DISTINCT
            @analdate + interval '1' day as anal_game_date,
            a.pitcher,
            player.pname,

            ###########  STD  ############

            @pa_out := COUNT(CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std,
            @outsmade := SUM(CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.outsmade ELSE 0 END) as outsmade_std,
            @hits := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std,
            @onbases := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std,
            @singles := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std,
            @doubles := COUNT(CASE WHEN @logic_std = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std,
            @triples := COUNT(CASE WHEN @logic_std = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std,
            @homeruns := COUNT(CASE WHEN @logic_std = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std,
            @walks := COUNT(CASE WHEN @logic_std = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std,
            @k := COUNT(CASE WHEN @logic_std = 1 AND a._k_fl > 0 THEN 1 END) as k_std,
            @klook := COUNT(CASE WHEN @logic_std = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std,
            @kswing := COUNT(CASE WHEN @logic_std = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std,
            @hbp := COUNT(CASE WHEN @logic_std = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std,
            @groundballs := COUNT(CASE WHEN @logic_std = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std,
            @linedrives := COUNT(CASE WHEN @logic_std = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std,
            @totbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std,
            @runsscored := SUM(CASE WHEN @logic_std = 1 THEN a.runsscored ELSE 0 END) as runsscored_std,

            ###########  std2  ############

            @pa_out := COUNT(CASE WHEN @logic_std2 := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate) = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std2,
            @outsmade := SUM(CASE WHEN @logic_std2 := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate) = 1 THEN a.outsmade ELSE 0 END) as outsmade_std2,
            @hits := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std2,
            @onbases := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std2,
            @singles := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std2,
            @doubles := COUNT(CASE WHEN @logic_std2 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std2,
            @triples := COUNT(CASE WHEN @logic_std2 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std2,
            @homeruns := COUNT(CASE WHEN @logic_std2 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std2,
            @walks := COUNT(CASE WHEN @logic_std2 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std2,
            @k := COUNT(CASE WHEN @logic_std2 = 1 AND a._k_fl > 0 THEN 1 END) as k_std2,
            @klook := COUNT(CASE WHEN @logic_std2 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std2,
            @kswing := COUNT(CASE WHEN @logic_std2 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std2,
            @hbp := COUNT(CASE WHEN @logic_std2 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std2,
            @groundballs := COUNT(CASE WHEN @logic_std2 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std2,
            @linedrives := COUNT(CASE WHEN @logic_std2 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std2,
            @totbasesadv := SUM(CASE WHEN @logic_std2 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std2,
            @runsscored := SUM(CASE WHEN @logic_std2 = 1 THEN a.runsscored ELSE 0 END) as runsscored_std2,

            ###########  std3  ############

            @pa_out := COUNT(CASE WHEN @logic_std3 := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std3,
            @outsmade := SUM(CASE WHEN @logic_std3 := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN a.outsmade ELSE 0 END) as outsmade_std3,
            @hits := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std3,
            @onbases := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std3,
            @singles := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std3,
            @doubles := COUNT(CASE WHEN @logic_std3 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std3,
            @triples := COUNT(CASE WHEN @logic_std3 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std3,
            @homeruns := COUNT(CASE WHEN @logic_std3 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std3,
            @walks := COUNT(CASE WHEN @logic_std3 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std3,
            @k := COUNT(CASE WHEN @logic_std3 = 1 AND a._k_fl > 0 THEN 1 END) as k_std3,
            @klook := COUNT(CASE WHEN @logic_std3 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std3,
            @kswing := COUNT(CASE WHEN @logic_std3 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std3,
            @hbp := COUNT(CASE WHEN @logic_std3 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std3,
            @groundballs := COUNT(CASE WHEN @logic_std3 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std3,
            @linedrives := COUNT(CASE WHEN @logic_std3 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std3,
            @totbasesadv := SUM(CASE WHEN @logic_std3 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std3,
            @runsscored := SUM(CASE WHEN @logic_std3 = 1 THEN a.runsscored ELSE 0 END) as runsscored_std3,

            ###########  last60  ############

            @pa_out := COUNT(CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60,
            @outsmade := SUM(CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60,
            @hits := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60,
            @onbases := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60,
            @singles := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last60,
            @doubles := COUNT(CASE WHEN @logic_last60 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last60,
            @triples := COUNT(CASE WHEN @logic_last60 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last60,
            @homeruns := COUNT(CASE WHEN @logic_last60 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last60,
            @walks := COUNT(CASE WHEN @logic_last60 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last60,
            @k := COUNT(CASE WHEN @logic_last60 = 1 AND a._k_fl > 0 THEN 1 END) as k_last60,
            @klook := COUNT(CASE WHEN @logic_last60 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last60,
            @kswing := COUNT(CASE WHEN @logic_last60 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last60,
            @hbp := COUNT(CASE WHEN @logic_last60 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last60,
            @groundballs := COUNT(CASE WHEN @logic_last60 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last60,
            @linedrives := COUNT(CASE WHEN @logic_last60 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last60,
            @totbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60,
            @runsscored := SUM(CASE WHEN @logic_last60 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60,

            ###########  last20  ############

            @pa_out := COUNT(CASE WHEN @logic_last20 := (a.game_date BETWEEN @analdate - interval '20' day AND @analdate) = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last20,
            @outsmade := SUM(CASE WHEN @logic_last20 := (a.game_date BETWEEN @analdate - interval '20' day AND @analdate) = 1 THEN a.outsmade ELSE 0 END) as outsmade_last20,
            @hits := COUNT(CASE WHEN @logic_last20 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last20,
            @onbases := COUNT(CASE WHEN @logic_last20 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last20,
            @singles := COUNT(CASE WHEN @logic_last20 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last20,
            @doubles := COUNT(CASE WHEN @logic_last20 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last20,
            @triples := COUNT(CASE WHEN @logic_last20 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last20,
            @homeruns := COUNT(CASE WHEN @logic_last20 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last20,
            @walks := COUNT(CASE WHEN @logic_last20 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last20,
            @k := COUNT(CASE WHEN @logic_last20 = 1 AND a._k_fl > 0 THEN 1 END) as k_last20,
            @klook := COUNT(CASE WHEN @logic_last20 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last20,
            @kswing := COUNT(CASE WHEN @logic_last20 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last20,
            @hbp := COUNT(CASE WHEN @logic_last20 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last20,
            @groundballs := COUNT(CASE WHEN @logic_last20 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last20,
            @linedrives := COUNT(CASE WHEN @logic_last20 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last20,
            @totbasesadv := SUM(CASE WHEN @logic_last20 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last20,
            @runsscored := SUM(CASE WHEN @logic_last20 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last20,

            NULL, -- for createddate
            NULL, -- for lastmodifieddate
            NULL -- for autoincrement PK

            FROM
            analbase_atbat as a
            JOIN pfx_game as g
            ON g.gid = a.gid
            LEFT JOIN
                (SELECT distinct id_, concat(first_,' ',last_) as pname FROM pfx_player
                WHERE player_record_id IN (SELECT max(player_record_id) FROM pfx_player group by id_)
                ) as player
            ON player.id_ = a.pitcher

            WHERE g.game_type = 'R'
            AND YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) 
            AND a.game_date <= @analdate

            GROUP BY
            @analdate,
            a.pitcher

            HAVING COUNT(DISTINCT CASE WHEN (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN concat(a.gid,a.ab_num) END) > 0

            ORDER BY k_std desc;
        """

        curA.execute(query3)
        cnx.commit()

    
    
    
    
    
        affectedrows += curA.rowcount

        print('   ',activedate, str(datetime.datetime.now() - starttime))

    curA.close()
    cnx.close()
    print('    mySQL DB connection closed')
    print('table updated: anal_pitcher_counting_ab')
    
    return affectedrows


# In[ ]:

def anal_pitcher_counting_p():
    
    print('update starting: anal_pitcher_counting_p')
    
    dates, cnx, curA = get_missingdates("anal_pitcher_counting_p")
    
    affectedrows = 0
    
    for date in dates:

        starttime = datetime.datetime.now()

        activedate = date[0]

        query2 = "SET @analdate := '"+str(activedate)+"';"
        curA.execute(query2)

        query3 = """

            INSERT INTO anal_pitcher_counting_p
            SELECT
            DISTINCT
            @analdate + interval '1' day as anal_game_date,
            a.pitcher,
            null as player,

            ###########  STD  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as pa_std,
            @pitches := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,p.pitch_id) END) as p_std,

            @ball := SUM(CASE WHEN @logic_std = 1 THEN p.ball_fl ELSE 0 END) as ball_std,
            @strike := SUM(CASE WHEN @logic_std = 1 THEN p.strike_fl ELSE 0 END) as strike_std,
            @calledstrike := SUM(CASE WHEN @logic_std = 1 THEN p.calledstrike_fl ELSE 0 END) as calledstrike_std,
            @whiffstrike := SUM(CASE WHEN @logic_std = 1 THEN p.swingmissstrike_fl ELSE 0 END) as whiffstrike_std,
            @foulstrike := SUM(CASE WHEN @logic_std = 1 THEN p.foulstrike_fl ELSE 0 END) as foulstrike_std,
            @inplay := SUM(CASE WHEN @logic_std = 1 THEN p.inplay_fl ELSE 0 END) as inplay_std,
            @swing := SUM(CASE WHEN @logic_std = 1 THEN p.swing_fl ELSE 0 END) as swing_std,
            @take := SUM(CASE WHEN @logic_std = 1 THEN p.take_fl ELSE 0 END) as take_std,
            @firstpstrike := SUM(CASE WHEN @logic_std = 1 THEN p.firstpstrike_fl ELSE 0 END) as firstpstrike_std,
            @firstpnotinplay := SUM(CASE WHEN @logic_std = 1 THEN p.firstpnotinplay_fl ELSE 0 END) as firstpnotinplay_fl_std,
            @secondpstrike := SUM(CASE WHEN @logic_std = 1 THEN p.secondpstrike_fl ELSE 0 END) as secondpstrike_std,

            @zoneedge_in2 := SUM(CASE WHEN @logic_std = 1 THEN p.zoneedge_in2 ELSE 0 END) as zoneedge_in2_std,
            @zoneedge_out2 := SUM(CASE WHEN @logic_std = 1 THEN p.zoneedge_out2 ELSE 0 END) as zoneedge_out2_std,
            @zoneedge_in4 := SUM(CASE WHEN @logic_std = 1 THEN p.zoneedge_in4 ELSE 0 END) as zoneedge_in4_std,
            @zoneedge_out4 := SUM(CASE WHEN @logic_std = 1 THEN p.zoneedge_out4 ELSE 0 END) as zoneedge_out4_std,
            @zonecorn_in2 := SUM(CASE WHEN @logic_std = 1 THEN p.zonecorn_in2 ELSE 0 END) as zonecorn_in2_std,
            @zonecorn_out2 := SUM(CASE WHEN @logic_std = 1 THEN p.zonecorn_out2 ELSE 0 END) as zonecorn_out2_std,
            @zonecorn_in4 := SUM(CASE WHEN @logic_std = 1 THEN p.zonecorn_in4 ELSE 0 END) as zonecorn_in4_std,
            @zonecorn_out4 := SUM(CASE WHEN @logic_std = 1 THEN p.zonecorn_out4 ELSE 0 END) as zonecorn_out4_std,
            @zone_mid3 := SUM(CASE WHEN @logic_std = 1 THEN p.zone_mid3 ELSE 0 END) as zone_mid3_std,
            @zone_mid6 := SUM(CASE WHEN @logic_std = 1 THEN p.zone_mid6 ELSE 0 END) as zone_mid6_std,
            @zone_bigmiss4 := SUM(CASE WHEN @logic_std = 1 THEN p.zone_bigmiss4 ELSE 0 END) as zone_bigmiss4_std,

            @fastball := COUNT(CASE WHEN @logic_std = 1 THEN p.fastball_endspeed END) as fastball_std,
            @fastball_endspeed := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_endspeed END) as fastball_endspeed_std,
            @fastball_spinrate := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_spinrate END) as fastball_spinrate_std,
            @fastball_spindir := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_spindir END) as fastball_spindir_std,
            @fastball_pfx_x := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_pfx_x END) as fastball_pfx_x_std,
            @fastball_pfx_z := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_pfx_z END) as fastball_pfx_z_std,
            @fastball_mnorm := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_mnorm END) as fastball_mnorm_std,
            @fastball_adjmnorm := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_adjmnorm END) as fastball_adjmnorm_std,

            @curveball := COUNT(CASE WHEN @logic_std = 1 THEN p.curveball_endspeed END) as curveball_std,
            @curveball_endspeed := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_endspeed END) as curveball_endspeed_std,
            @curveball_spinrate := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_spinrate END) as curveball_spinrate_std,
            @curveball_spindir := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_spindir END) as curveball_spindir_std,
            @curveball_pfx_x := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_pfx_x END) as curveball_pfx_x_std,
            @curveball_pfx_z := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_pfx_z END) as curveball_pfx_z_std,
            @curveball_mnorm := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_mnorm END) as curveball_mnorm_std,
            @curveball_adjmnorm := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_adjmnorm END) as curveball_adjmnorm_std,

            @slider := COUNT(CASE WHEN @logic_std = 1 THEN p.slider_endspeed END) as slider_std,
            @slider_endspeed := SUM(CASE WHEN @logic_std = 1 THEN p.slider_endspeed END) as slider_endspeed_std,
            @slider_spinrate := SUM(CASE WHEN @logic_std = 1 THEN p.slider_spinrate END) as slider_spinrate_std,
            @slider_spindir := SUM(CASE WHEN @logic_std = 1 THEN p.slider_spindir END) as slider_spindir_std,
            @slider_pfx_x := SUM(CASE WHEN @logic_std = 1 THEN p.slider_pfx_x END) as slider_pfx_x_std,
            @slider_pfx_z := SUM(CASE WHEN @logic_std = 1 THEN p.slider_pfx_z END) as slider_pfx_z_std,
            @slider_mnorm := SUM(CASE WHEN @logic_std = 1 THEN p.slider_mnorm END) as slider_mnorm_std,
            @slider_adjmnorm := SUM(CASE WHEN @logic_std = 1 THEN p.slider_adjmnorm END) as slider_adjmnorm_std,

            @changeup := COUNT(CASE WHEN @logic_std = 1 THEN p.changeup_endspeed END) as changeup_std,
            @changeup_endspeed := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_endspeed END) as changeup_endspeed_std,
            @changeup_spinrate := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_spinrate END) as changeup_spinrate_std,
            @changeup_spindir := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_spindir END) as changeup_spindir_std,
            @changeup_pfx_x := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_pfx_x END) as changeup_pfx_x_std,
            @changeup_pfx_z := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_pfx_z END) as changeup_pfx_z_std,
            @changeup_mnorm := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_mnorm END) as changeup_mnorm_std,
            @changeup_adjmnorm := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_adjmnorm END) as changeup_adjmnorm_std,

            ###########  std2  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std2 := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std2,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as pa_std2,
            @pitches := COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,p.pitch_id) END) as p_std2,

            @ball := SUM(CASE WHEN @logic_std2 = 1 THEN p.ball_fl ELSE 0 END) as ball_std2,
            @strike := SUM(CASE WHEN @logic_std2 = 1 THEN p.strike_fl ELSE 0 END) as strike_std2,
            @calledstrike := SUM(CASE WHEN @logic_std2 = 1 THEN p.calledstrike_fl ELSE 0 END) as calledstrike_std2,
            @whiffstrike := SUM(CASE WHEN @logic_std2 = 1 THEN p.swingmissstrike_fl ELSE 0 END) as whiffstrike_std2,
            @foulstrike := SUM(CASE WHEN @logic_std2 = 1 THEN p.foulstrike_fl ELSE 0 END) as foulstrike_std2,
            @inplay := SUM(CASE WHEN @logic_std2 = 1 THEN p.inplay_fl ELSE 0 END) as inplay_std2,
            @swing := SUM(CASE WHEN @logic_std2 = 1 THEN p.swing_fl ELSE 0 END) as swing_std2,
            @take := SUM(CASE WHEN @logic_std2 = 1 THEN p.take_fl ELSE 0 END) as take_std2,
            @firstpstrike := SUM(CASE WHEN @logic_std2 = 1 THEN p.firstpstrike_fl ELSE 0 END) as firstpstrike_std2,
            @firstpnotinplay := SUM(CASE WHEN @logic_std2 = 1 THEN p.firstpnotinplay_fl ELSE 0 END) as firstpnotinplay_fl_std2,
            @secondpstrike := SUM(CASE WHEN @logic_std2 = 1 THEN p.secondpstrike_fl ELSE 0 END) as secondpstrike_std2,

            @zoneedge_in2 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zoneedge_in2 ELSE 0 END) as zoneedge_in2_std2,
            @zoneedge_out2 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zoneedge_out2 ELSE 0 END) as zoneedge_out2_std2,
            @zoneedge_in4 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zoneedge_in4 ELSE 0 END) as zoneedge_in4_std2,
            @zoneedge_out4 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zoneedge_out4 ELSE 0 END) as zoneedge_out4_std2,
            @zonecorn_in2 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zonecorn_in2 ELSE 0 END) as zonecorn_in2_std2,
            @zonecorn_out2 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zonecorn_out2 ELSE 0 END) as zonecorn_out2_std2,
            @zonecorn_in4 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zonecorn_in4 ELSE 0 END) as zonecorn_in4_std2,
            @zonecorn_out4 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zonecorn_out4 ELSE 0 END) as zonecorn_out4_std2,
            @zone_mid3 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zone_mid3 ELSE 0 END) as zone_mid3_std2,
            @zone_mid6 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zone_mid6 ELSE 0 END) as zone_mid6_std2,
            @zone_bigmiss4 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zone_bigmiss4 ELSE 0 END) as zone_bigmiss4_std2,

            @fastball := COUNT(CASE WHEN @logic_std2 = 1 THEN p.fastball_endspeed END) as fastball_std2,
            @fastball_endspeed := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_endspeed END) as fastball_endspeed_std2,
            @fastball_spinrate := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_spinrate END) as fastball_spinrate_std2,
            @fastball_spindir := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_spindir END) as fastball_spindir_std2,
            @fastball_pfx_x := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_pfx_x END) as fastball_pfx_x_std2,
            @fastball_pfx_z := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_pfx_z END) as fastball_pfx_z_std2,
            @fastball_mnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_mnorm END) as fastball_mnorm_std2,
            @fastball_adjmnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_adjmnorm END) as fastball_adjmnorm_std2,

            @curveball := COUNT(CASE WHEN @logic_std2 = 1 THEN p.curveball_endspeed END) as curveball_std2,
            @curveball_endspeed := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_endspeed END) as curveball_endspeed_std2,
            @curveball_spinrate := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_spinrate END) as curveball_spinrate_std2,
            @curveball_spindir := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_spindir END) as curveball_spindir_std2,
            @curveball_pfx_x := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_pfx_x END) as curveball_pfx_x_std2,
            @curveball_pfx_z := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_pfx_z END) as curveball_pfx_z_std2,
            @curveball_mnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_mnorm END) as curveball_mnorm_std2,
            @curveball_adjmnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_adjmnorm END) as curveball_adjmnorm_std2,

            @slider := COUNT(CASE WHEN @logic_std2 = 1 THEN p.slider_endspeed END) as slider_std2,
            @slider_endspeed := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_endspeed END) as slider_endspeed_std2,
            @slider_spinrate := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_spinrate END) as slider_spinrate_std2,
            @slider_spindir := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_spindir END) as slider_spindir_std2,
            @slider_pfx_x := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_pfx_x END) as slider_pfx_x_std2,
            @slider_pfx_z := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_pfx_z END) as slider_pfx_z_std2,
            @slider_mnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_mnorm END) as slider_mnorm_std2,
            @slider_adjmnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_adjmnorm END) as slider_adjmnorm_std2,

            @changeup := COUNT(CASE WHEN @logic_std2 = 1 THEN p.changeup_endspeed END) as changeup_std2,
            @changeup_endspeed := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_endspeed END) as changeup_endspeed_std2,
            @changeup_spinrate := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_spinrate END) as changeup_spinrate_std2,
            @changeup_spindir := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_spindir END) as changeup_spindir_std2,
            @changeup_pfx_x := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_pfx_x END) as changeup_pfx_x_std2,
            @changeup_pfx_z := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_pfx_z END) as changeup_pfx_z_std2,
            @changeup_mnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_mnorm END) as changeup_mnorm_std2,
            @changeup_adjmnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_adjmnorm END) as changeup_adjmnorm_std2,

            ###########  std3  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std3 := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std3,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as pa_std3,
            @pitches := COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,p.pitch_id) END) as p_std3,

            @ball := SUM(CASE WHEN @logic_std3 = 1 THEN p.ball_fl ELSE 0 END) as ball_std3,
            @strike := SUM(CASE WHEN @logic_std3 = 1 THEN p.strike_fl ELSE 0 END) as strike_std3,
            @calledstrike := SUM(CASE WHEN @logic_std3 = 1 THEN p.calledstrike_fl ELSE 0 END) as calledstrike_std3,
            @whiffstrike := SUM(CASE WHEN @logic_std3 = 1 THEN p.swingmissstrike_fl ELSE 0 END) as whiffstrike_std3,
            @foulstrike := SUM(CASE WHEN @logic_std3 = 1 THEN p.foulstrike_fl ELSE 0 END) as foulstrike_std3,
            @inplay := SUM(CASE WHEN @logic_std3 = 1 THEN p.inplay_fl ELSE 0 END) as inplay_std3,
            @swing := SUM(CASE WHEN @logic_std3 = 1 THEN p.swing_fl ELSE 0 END) as swing_std3,
            @take := SUM(CASE WHEN @logic_std3 = 1 THEN p.take_fl ELSE 0 END) as take_std3,
            @firstpstrike := SUM(CASE WHEN @logic_std3 = 1 THEN p.firstpstrike_fl ELSE 0 END) as firstpstrike_std3,
            @firstpnotinplay := SUM(CASE WHEN @logic_std3 = 1 THEN p.firstpnotinplay_fl ELSE 0 END) as firstpnotinplay_fl_std3,
            @secondpstrike := SUM(CASE WHEN @logic_std3 = 1 THEN p.secondpstrike_fl ELSE 0 END) as secondpstrike_std3,

            @zoneedge_in2 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zoneedge_in2 ELSE 0 END) as zoneedge_in2_std3,
            @zoneedge_out2 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zoneedge_out2 ELSE 0 END) as zoneedge_out2_std3,
            @zoneedge_in4 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zoneedge_in4 ELSE 0 END) as zoneedge_in4_std3,
            @zoneedge_out4 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zoneedge_out4 ELSE 0 END) as zoneedge_out4_std3,
            @zonecorn_in2 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zonecorn_in2 ELSE 0 END) as zonecorn_in2_std3,
            @zonecorn_out2 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zonecorn_out2 ELSE 0 END) as zonecorn_out2_std3,
            @zonecorn_in4 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zonecorn_in4 ELSE 0 END) as zonecorn_in4_std3,
            @zonecorn_out4 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zonecorn_out4 ELSE 0 END) as zonecorn_out4_std3,
            @zone_mid3 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zone_mid3 ELSE 0 END) as zone_mid3_std3,
            @zone_mid6 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zone_mid6 ELSE 0 END) as zone_mid6_std3,
            @zone_bigmiss4 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zone_bigmiss4 ELSE 0 END) as zone_bigmiss4_std3,

            @fastball := COUNT(CASE WHEN @logic_std3 = 1 THEN p.fastball_endspeed END) as fastball_std3,
            @fastball_endspeed := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_endspeed END) as fastball_endspeed_std3,
            @fastball_spinrate := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_spinrate END) as fastball_spinrate_std3,
            @fastball_spindir := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_spindir END) as fastball_spindir_std3,
            @fastball_pfx_x := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_pfx_x END) as fastball_pfx_x_std3,
            @fastball_pfx_z := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_pfx_z END) as fastball_pfx_z_std3,
            @fastball_mnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_mnorm END) as fastball_mnorm_std3,
            @fastball_adjmnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_adjmnorm END) as fastball_adjmnorm_std3,

            @curveball := COUNT(CASE WHEN @logic_std3 = 1 THEN p.curveball_endspeed END) as curveball_std3,
            @curveball_endspeed := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_endspeed END) as curveball_endspeed_std3,
            @curveball_spinrate := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_spinrate END) as curveball_spinrate_std3,
            @curveball_spindir := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_spindir END) as curveball_spindir_std3,
            @curveball_pfx_x := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_pfx_x END) as curveball_pfx_x_std3,
            @curveball_pfx_z := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_pfx_z END) as curveball_pfx_z_std3,
            @curveball_mnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_mnorm END) as curveball_mnorm_std3,
            @curveball_adjmnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_adjmnorm END) as curveball_adjmnorm_std3,

            @slider := COUNT(CASE WHEN @logic_std3 = 1 THEN p.slider_endspeed END) as slider_std3,
            @slider_endspeed := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_endspeed END) as slider_endspeed_std3,
            @slider_spinrate := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_spinrate END) as slider_spinrate_std3,
            @slider_spindir := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_spindir END) as slider_spindir_std3,
            @slider_pfx_x := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_pfx_x END) as slider_pfx_x_std3,
            @slider_pfx_z := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_pfx_z END) as slider_pfx_z_std3,
            @slider_mnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_mnorm END) as slider_mnorm_std3,
            @slider_adjmnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_adjmnorm END) as slider_adjmnorm_std3,

            @changeup := COUNT(CASE WHEN @logic_std3 = 1 THEN p.changeup_endspeed END) as changeup_std3,
            @changeup_endspeed := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_endspeed END) as changeup_endspeed_std3,
            @changeup_spinrate := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_spinrate END) as changeup_spinrate_std3,
            @changeup_spindir := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_spindir END) as changeup_spindir_std3,
            @changeup_pfx_x := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_pfx_x END) as changeup_pfx_x_std3,
            @changeup_pfx_z := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_pfx_z END) as changeup_pfx_z_std3,
            @changeup_mnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_mnorm END) as changeup_mnorm_std3,
            @changeup_adjmnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_adjmnorm END) as changeup_adjmnorm_std3,

            ###########  last60  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 THEN a.gid END) as gp_last60,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60,
            @pitches := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,p.pitch_id) END) as p_last60,

            @ball := SUM(CASE WHEN @logic_last60 = 1 THEN p.ball_fl ELSE 0 END) as ball_last60,
            @strike := SUM(CASE WHEN @logic_last60 = 1 THEN p.strike_fl ELSE 0 END) as strike_last60,
            @calledstrike := SUM(CASE WHEN @logic_last60 = 1 THEN p.calledstrike_fl ELSE 0 END) as calledstrike_last60,
            @whiffstrike := SUM(CASE WHEN @logic_last60 = 1 THEN p.swingmissstrike_fl ELSE 0 END) as whiffstrike_last60,
            @foulstrike := SUM(CASE WHEN @logic_last60 = 1 THEN p.foulstrike_fl ELSE 0 END) as foulstrike_last60,
            @inplay := SUM(CASE WHEN @logic_last60 = 1 THEN p.inplay_fl ELSE 0 END) as inplay_last60,
            @swing := SUM(CASE WHEN @logic_last60 = 1 THEN p.swing_fl ELSE 0 END) as swing_last60,
            @take := SUM(CASE WHEN @logic_last60 = 1 THEN p.take_fl ELSE 0 END) as take_last60,
            @firstpstrike := SUM(CASE WHEN @logic_last60 = 1 THEN p.firstpstrike_fl ELSE 0 END) as firstpstrike_last60,
            @firstpnotinplay := SUM(CASE WHEN @logic_last60 = 1 THEN p.firstpnotinplay_fl ELSE 0 END) as firstpnotinplay_fl_last60,
            @secondpstrike := SUM(CASE WHEN @logic_last60 = 1 THEN p.secondpstrike_fl ELSE 0 END) as secondpstrike_last60,

            @zoneedge_in2 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zoneedge_in2 ELSE 0 END) as zoneedge_in2_last60,
            @zoneedge_out2 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zoneedge_out2 ELSE 0 END) as zoneedge_out2_last60,
            @zoneedge_in4 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zoneedge_in4 ELSE 0 END) as zoneedge_in4_last60,
            @zoneedge_out4 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zoneedge_out4 ELSE 0 END) as zoneedge_out4_last60,
            @zonecorn_in2 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zonecorn_in2 ELSE 0 END) as zonecorn_in2_last60,
            @zonecorn_out2 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zonecorn_out2 ELSE 0 END) as zonecorn_out2_last60,
            @zonecorn_in4 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zonecorn_in4 ELSE 0 END) as zonecorn_in4_last60,
            @zonecorn_out4 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zonecorn_out4 ELSE 0 END) as zonecorn_out4_last60,
            @zone_mid3 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zone_mid3 ELSE 0 END) as zone_mid3_last60,
            @zone_mid6 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zone_mid6 ELSE 0 END) as zone_mid6_last60,
            @zone_bigmiss4 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zone_bigmiss4 ELSE 0 END) as zone_bigmiss4_last60,

            @fastball := COUNT(CASE WHEN @logic_last60 = 1 THEN p.fastball_endspeed END) as fastball_last60,
            @fastball_endspeed := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_endspeed END) as fastball_endspeed_last60,
            @fastball_spinrate := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_spinrate END) as fastball_spinrate_last60,
            @fastball_spindir := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_spindir END) as fastball_spindir_last60,
            @fastball_pfx_x := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_pfx_x END) as fastball_pfx_x_last60,
            @fastball_pfx_z := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_pfx_z END) as fastball_pfx_z_last60,
            @fastball_mnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_mnorm END) as fastball_mnorm_last60,
            @fastball_adjmnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_adjmnorm END) as fastball_adjmnorm_last60,

            @curveball := COUNT(CASE WHEN @logic_last60 = 1 THEN p.curveball_endspeed END) as curveball_last60,
            @curveball_endspeed := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_endspeed END) as curveball_endspeed_last60,
            @curveball_spinrate := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_spinrate END) as curveball_spinrate_last60,
            @curveball_spindir := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_spindir END) as curveball_spindir_last60,
            @curveball_pfx_x := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_pfx_x END) as curveball_pfx_x_last60,
            @curveball_pfx_z := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_pfx_z END) as curveball_pfx_z_last60,
            @curveball_mnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_mnorm END) as curveball_mnorm_last60,
            @curveball_adjmnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_adjmnorm END) as curveball_adjmnorm_last60,

            @slider := COUNT(CASE WHEN @logic_last60 = 1 THEN p.slider_endspeed END) as slider_last60,
            @slider_endspeed := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_endspeed END) as slider_endspeed_last60,
            @slider_spinrate := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_spinrate END) as slider_spinrate_last60,
            @slider_spindir := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_spindir END) as slider_spindir_last60,
            @slider_pfx_x := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_pfx_x END) as slider_pfx_x_last60,
            @slider_pfx_z := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_pfx_z END) as slider_pfx_z_last60,
            @slider_mnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_mnorm END) as slider_mnorm_last60,
            @slider_adjmnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_adjmnorm END) as slider_adjmnorm_last60,

            @changeup := COUNT(CASE WHEN @logic_last60 = 1 THEN p.changeup_endspeed END) as changeup_last60,
            @changeup_endspeed := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_endspeed END) as changeup_endspeed_last60,
            @changeup_spinrate := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_spinrate END) as changeup_spinrate_last60,
            @changeup_spindir := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_spindir END) as changeup_spindir_last60,
            @changeup_pfx_x := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_pfx_x END) as changeup_pfx_x_last60,
            @changeup_pfx_z := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_pfx_z END) as changeup_pfx_z_last60,
            @changeup_mnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_mnorm END) as changeup_mnorm_last60,
            @changeup_adjmnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_adjmnorm END) as changeup_adjmnorm_last60,


            ###########  last20  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last20 := (a.game_date BETWEEN @analdate - interval '20' day AND @analdate) = 1 THEN a.gid END) as gp_last20,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last20 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last20,
            @pitches := COUNT(DISTINCT CASE WHEN @logic_last20 = 1 THEN concat(a.gid,p.pitch_id) END) as p_last20,

            @ball := SUM(CASE WHEN @logic_last20 = 1 THEN p.ball_fl ELSE 0 END) as ball_last20,
            @strike := SUM(CASE WHEN @logic_last20 = 1 THEN p.strike_fl ELSE 0 END) as strike_last20,
            @calledstrike := SUM(CASE WHEN @logic_last20 = 1 THEN p.calledstrike_fl ELSE 0 END) as calledstrike_last20,
            @whiffstrike := SUM(CASE WHEN @logic_last20 = 1 THEN p.swingmissstrike_fl ELSE 0 END) as whiffstrike_last20,
            @foulstrike := SUM(CASE WHEN @logic_last20 = 1 THEN p.foulstrike_fl ELSE 0 END) as foulstrike_last20,
            @inplay := SUM(CASE WHEN @logic_last20 = 1 THEN p.inplay_fl ELSE 0 END) as inplay_last20,
            @swing := SUM(CASE WHEN @logic_last20 = 1 THEN p.swing_fl ELSE 0 END) as swing_last20,
            @take := SUM(CASE WHEN @logic_last20 = 1 THEN p.take_fl ELSE 0 END) as take_last20,
            @firstpstrike := SUM(CASE WHEN @logic_last20 = 1 THEN p.firstpstrike_fl ELSE 0 END) as firstpstrike_last20,
            @firstpnotinplay := SUM(CASE WHEN @logic_last20 = 1 THEN p.firstpnotinplay_fl ELSE 0 END) as firstpnotinplay_fl_last20,
            @secondpstrike := SUM(CASE WHEN @logic_last20 = 1 THEN p.secondpstrike_fl ELSE 0 END) as secondpstrike_last20,

            @zoneedge_in2 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zoneedge_in2 ELSE 0 END) as zoneedge_in2_last20,
            @zoneedge_out2 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zoneedge_out2 ELSE 0 END) as zoneedge_out2_last20,
            @zoneedge_in4 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zoneedge_in4 ELSE 0 END) as zoneedge_in4_last20,
            @zoneedge_out4 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zoneedge_out4 ELSE 0 END) as zoneedge_out4_last20,
            @zonecorn_in2 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zonecorn_in2 ELSE 0 END) as zonecorn_in2_last20,
            @zonecorn_out2 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zonecorn_out2 ELSE 0 END) as zonecorn_out2_last20,
            @zonecorn_in4 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zonecorn_in4 ELSE 0 END) as zonecorn_in4_last20,
            @zonecorn_out4 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zonecorn_out4 ELSE 0 END) as zonecorn_out4_last20,
            @zone_mid3 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zone_mid3 ELSE 0 END) as zone_mid3_last20,
            @zone_mid6 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zone_mid6 ELSE 0 END) as zone_mid6_last20,
            @zone_bigmiss4 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zone_bigmiss4 ELSE 0 END) as zone_bigmiss4_last20,

            @fastball := COUNT(CASE WHEN @logic_last20 = 1 THEN p.fastball_endspeed END) as fastball_last20,
            @fastball_endspeed := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_endspeed END) as fastball_endspeed_last20,
            @fastball_spinrate := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_spinrate END) as fastball_spinrate_last20,
            @fastball_spindir := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_spindir END) as fastball_spindir_last20,
            @fastball_pfx_x := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_pfx_x END) as fastball_pfx_x_last20,
            @fastball_pfx_z := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_pfx_z END) as fastball_pfx_z_last20,
            @fastball_mnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_mnorm END) as fastball_mnorm_last20,
            @fastball_adjmnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_adjmnorm END) as fastball_adjmnorm_last20,

            @curveball := COUNT(CASE WHEN @logic_last20 = 1 THEN p.curveball_endspeed END) as curveball_last20,
            @curveball_endspeed := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_endspeed END) as curveball_endspeed_last20,
            @curveball_spinrate := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_spinrate END) as curveball_spinrate_last20,
            @curveball_spindir := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_spindir END) as curveball_spindir_last20,
            @curveball_pfx_x := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_pfx_x END) as curveball_pfx_x_last20,
            @curveball_pfx_z := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_pfx_z END) as curveball_pfx_z_last20,
            @curveball_mnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_mnorm END) as curveball_mnorm_last20,
            @curveball_adjmnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_adjmnorm END) as curveball_adjmnorm_last20,

            @slider := COUNT(CASE WHEN @logic_last20 = 1 THEN p.slider_endspeed END) as slider_last20,
            @slider_endspeed := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_endspeed END) as slider_endspeed_last20,
            @slider_spinrate := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_spinrate END) as slider_spinrate_last20,
            @slider_spindir := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_spindir END) as slider_spindir_last20,
            @slider_pfx_x := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_pfx_x END) as slider_pfx_x_last20,
            @slider_pfx_z := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_pfx_z END) as slider_pfx_z_last20,
            @slider_mnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_mnorm END) as slider_mnorm_last20,
            @slider_adjmnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_adjmnorm END) as slider_adjmnorm_last20,

            @changeup := COUNT(CASE WHEN @logic_last20 = 1 THEN p.changeup_endspeed END) as changeup_last20,
            @changeup_endspeed := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_endspeed END) as changeup_endspeed_last20,
            @changeup_spinrate := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_spinrate END) as changeup_spinrate_last20,
            @changeup_spindir := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_spindir END) as changeup_spindir_last20,
            @changeup_pfx_x := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_pfx_x END) as changeup_pfx_x_last20,
            @changeup_pfx_z := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_pfx_z END) as changeup_pfx_z_last20,
            @changeup_mnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_mnorm END) as changeup_mnorm_last20,
            @changeup_adjmnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_adjmnorm END) as changeup_adjmnorm_last20,

            NULL, -- for createddate
            NULL, -- for lastmodifieddate
            NULL -- for autoincrement PK

            FROM
            analbase_pitch as p
            LEFT JOIN
            analbase_atbat as a
            ON a.gid = p.gid AND a.ab_num = p.ab_num
            JOIN pfx_game as g
            ON g.gid = a.gid

            WHERE g.game_type = 'R'
            AND YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) 
            AND a.game_date <= @analdate

            GROUP BY
            @analdate,
            a.pitcher

            HAVING COUNT(DISTINCT CASE WHEN (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN concat(a.gid,a.ab_num) END) > 0
            ;

        """

        curA.execute(query3)
        cnx.commit()

        affectedrows += curA.rowcount

        print('   ',activedate, str(datetime.datetime.now() - starttime))

    curA.close()
    cnx.close()
    print('    mySQL DB connection closed')
    print('table updated: anal_pitcher_counting_p')
    
    return affectedrows


# In[ ]:

def anal_pitcher_rate():
    
    print('update starting: anal_pitcher_rate')
    
    dates, cnx, curA = get_missingdates("anal_pitcher_rate")
    
    affectedrows = 0    
    
    for date in dates:
    
        starttime = datetime.datetime.now()

        activedate = date[0]

        query2 = "SET @analdate := '"+str(activedate)+"';"
        curA.execute(query2)
        
        
        query3 = ("""
            INSERT INTO anal_pitcher_rate

            SELECT DISTINCT

            p.anal_game_date,
            p.pitcher,
            p.pname,

            ##### std ######

            @outsmade_pa := outsmade_std/pa_std as outsmade_pa_std,
            @hits_pa := hits_std/pa_std as hits_pa_std,
            @avg_ := hits_std/(pa_std - walks_std - hbp_std ) as avg_std,
            @obp := onbases_std/pa_std as obp_std,
            @tba_pa := totbasesadv_std/pa_std as tba_pa_std,
            @tba_o := totbasesadv_std/outsmade_std as tba_o_std,
            @k_pa := k_std/pa_std as k_pa_std,
            @klook_pa := klook_std/pa_std as klook_pa_std,
            @kswing_pa := kswing_std/pa_std as kswing_pa_std,
            @walk_pa := walks_std/pa_std as walk_pa_std,
            @singles_pa := singles_std/pa_std as singles_pa_std,
            @doubles_pa := doubles_std/pa_std as doubles_pa_std,
            @triples_pa := triples_std/pa_std as triples_pa_std,
            @homeruns_pa := homeruns_std/pa_std as homeruns_pa_std,
            @groundballs_pa := groundballs_std/pa_std as groundballs_pa_std,
            @linedrives_pa := linedrives_std/pa_std as linedrives_pa_std,

            @ball_p := ball_std/p_std as ball_p_std,
            @ball_pa := ball_std/pa_std as ball_pa_std,
            @strike_p := strike_std/p_std as strike_p_std,
            @strike_pa := strike_std/pa_std as strike_pa_std,

            @calledstrike_strike := calledstrike_std/strike_std as calledstrike_strike_std,
            @whiffstrike_strike := whiffstrike_std/strike_std as whiffstrike_strike_std,
            @foulstrike_strike := foulstrike_std/strike_std as foulstrike_strike_std,
            @inplay_strike := inplay_std/strike_std as inplay_strike_std,
            @calledstrike_pa := calledstrike_std/pa_std as calledstrike_pa_std,
            @whiffstrike_pa := whiffstrike_std/pa_std as whiffstrike_pa_std,
            @foulstrike_pa := foulstrike_std/pa_std as foulstrike_pa_std,
            @inplay_pa := inplay_std/pa_std as inplay_pa_std,

            @swing_p := swing_std/p_std as swing_p_std,
            @take_p := take_std/p_std as take_p_std,
            @swing_pa := swing_std/pa_std as swing_pa_std,
            @take_pa := take_std/pa_std as take_pa_std,

            @firstpstrike_pa := firstpstrike_std/pa_std as firstpstrike_pa_std,
            @secondpstrike_pa := secondpstrike_std/firstpnotinplay_fl_std as secondpstrike_pa_std,

            @zoneedge_in2_p := zoneedge_in2_std/p_std as zoneedge_in2_p_std,
            @zoneedge_in4_p := zoneedge_in4_std/p_std as zoneedge_in4_p_std,
            @zoneedge_out2_p := zoneedge_out2_std/p_std as zoneedge_out2_p_std,
            @zoneedge_out4_p := zoneedge_out4_std/p_std as zoneedge_out4_p_std,
            @zonecorn_in2_p := zonecorn_in2_std/p_std as zonecorn_in2_p_std,
            @zonecorn_in4_p := zonecorn_in4_std/p_std as zonecorn_in4_p_std,
            @zonecorn_out2_p := zonecorn_out2_std/p_std as zonecorn_out2_p_std,
            @zonecorn_out4_p := zonecorn_out4_std/p_std as zonecorn_out4_p_std,
            @zone_mid3_p := zone_mid3_std/p_std as zone_mid3_p_std,
            @zone_mid6_p := zone_mid6_std/p_std as zone_mid6_p_std,
            @zone_bigmiss4_p := zone_bigmiss4_std/p_std as zone_bigmiss4_p_std,

            @fastball_p := fastball_std/p_std as fastball_p_std,
            @fastball_endspeed_p := fastball_endspeed_std/p_std as fastball_endspeed_p_std,
            @fastball_spinrate_p := fastball_spinrate_std/p_std as fastball_spinrate_p_std,
            @fastball_spindir_p := fastball_spindir_std/p_std as fastball_spindir_p_std,
            @fastball_pfx_x_p := fastball_pfx_x_std/p_std as fastball_pfx_x_p_std,
            @fastball_pfx_z_p := fastball_pfx_z_std/p_std as fastball_pfx_z_p_std,
            @fastball_mnorm_p := fastball_mnorm_std/p_std as fastball_mnorm_p_std,
            @fastball_adjmnorm_p := fastball_adjmnorm_std/p_std as fastball_adjmnorm_p_std,

            @curveball_p := curveball_std/p_std as curveball_p_std,
            @curveball_endspeed_p := curveball_endspeed_std/p_std as curveball_endspeed_p_std,
            @curveball_spinrate_p := curveball_spinrate_std/p_std as curveball_spinrate_p_std,
            @curveball_spindir_p := curveball_spindir_std/p_std as curveball_spindir_p_std,
            @curveball_pfx_x_p := curveball_pfx_x_std/p_std as curveball_pfx_x_p_std,
            @curveball_pfx_z_p := curveball_pfx_z_std/p_std as curveball_pfx_z_p_std,
            @curveball_mnorm_p := curveball_mnorm_std/p_std as curveball_mnorm_p_std,
            @curveball_adjmnorm_p := curveball_adjmnorm_std/p_std as curveball_adjmnorm_p_std,

            @slider_p := slider_std/p_std as slider_p_std,
            @slider_endspeed_p := slider_endspeed_std/p_std as slider_endspeed_p_std,
            @slider_spinrate_p := slider_spinrate_std/p_std as slider_spinrate_p_std,
            @slider_spindir_p := slider_spindir_std/p_std as slider_spindir_p_std,
            @slider_pfx_x_p := slider_pfx_x_std/p_std as slider_pfx_x_p_std,
            @slider_pfx_z_p := slider_pfx_z_std/p_std as slider_pfx_z_p_std,
            @slider_mnorm_p := slider_mnorm_std/p_std as slider_mnorm_p_std,
            @slider_adjmnorm_p := slider_adjmnorm_std/p_std as slider_adjmnorm_p_std,

            @changeup_p := changeup_std/p_std as changeup_p_std,
            @changeup_endspeed_p := changeup_endspeed_std/p_std as changeup_endspeed_p_std,
            @changeup_spinrate_p := changeup_spinrate_std/p_std as changeup_spinrate_p_std,
            @changeup_spindir_p := changeup_spindir_std/p_std as changeup_spindir_p_std,
            @changeup_pfx_x_p := changeup_pfx_x_std/p_std as changeup_pfx_x_p_std,
            @changeup_pfx_z_p := changeup_pfx_z_std/p_std as changeup_pfx_z_p_std,
            @changeup_mnorm_p := changeup_mnorm_std/p_std as changeup_mnorm_p_std,
            @changeup_adjmnorm_p := changeup_adjmnorm_std/p_std as changeup_adjmnorm_p_std,

            ##### std2 ######

            @outsmade_pa := outsmade_std2/pa_std2 as outsmade_pa_std2,
            @hits_pa := hits_std2/pa_std2 as hits_pa_std2,
            @avg_ := hits_std2/(pa_std2 - walks_std2 - hbp_std2) as avg_std2,
            @obp := onbases_std2/pa_std2 as obp_std2,
            @tba_pa := totbasesadv_std2/pa_std2 as tba_pa_std2,
            @tba_o := totbasesadv_std2/outsmade_std2 as tba_o_std2,
            @k_pa := k_std2/pa_std2 as k_pa_std2,
            @klook_pa := klook_std2/pa_std2 as klook_pa_std2,
            @kswing_pa := kswing_std2/pa_std2 as kswing_pa_std2,
            @walk_pa := walks_std2/pa_std2 as walk_pa_std2,
            @singles_pa := singles_std2/pa_std2 as singles_pa_std2,
            @doubles_pa := doubles_std2/pa_std2 as doubles_pa_std2,
            @triples_pa := triples_std2/pa_std2 as triples_pa_std2,
            @homeruns_pa := homeruns_std2/pa_std2 as homeruns_pa_std2,
            @groundballs_pa := groundballs_std2/pa_std2 as groundballs_pa_std2,
            @linedrives_pa := linedrives_std2/pa_std2 as linedrives_pa_std2,

            @ball_p := ball_std2/p_std2 as ball_p_std2,
            @ball_pa := ball_std2/pa_std2 as ball_pa_std2,
            @strike_p := strike_std2/p_std2 as strike_p_std2,
            @strike_pa := strike_std2/pa_std2 as strike_pa_std2,

            @calledstrike_strike := calledstrike_std2/strike_std2 as calledstrike_strike_std2,
            @whiffstrike_strike := whiffstrike_std2/strike_std2 as whiffstrike_strike_std2,
            @foulstrike_strike := foulstrike_std2/strike_std2 as foulstrike_strike_std2,
            @inplay_strike := inplay_std2/strike_std2 as inplay_strike_std2,
            @calledstrike_pa := calledstrike_std2/pa_std2 as calledstrike_pa_std2,
            @whiffstrike_pa := whiffstrike_std2/pa_std2 as whiffstrike_pa_std2,
            @foulstrike_pa := foulstrike_std2/pa_std2 as foulstrike_pa_std2,
            @inplay_pa := inplay_std2/pa_std2 as inplay_pa_std2,

            @swing_p := swing_std2/p_std2 as swing_p_std2,
            @take_p := take_std2/p_std2 as take_p_std2,
            @swing_pa := swing_std2/pa_std2 as swing_pa_std2,
            @take_pa := take_std2/pa_std2 as take_pa_std2,

            @firstpstrike_pa := firstpstrike_std2/pa_std2 as firstpstrike_pa_std2,
            @secondpstrike_pa := secondpstrike_std2/firstpnotinplay_fl_std2 as secondpstrike_pa_std2,

            @zoneedge_in2_p := zoneedge_in2_std2/p_std2 as zoneedge_in2_p_std2,
            @zoneedge_in4_p := zoneedge_in4_std2/p_std2 as zoneedge_in4_p_std2,
            @zoneedge_out2_p := zoneedge_out2_std2/p_std2 as zoneedge_out2_p_std2,
            @zoneedge_out4_p := zoneedge_out4_std2/p_std2 as zoneedge_out4_p_std2,
            @zonecorn_in2_p := zonecorn_in2_std2/p_std2 as zonecorn_in2_p_std2,
            @zonecorn_in4_p := zonecorn_in4_std2/p_std2 as zonecorn_in4_p_std2,
            @zonecorn_out2_p := zonecorn_out2_std2/p_std2 as zonecorn_out2_p_std2,
            @zonecorn_out4_p := zonecorn_out4_std2/p_std2 as zonecorn_out4_p_std2,
            @zone_mid3_p := zone_mid3_std2/p_std2 as zone_mid3_p_std2,
            @zone_mid6_p := zone_mid6_std2/p_std2 as zone_mid6_p_std2,
            @zone_bigmiss4_p := zone_bigmiss4_std2/p_std2 as zone_bigmiss4_p_std2,

            @fastball_p := fastball_std2/p_std2 as fastball_p_std2,
            @fastball_endspeed_p := fastball_endspeed_std2/p_std2 as fastball_endspeed_p_std2,
            @fastball_spinrate_p := fastball_spinrate_std2/p_std2 as fastball_spinrate_p_std2,
            @fastball_spindir_p := fastball_spindir_std2/p_std2 as fastball_spindir_p_std2,
            @fastball_pfx_x_p := fastball_pfx_x_std2/p_std2 as fastball_pfx_x_p_std2,
            @fastball_pfx_z_p := fastball_pfx_z_std2/p_std2 as fastball_pfx_z_p_std2,
            @fastball_mnorm_p := fastball_mnorm_std2/p_std2 as fastball_mnorm_p_std2,
            @fastball_adjmnorm_p := fastball_adjmnorm_std2/p_std2 as fastball_adjmnorm_p_std2,

            @curveball_p := curveball_std2/p_std2 as curveball_p_std2,
            @curveball_endspeed_p := curveball_endspeed_std2/p_std2 as curveball_endspeed_p_std2,
            @curveball_spinrate_p := curveball_spinrate_std2/p_std2 as curveball_spinrate_p_std2,
            @curveball_spindir_p := curveball_spindir_std2/p_std2 as curveball_spindir_p_std2,
            @curveball_pfx_x_p := curveball_pfx_x_std2/p_std2 as curveball_pfx_x_p_std2,
            @curveball_pfx_z_p := curveball_pfx_z_std2/p_std2 as curveball_pfx_z_p_std2,
            @curveball_mnorm_p := curveball_mnorm_std2/p_std2 as curveball_mnorm_p_std2,
            @curveball_adjmnorm_p := curveball_adjmnorm_std2/p_std2 as curveball_adjmnorm_p_std2,

            @slider_p := slider_std2/p_std2 as slider_p_std2,
            @slider_endspeed_p := slider_endspeed_std2/p_std2 as slider_endspeed_p_std2,
            @slider_spinrate_p := slider_spinrate_std2/p_std2 as slider_spinrate_p_std2,
            @slider_spindir_p := slider_spindir_std2/p_std2 as slider_spindir_p_std2,
            @slider_pfx_x_p := slider_pfx_x_std2/p_std2 as slider_pfx_x_p_std2,
            @slider_pfx_z_p := slider_pfx_z_std2/p_std2 as slider_pfx_z_p_std2,
            @slider_mnorm_p := slider_mnorm_std2/p_std2 as slider_mnorm_p_std2,
            @slider_adjmnorm_p := slider_adjmnorm_std2/p_std2 as slider_adjmnorm_p_std2,

            @changeup_p := changeup_std2/p_std2 as changeup_p_std2,
            @changeup_endspeed_p := changeup_endspeed_std2/p_std2 as changeup_endspeed_p_std2,
            @changeup_spinrate_p := changeup_spinrate_std2/p_std2 as changeup_spinrate_p_std2,
            @changeup_spindir_p := changeup_spindir_std2/p_std2 as changeup_spindir_p_std2,
            @changeup_pfx_x_p := changeup_pfx_x_std2/p_std2 as changeup_pfx_x_p_std2,
            @changeup_pfx_z_p := changeup_pfx_z_std2/p_std2 as changeup_pfx_z_p_std2,
            @changeup_mnorm_p := changeup_mnorm_std2/p_std2 as changeup_mnorm_p_std2,
            @changeup_adjmnorm_p := changeup_adjmnorm_std2/p_std2 as changeup_adjmnorm_p_std2,

            ##### std3 ######

            @outsmade_pa := outsmade_std3/pa_std3 as outsmade_pa_std3,
            @hits_pa := hits_std3/pa_std3 as hits_pa_std3,
            @avg_ := hits_std3/(pa_std3 - walks_std3 - hbp_std3) as avg_std3,
            @obp := onbases_std3/pa_std3 as obp_std3,
            @tba_pa := totbasesadv_std3/pa_std3 as tba_pa_std3,
            @tba_o := totbasesadv_std3/outsmade_std3 as tba_o_std3,
            @k_pa := k_std3/pa_std3 as k_pa_std3,
            @klook_pa := klook_std3/pa_std3 as klook_pa_std3,
            @kswing_pa := kswing_std3/pa_std3 as kswing_pa_std3,
            @walk_pa := walks_std3/pa_std3 as walk_pa_std3,
            @singles_pa := singles_std3/pa_std3 as singles_pa_std3,
            @doubles_pa := doubles_std3/pa_std3 as doubles_pa_std3,
            @triples_pa := triples_std3/pa_std3 as triples_pa_std3,
            @homeruns_pa := homeruns_std3/pa_std3 as homeruns_pa_std3,
            @groundballs_pa := groundballs_std3/pa_std3 as groundballs_pa_std3,
            @linedrives_pa := linedrives_std3/pa_std3 as linedrives_pa_std3,

            @ball_p := ball_std3/p_std3 as ball_p_std3,
            @ball_pa := ball_std3/pa_std3 as ball_pa_std3,
            @strike_p := strike_std3/p_std3 as strike_p_std3,
            @strike_pa := strike_std3/pa_std3 as strike_pa_std3,

            @calledstrike_strike := calledstrike_std3/strike_std3 as calledstrike_strike_std3,
            @whiffstrike_strike := whiffstrike_std3/strike_std3 as whiffstrike_strike_std3,
            @foulstrike_strike := foulstrike_std3/strike_std3 as foulstrike_strike_std3,
            @inplay_strike := inplay_std3/strike_std3 as inplay_strike_std3,
            @calledstrike_pa := calledstrike_std3/pa_std3 as calledstrike_pa_std3,
            @whiffstrike_pa := whiffstrike_std3/pa_std3 as whiffstrike_pa_std3,
            @foulstrike_pa := foulstrike_std3/pa_std3 as foulstrike_pa_std3,
            @inplay_pa := inplay_std3/pa_std3 as inplay_pa_std3,

            @swing_p := swing_std3/p_std3 as swing_p_std3,
            @take_p := take_std3/p_std3 as take_p_std3,
            @swing_pa := swing_std3/pa_std3 as swing_pa_std3,
            @take_pa := take_std3/pa_std3 as take_pa_std3,

            @firstpstrike_pa := firstpstrike_std3/pa_std3 as firstpstrike_pa_std3,
            @secondpstrike_pa := secondpstrike_std3/firstpnotinplay_fl_std3 as secondpstrike_pa_std3,

            @zoneedge_in2_p := zoneedge_in2_std3/p_std3 as zoneedge_in2_p_std3,
            @zoneedge_in4_p := zoneedge_in4_std3/p_std3 as zoneedge_in4_p_std3,
            @zoneedge_out2_p := zoneedge_out2_std3/p_std3 as zoneedge_out2_p_std3,
            @zoneedge_out4_p := zoneedge_out4_std3/p_std3 as zoneedge_out4_p_std3,
            @zonecorn_in2_p := zonecorn_in2_std3/p_std3 as zonecorn_in2_p_std3,
            @zonecorn_in4_p := zonecorn_in4_std3/p_std3 as zonecorn_in4_p_std3,
            @zonecorn_out2_p := zonecorn_out2_std3/p_std3 as zonecorn_out2_p_std3,
            @zonecorn_out4_p := zonecorn_out4_std3/p_std3 as zonecorn_out4_p_std3,
            @zone_mid3_p := zone_mid3_std3/p_std3 as zone_mid3_p_std3,
            @zone_mid6_p := zone_mid6_std3/p_std3 as zone_mid6_p_std3,
            @zone_bigmiss4_p := zone_bigmiss4_std3/p_std3 as zone_bigmiss4_p_std3,

            @fastball_p := fastball_std3/p_std3 as fastball_p_std3,
            @fastball_endspeed_p := fastball_endspeed_std3/p_std3 as fastball_endspeed_p_std3,
            @fastball_spinrate_p := fastball_spinrate_std3/p_std3 as fastball_spinrate_p_std3,
            @fastball_spindir_p := fastball_spindir_std3/p_std3 as fastball_spindir_p_std3,
            @fastball_pfx_x_p := fastball_pfx_x_std3/p_std3 as fastball_pfx_x_p_std3,
            @fastball_pfx_z_p := fastball_pfx_z_std3/p_std3 as fastball_pfx_z_p_std3,
            @fastball_mnorm_p := fastball_mnorm_std3/p_std3 as fastball_mnorm_p_std3,
            @fastball_adjmnorm_p := fastball_adjmnorm_std3/p_std3 as fastball_adjmnorm_p_std3,

            @curveball_p := curveball_std3/p_std3 as curveball_p_std3,
            @curveball_endspeed_p := curveball_endspeed_std3/p_std3 as curveball_endspeed_p_std3,
            @curveball_spinrate_p := curveball_spinrate_std3/p_std3 as curveball_spinrate_p_std3,
            @curveball_spindir_p := curveball_spindir_std3/p_std3 as curveball_spindir_p_std3,
            @curveball_pfx_x_p := curveball_pfx_x_std3/p_std3 as curveball_pfx_x_p_std3,
            @curveball_pfx_z_p := curveball_pfx_z_std3/p_std3 as curveball_pfx_z_p_std3,
            @curveball_mnorm_p := curveball_mnorm_std3/p_std3 as curveball_mnorm_p_std3,
            @curveball_adjmnorm_p := curveball_adjmnorm_std3/p_std3 as curveball_adjmnorm_p_std3,

            @slider_p := slider_std3/p_std3 as slider_p_std3,
            @slider_endspeed_p := slider_endspeed_std3/p_std3 as slider_endspeed_p_std3,
            @slider_spinrate_p := slider_spinrate_std3/p_std3 as slider_spinrate_p_std3,
            @slider_spindir_p := slider_spindir_std3/p_std3 as slider_spindir_p_std3,
            @slider_pfx_x_p := slider_pfx_x_std3/p_std3 as slider_pfx_x_p_std3,
            @slider_pfx_z_p := slider_pfx_z_std3/p_std3 as slider_pfx_z_p_std3,
            @slider_mnorm_p := slider_mnorm_std3/p_std3 as slider_mnorm_p_std3,
            @slider_adjmnorm_p := slider_adjmnorm_std3/p_std3 as slider_adjmnorm_p_std3,

            @changeup_p := changeup_std3/p_std3 as changeup_p_std3,
            @changeup_endspeed_p := changeup_endspeed_std3/p_std3 as changeup_endspeed_p_std3,
            @changeup_spinrate_p := changeup_spinrate_std3/p_std3 as changeup_spinrate_p_std3,
            @changeup_spindir_p := changeup_spindir_std3/p_std3 as changeup_spindir_p_std3,
            @changeup_pfx_x_p := changeup_pfx_x_std3/p_std3 as changeup_pfx_x_p_std3,
            @changeup_pfx_z_p := changeup_pfx_z_std3/p_std3 as changeup_pfx_z_p_std3,
            @changeup_mnorm_p := changeup_mnorm_std3/p_std3 as changeup_mnorm_p_std3,
            @changeup_adjmnorm_p := changeup_adjmnorm_std3/p_std3 as changeup_adjmnorm_p_std3,

            ##### last60 ######

            @outsmade_pa := outsmade_last60/pa_last60 as outsmade_pa_last60,
            @hits_pa := hits_last60/pa_last60 as hits_pa_last60,
            @avg_ := hits_last60/(pa_last60 - walks_last60 - hbp_last60) as avg_last60,
            @obp := onbases_last60/pa_last60 as obp_last60,
            @tba_pa := totbasesadv_last60/pa_last60 as tba_pa_last60,
            @tba_o := totbasesadv_last60/outsmade_last60 as tba_o_last60,
            @k_pa := k_last60/pa_last60 as k_pa_last60,
            @klook_pa := klook_last60/pa_last60 as klook_pa_last60,
            @kswing_pa := kswing_last60/pa_last60 as kswing_pa_last60,
            @walk_pa := walks_last60/pa_last60 as walk_pa_last60,
            @singles_pa := singles_last60/pa_last60 as singles_pa_last60,
            @doubles_pa := doubles_last60/pa_last60 as doubles_pa_last60,
            @triples_pa := triples_last60/pa_last60 as triples_pa_last60,
            @homeruns_pa := homeruns_last60/pa_last60 as homeruns_pa_last60,
            @groundballs_pa := groundballs_last60/pa_last60 as groundballs_pa_last60,
            @linedrives_pa := linedrives_last60/pa_last60 as linedrives_pa_last60,

            @ball_p := ball_last60/p_last60 as ball_p_last60,
            @ball_pa := ball_last60/pa_last60 as ball_pa_last60,
            @strike_p := strike_last60/p_last60 as strike_p_last60,
            @strike_pa := strike_last60/pa_last60 as strike_pa_last60,

            @calledstrike_strike := calledstrike_last60/strike_last60 as calledstrike_strike_last60,
            @whiffstrike_strike := whiffstrike_last60/strike_last60 as whiffstrike_strike_last60,
            @foulstrike_strike := foulstrike_last60/strike_last60 as foulstrike_strike_last60,
            @inplay_strike := inplay_last60/strike_last60 as inplay_strike_last60,
            @calledstrike_pa := calledstrike_last60/pa_last60 as calledstrike_pa_last60,
            @whiffstrike_pa := whiffstrike_last60/pa_last60 as whiffstrike_pa_last60,
            @foulstrike_pa := foulstrike_last60/pa_last60 as foulstrike_pa_last60,
            @inplay_pa := inplay_last60/pa_last60 as inplay_pa_last60,

            @swing_p := swing_last60/p_last60 as swing_p_last60,
            @take_p := take_last60/p_last60 as take_p_last60,
            @swing_pa := swing_last60/pa_last60 as swing_pa_last60,
            @take_pa := take_last60/pa_last60 as take_pa_last60,

            @firstpstrike_pa := firstpstrike_last60/pa_last60 as firstpstrike_pa_last60,
            @secondpstrike_pa := secondpstrike_last60/firstpnotinplay_fl_last60 as secondpstrike_pa_last60,

            @zoneedge_in2_p := zoneedge_in2_last60/p_last60 as zoneedge_in2_p_last60,
            @zoneedge_in4_p := zoneedge_in4_last60/p_last60 as zoneedge_in4_p_last60,
            @zoneedge_out2_p := zoneedge_out2_last60/p_last60 as zoneedge_out2_p_last60,
            @zoneedge_out4_p := zoneedge_out4_last60/p_last60 as zoneedge_out4_p_last60,
            @zonecorn_in2_p := zonecorn_in2_last60/p_last60 as zonecorn_in2_p_last60,
            @zonecorn_in4_p := zonecorn_in4_last60/p_last60 as zonecorn_in4_p_last60,
            @zonecorn_out2_p := zonecorn_out2_last60/p_last60 as zonecorn_out2_p_last60,
            @zonecorn_out4_p := zonecorn_out4_last60/p_last60 as zonecorn_out4_p_last60,
            @zone_mid3_p := zone_mid3_last60/p_last60 as zone_mid3_p_last60,
            @zone_mid6_p := zone_mid6_last60/p_last60 as zone_mid6_p_last60,
            @zone_bigmiss4_p := zone_bigmiss4_last60/p_last60 as zone_bigmiss4_p_last60,

            @fastball_p := fastball_last60/p_last60 as fastball_p_last60,
            @fastball_endspeed_p := fastball_endspeed_last60/p_last60 as fastball_endspeed_p_last60,
            @fastball_spinrate_p := fastball_spinrate_last60/p_last60 as fastball_spinrate_p_last60,
            @fastball_spindir_p := fastball_spindir_last60/p_last60 as fastball_spindir_p_last60,
            @fastball_pfx_x_p := fastball_pfx_x_last60/p_last60 as fastball_pfx_x_p_last60,
            @fastball_pfx_z_p := fastball_pfx_z_last60/p_last60 as fastball_pfx_z_p_last60,
            @fastball_mnorm_p := fastball_mnorm_last60/p_last60 as fastball_mnorm_p_last60,
            @fastball_adjmnorm_p := fastball_adjmnorm_last60/p_last60 as fastball_adjmnorm_p_last60,

            @curveball_p := curveball_last60/p_last60 as curveball_p_last60,
            @curveball_endspeed_p := curveball_endspeed_last60/p_last60 as curveball_endspeed_p_last60,
            @curveball_spinrate_p := curveball_spinrate_last60/p_last60 as curveball_spinrate_p_last60,
            @curveball_spindir_p := curveball_spindir_last60/p_last60 as curveball_spindir_p_last60,
            @curveball_pfx_x_p := curveball_pfx_x_last60/p_last60 as curveball_pfx_x_p_last60,
            @curveball_pfx_z_p := curveball_pfx_z_last60/p_last60 as curveball_pfx_z_p_last60,
            @curveball_mnorm_p := curveball_mnorm_last60/p_last60 as curveball_mnorm_p_last60,
            @curveball_adjmnorm_p := curveball_adjmnorm_last60/p_last60 as curveball_adjmnorm_p_last60,

            @slider_p := slider_last60/p_last60 as slider_p_last60,
            @slider_endspeed_p := slider_endspeed_last60/p_last60 as slider_endspeed_p_last60,
            @slider_spinrate_p := slider_spinrate_last60/p_last60 as slider_spinrate_p_last60,
            @slider_spindir_p := slider_spindir_last60/p_last60 as slider_spindir_p_last60,
            @slider_pfx_x_p := slider_pfx_x_last60/p_last60 as slider_pfx_x_p_last60,
            @slider_pfx_z_p := slider_pfx_z_last60/p_last60 as slider_pfx_z_p_last60,
            @slider_mnorm_p := slider_mnorm_last60/p_last60 as slider_mnorm_p_last60,
            @slider_adjmnorm_p := slider_adjmnorm_last60/p_last60 as slider_adjmnorm_p_last60,

            @changeup_p := changeup_last60/p_last60 as changeup_p_last60,
            @changeup_endspeed_p := changeup_endspeed_last60/p_last60 as changeup_endspeed_p_last60,
            @changeup_spinrate_p := changeup_spinrate_last60/p_last60 as changeup_spinrate_p_last60,
            @changeup_spindir_p := changeup_spindir_last60/p_last60 as changeup_spindir_p_last60,
            @changeup_pfx_x_p := changeup_pfx_x_last60/p_last60 as changeup_pfx_x_p_last60,
            @changeup_pfx_z_p := changeup_pfx_z_last60/p_last60 as changeup_pfx_z_p_last60,
            @changeup_mnorm_p := changeup_mnorm_last60/p_last60 as changeup_mnorm_p_last60,
            @changeup_adjmnorm_p := changeup_adjmnorm_last60/p_last60 as changeup_adjmnorm_p_last60,

            ##### last20 ######

            @outsmade_pa := outsmade_last20/pa_last20 as outsmade_pa_last20,
            @hits_pa := hits_last20/pa_last20 as hits_pa_last20,
            @avg_ := hits_last20/(pa_last20 - walks_last20 - hbp_last20) as avg_last20,
            @obp := onbases_last20/pa_last20 as obp_last20,
            @tba_pa := totbasesadv_last20/pa_last20 as tba_pa_last20,
            @tba_o := totbasesadv_last20/outsmade_last20 as tba_o_last20,
            @k_pa := k_last20/pa_last20 as k_pa_last20,
            @klook_pa := klook_last20/pa_last20 as klook_pa_last20,
            @kswing_pa := kswing_last20/pa_last20 as kswing_pa_last20,
            @walk_pa := walks_last20/pa_last20 as walk_pa_last20,
            @singles_pa := singles_last20/pa_last20 as singles_pa_last20,
            @doubles_pa := doubles_last20/pa_last20 as doubles_pa_last20,
            @triples_pa := triples_last20/pa_last20 as triples_pa_last20,
            @homeruns_pa := homeruns_last20/pa_last20 as homeruns_pa_last20,
            @groundballs_pa := groundballs_last20/pa_last20 as groundballs_pa_last20,
            @linedrives_pa := linedrives_last20/pa_last20 as linedrives_pa_last20,

            @ball_p := ball_last20/p_last20 as ball_p_last20,
            @ball_pa := ball_last20/pa_last20 as ball_pa_last20,
            @strike_p := strike_last20/p_last20 as strike_p_last20,
            @strike_pa := strike_last20/pa_last20 as strike_pa_last20,

            @calledstrike_strike := calledstrike_last20/strike_last20 as calledstrike_strike_last20,
            @whiffstrike_strike := whiffstrike_last20/strike_last20 as whiffstrike_strike_last20,
            @foulstrike_strike := foulstrike_last20/strike_last20 as foulstrike_strike_last20,
            @inplay_strike := inplay_last20/strike_last20 as inplay_strike_last20,
            @calledstrike_pa := calledstrike_last20/pa_last20 as calledstrike_pa_last20,
            @whiffstrike_pa := whiffstrike_last20/pa_last20 as whiffstrike_pa_last20,
            @foulstrike_pa := foulstrike_last20/pa_last20 as foulstrike_pa_last20,
            @inplay_pa := inplay_last20/pa_last20 as inplay_pa_last20,

            @swing_p := swing_last20/p_last20 as swing_p_last20,
            @take_p := take_last20/p_last20 as take_p_last20,
            @swing_pa := swing_last20/pa_last20 as swing_pa_last20,
            @take_pa := take_last20/pa_last20 as take_pa_last20,

            @firstpstrike_pa := firstpstrike_last20/pa_last20 as firstpstrike_pa_last20,
            @secondpstrike_pa := secondpstrike_last20/firstpnotinplay_fl_last20 as secondpstrike_pa_last20,

            @zoneedge_in2_p := zoneedge_in2_last20/p_last20 as zoneedge_in2_p_last20,
            @zoneedge_in4_p := zoneedge_in4_last20/p_last20 as zoneedge_in4_p_last20,
            @zoneedge_out2_p := zoneedge_out2_last20/p_last20 as zoneedge_out2_p_last20,
            @zoneedge_out4_p := zoneedge_out4_last20/p_last20 as zoneedge_out4_p_last20,
            @zonecorn_in2_p := zonecorn_in2_last20/p_last20 as zonecorn_in2_p_last20,
            @zonecorn_in4_p := zonecorn_in4_last20/p_last20 as zonecorn_in4_p_last20,
            @zonecorn_out2_p := zonecorn_out2_last20/p_last20 as zonecorn_out2_p_last20,
            @zonecorn_out4_p := zonecorn_out4_last20/p_last20 as zonecorn_out4_p_last20,
            @zone_mid3_p := zone_mid3_last20/p_last20 as zone_mid3_p_last20,
            @zone_mid6_p := zone_mid6_last20/p_last20 as zone_mid6_p_last20,
            @zone_bigmiss4_p := zone_bigmiss4_last20/p_last20 as zone_bigmiss4_p_last20,

            @fastball_p := fastball_last20/p_last20 as fastball_p_last20,
            @fastball_endspeed_p := fastball_endspeed_last20/p_last20 as fastball_endspeed_p_last20,
            @fastball_spinrate_p := fastball_spinrate_last20/p_last20 as fastball_spinrate_p_last20,
            @fastball_spindir_p := fastball_spindir_last20/p_last20 as fastball_spindir_p_last20,
            @fastball_pfx_x_p := fastball_pfx_x_last20/p_last20 as fastball_pfx_x_p_last20,
            @fastball_pfx_z_p := fastball_pfx_z_last20/p_last20 as fastball_pfx_z_p_last20,
            @fastball_mnorm_p := fastball_mnorm_last20/p_last20 as fastball_mnorm_p_last20,
            @fastball_adjmnorm_p := fastball_adjmnorm_last20/p_last20 as fastball_adjmnorm_p_last20,

            @curveball_p := curveball_last20/p_last20 as curveball_p_last20,
            @curveball_endspeed_p := curveball_endspeed_last20/p_last20 as curveball_endspeed_p_last20,
            @curveball_spinrate_p := curveball_spinrate_last20/p_last20 as curveball_spinrate_p_last20,
            @curveball_spindir_p := curveball_spindir_last20/p_last20 as curveball_spindir_p_last20,
            @curveball_pfx_x_p := curveball_pfx_x_last20/p_last20 as curveball_pfx_x_p_last20,
            @curveball_pfx_z_p := curveball_pfx_z_last20/p_last20 as curveball_pfx_z_p_last20,
            @curveball_mnorm_p := curveball_mnorm_last20/p_last20 as curveball_mnorm_p_last20,
            @curveball_adjmnorm_p := curveball_adjmnorm_last20/p_last20 as curveball_adjmnorm_p_last20,

            @slider_p := slider_last20/p_last20 as slider_p_last20,
            @slider_endspeed_p := slider_endspeed_last20/p_last20 as slider_endspeed_p_last20,
            @slider_spinrate_p := slider_spinrate_last20/p_last20 as slider_spinrate_p_last20,
            @slider_spindir_p := slider_spindir_last20/p_last20 as slider_spindir_p_last20,
            @slider_pfx_x_p := slider_pfx_x_last20/p_last20 as slider_pfx_x_p_last20,
            @slider_pfx_z_p := slider_pfx_z_last20/p_last20 as slider_pfx_z_p_last20,
            @slider_mnorm_p := slider_mnorm_last20/p_last20 as slider_mnorm_p_last20,
            @slider_adjmnorm_p := slider_adjmnorm_last20/p_last20 as slider_adjmnorm_p_last20,

            @changeup_p := changeup_last20/p_last20 as changeup_p_last20,
            @changeup_endspeed_p := changeup_endspeed_last20/p_last20 as changeup_endspeed_p_last20,
            @changeup_spinrate_p := changeup_spinrate_last20/p_last20 as changeup_spinrate_p_last20,
            @changeup_spindir_p := changeup_spindir_last20/p_last20 as changeup_spindir_p_last20,
            @changeup_pfx_x_p := changeup_pfx_x_last20/p_last20 as changeup_pfx_x_p_last20,
            @changeup_pfx_z_p := changeup_pfx_z_last20/p_last20 as changeup_pfx_z_p_last20,
            @changeup_mnorm_p := changeup_mnorm_last20/p_last20 as changeup_mnorm_p_last20,
            @changeup_adjmnorm_p := changeup_adjmnorm_last20/p_last20 as changeup_adjmnorm_p_last20,
        
            NULL,
            NULL,
            NULL

            FROM anal_pitcher_counting_p as p
            JOIN anal_pitcher_counting_ab as ab
            ON p.anal_game_date = ab.anal_game_date
            AND p.pitcher = ab.pitcher
            
            WHERE p.anal_game_date = @analdate + interval '1' day
            AND ab.anal_game_date = @analdate + interval '1' day
        

        """)   

        curA.execute(query3)
        cnx.commit()

    
        affectedrows += curA.rowcount    
    

        print('   ',activedate, str(datetime.datetime.now() - starttime))

    curA.close()
    cnx.close()
    print('    mySQL DB connection closed')
    print('table updated: anal_pitcher_rate')
    
    return affectedrows


# In[ ]:

def anal_team_counting_off():
    
    print('update starting: anal_team_counting_off')
    
    dates, cnx, curA = get_missingdates("anal_team_counting_off")
    
    affectedrows = 0      
    
    for date in dates:

        starttime = datetime.datetime.now()

        activedate = date[0]

        query2 = "SET @analdate := '"+str(activedate)+"';"
        curA.execute(query2)

        query3 = """
            INSERT INTO anal_team_counting_off
            SELECT DISTINCT
            @analdate + interval '1' day as anal_game_date_off,
            @team_id := CASE WHEN a.home_bat_fl = 0 THEN g.away_id ELSE g.home_id END as team_id_off,
            @team_name := CASE WHEN a.home_bat_fl = 0 THEN g.away_name_full ELSE g.home_name_full END as team_name_off,

            ###########  STD  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.inn_num) END) as i_std_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_off,

            @pa_out := COUNT(CASE WHEN @logic_std = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_off,
            @outsmade := SUM(CASE WHEN @logic_std = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_off,
            @hits := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_off,
            @onbases := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_off,
            @totbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_off,
            @runsscored := SUM(CASE WHEN @logic_std = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_off,

            ###########  std_home  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std_home := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.home_bat_fl = 1) = 1 THEN a.gid END) as gp_std_home_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_std_home = 1 THEN concat(a.gid,a.inn_num) END) as i_std_home_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std_home = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_home_off,

            @pa_out := COUNT(CASE WHEN @logic_std_home = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_home_off,
            @outsmade := SUM(CASE WHEN @logic_std_home = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_home_off,
            @hits := COUNT(CASE WHEN @logic_std_home = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_home_off,
            @onbases := COUNT(CASE WHEN @logic_std_home = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_home_off,
            @totbasesadv := SUM(CASE WHEN @logic_std_home = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_home_off,
            @runsscored := SUM(CASE WHEN @logic_std_home = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_home_off,

            ###########  std_away  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std_away := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.home_bat_fl = 0) = 1 THEN a.gid END) as gp_std_away_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_std_away = 1 THEN concat(a.gid,a.inn_num) END) as i_std_away_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std_away = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_away_off,

            @pa_out := COUNT(CASE WHEN @logic_std_away = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_away_off,
            @outsmade := SUM(CASE WHEN @logic_std_away = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_away_off,
            @hits := COUNT(CASE WHEN @logic_std_away = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_away_off,
            @onbases := COUNT(CASE WHEN @logic_std_away = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_away_off,
            @totbasesadv := SUM(CASE WHEN @logic_std_away = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_away_off,
            @runsscored := SUM(CASE WHEN @logic_std_away = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_away_off,

            ###########  last60  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 THEN a.gid END) as gp_last60_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_off,

            @pa_out := COUNT(CASE WHEN @logic_last60 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_off,
            @outsmade := SUM(CASE WHEN @logic_last60 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_off,
            @hits := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_off,
            @onbases := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_off,
            @totbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_off,
            @runsscored := SUM(CASE WHEN @logic_last60 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_off,

            ###########  last30  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last30 := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate) = 1 THEN a.gid END) as gp_last30_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_off,

            @pa_out := COUNT(CASE WHEN @logic_last30 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_off,
            @outsmade := SUM(CASE WHEN @logic_last30 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_off,
            @hits := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_off,
            @onbases := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_off,
            @totbasesadv := SUM(CASE WHEN @logic_last30 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_off,
            @runsscored := SUM(CASE WHEN @logic_last30 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_off,

            ###########  last10  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last10 := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate) = 1 THEN a.gid END) as gp_last10_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_off,

            @pa_out := COUNT(CASE WHEN @logic_last10 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_off,
            @outsmade := SUM(CASE WHEN @logic_last10 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_off,
            @hits := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_off,
            @onbases := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_off,
            @totbasesadv := SUM(CASE WHEN @logic_last10 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_off,
            @runsscored := SUM(CASE WHEN @logic_last10 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_off,

            ###########  std_sp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std_sp := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND sp_fl = 1) = 1 THEN a.gid END) as gp_std_sp_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_std_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_std_sp_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_sp_off,

            @pa_out := COUNT(CASE WHEN @logic_std_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_sp_off,
            @outsmade := SUM(CASE WHEN @logic_std_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_sp_off,
            @hits := COUNT(CASE WHEN @logic_std_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_sp_off,
            @onbases := COUNT(CASE WHEN @logic_std_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_sp_off,
            @totbasesadv := SUM(CASE WHEN @logic_std_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_sp_off,
            @runsscored := SUM(CASE WHEN @logic_std_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_sp_off,

            ###########  std_rp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std_rp := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND sp_fl = 0) = 1 THEN a.gid END) as gp_std_rp_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_std_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_std_rp_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_rp_off,

            @pa_out := COUNT(CASE WHEN @logic_std_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_rp_off,
            @outsmade := SUM(CASE WHEN @logic_std_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_rp_off,
            @hits := COUNT(CASE WHEN @logic_std_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_rp_off,
            @onbases := COUNT(CASE WHEN @logic_std_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_rp_off,
            @totbasesadv := SUM(CASE WHEN @logic_std_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_rp_off,
            @runsscored := SUM(CASE WHEN @logic_std_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_rp_off,

            ###########  last60_sp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last60_sp := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last60_sp_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_last60_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_sp_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last60_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_sp_off,

            @pa_out := COUNT(CASE WHEN @logic_last60_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_sp_off,
            @outsmade := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_sp_off,
            @hits := COUNT(CASE WHEN @logic_last60_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_sp_off,
            @onbases := COUNT(CASE WHEN @logic_last60_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_sp_off,
            @totbasesadv := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_sp_off,
            @runsscored := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_sp_off,

            ###########  last30_sp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last30_sp := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last30_sp_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_last30_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_sp_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last30_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_sp_off,

            @pa_out := COUNT(CASE WHEN @logic_last30_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_sp_off,
            @outsmade := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_sp_off,
            @hits := COUNT(CASE WHEN @logic_last30_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_sp_off,
            @onbases := COUNT(CASE WHEN @logic_last30_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_sp_off,
            @totbasesadv := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_sp_off,
            @runsscored := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_sp_off,

            ###########  last10_sp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last10_sp := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last10_sp_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_last10_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_sp_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last10_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_sp_off,

            @pa_out := COUNT(CASE WHEN @logic_last10_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_sp_off,
            @outsmade := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_sp_off,
            @hits := COUNT(CASE WHEN @logic_last10_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_sp_off,
            @onbases := COUNT(CASE WHEN @logic_last10_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_sp_off,
            @totbasesadv := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_sp_off,
            @runsscored := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_sp_off,

            ###########  last60_rp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last60_rp := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last60_rp_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_last60_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_rp_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last60_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_rp_off,

            @pa_out := COUNT(CASE WHEN @logic_last60_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_rp_off,
            @outsmade := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_rp_off,
            @hits := COUNT(CASE WHEN @logic_last60_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_rp_off,
            @onbases := COUNT(CASE WHEN @logic_last60_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_rp_off,
            @totbasesadv := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_rp_off,
            @runsscored := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_rp_off,

            ###########  last30_rp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last30_rp := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last30_rp_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_last30_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_rp_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last30_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_rp_off,

            @pa_out := COUNT(CASE WHEN @logic_last30_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_rp_off,
            @outsmade := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_rp_off,
            @hits := COUNT(CASE WHEN @logic_last30_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_rp_off,
            @onbases := COUNT(CASE WHEN @logic_last30_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_rp_off,
            @totbasesadv := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_rp_off,
            @runsscored := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_rp_off,

            ###########  last10_rp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last10_rp := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last10_rp_off,
            @i := COUNT(DISTINCT CASE WHEN @logic_last10_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_rp_off,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last10_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_rp_off,

            @pa_out := COUNT(CASE WHEN @logic_last10_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_rp_off,
            @outsmade := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_rp_off,
            @hits := COUNT(CASE WHEN @logic_last10_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_rp_off,
            @onbases := COUNT(CASE WHEN @logic_last10_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_rp_off,
            @totbasesadv := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_rp_off,
            @runsscored := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_rp_off,

            NULL, -- for createddate
            NULL, -- for lastmodifieddate
            NULL -- for autoincrement PK

            FROM
            analbase_atbat as a
            JOIN pfx_game as g
            ON g.gid = a.gid

            WHERE g.game_type = 'R'
            AND YEAR(@analdate) = YEAR(a.game_date) 
            AND a.game_date <= @analdate

            GROUP BY
            @analdate,
            CASE WHEN a.home_bat_fl = 0 THEN g.away_id ELSE g.home_id END

            HAVING COUNT(DISTINCT CASE WHEN (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.gid END) > 0
            ;
        """

        curA.execute(query3)
        cnx.commit()

    
    
    
        affectedrows += curA.rowcount

        print('   ',activedate, str(datetime.datetime.now() - starttime))

    curA.close()
    cnx.close()
    print('    mySQL DB connection closed')
    print('table updated: anal_team_counting_off')
    
    return affectedrows


# In[ ]:

def anal_team_counting_def():
    
    print('update starting: anal_team_counting_def')
    
    dates, cnx, curA = get_missingdates("anal_team_counting_def")
    
    affectedrows = 0    
    
    for date in dates:

        starttime = datetime.datetime.now()

        activedate = date[0]

        query2 = "SET @analdate := '"+str(activedate)+"';"
        curA.execute(query2)

        query3 = """
            INSERT INTO anal_team_counting_def
            SELECT DISTINCT
            @analdate + interval '1' day as anal_game_date_def,
            @team_id := CASE WHEN a.home_bat_fl = 1 THEN g.away_id ELSE g.home_id END as team_id_def,
            @team_name := CASE WHEN a.home_bat_fl = 1 THEN g.away_name_full ELSE g.home_name_full END as team_name_def,

            ###########  STD  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.inn_num) END) as i_std_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_def,

            @pa_out := COUNT(CASE WHEN @logic_std = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_def,
            @outsmade := SUM(CASE WHEN @logic_std = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_def,
            @hits := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_def,
            @onbases := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_def,
            @totbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_def,
            @runsscored := SUM(CASE WHEN @logic_std = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_def,

            ###########  std_home  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std_home := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.home_bat_fl = 1) = 1 THEN a.gid END) as gp_std_home_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_std_home = 1 THEN concat(a.gid,a.inn_num) END) as i_std_home_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std_home = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_home_def,

            @pa_out := COUNT(CASE WHEN @logic_std_home = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_home_def,
            @outsmade := SUM(CASE WHEN @logic_std_home = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_home_def,
            @hits := COUNT(CASE WHEN @logic_std_home = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_home_def,
            @onbases := COUNT(CASE WHEN @logic_std_home = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_home_def,
            @totbasesadv := SUM(CASE WHEN @logic_std_home = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_home_def,
            @runsscored := SUM(CASE WHEN @logic_std_home = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_home_def,

            ###########  std_away  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std_away := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.home_bat_fl = 0) = 1 THEN a.gid END) as gp_std_away_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_std_away = 1 THEN concat(a.gid,a.inn_num) END) as i_std_away_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std_away = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_away_def,

            @pa_out := COUNT(CASE WHEN @logic_std_away = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_away_def,
            @outsmade := SUM(CASE WHEN @logic_std_away = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_away_def,
            @hits := COUNT(CASE WHEN @logic_std_away = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_away_def,
            @onbases := COUNT(CASE WHEN @logic_std_away = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_away_def,
            @totbasesadv := SUM(CASE WHEN @logic_std_away = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_away_def,
            @runsscored := SUM(CASE WHEN @logic_std_away = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_away_def,

            ###########  last60  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 THEN a.gid END) as gp_last60_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_def,

            @pa_out := COUNT(CASE WHEN @logic_last60 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_def,
            @outsmade := SUM(CASE WHEN @logic_last60 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_def,
            @hits := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_def,
            @onbases := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_def,
            @totbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_def,
            @runsscored := SUM(CASE WHEN @logic_last60 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_def,

            ###########  last30  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last30 := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate) = 1 THEN a.gid END) as gp_last30_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_def,

            @pa_out := COUNT(CASE WHEN @logic_last30 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_def,
            @outsmade := SUM(CASE WHEN @logic_last30 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_def,
            @hits := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_def,
            @onbases := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_def,
            @totbasesadv := SUM(CASE WHEN @logic_last30 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_def,
            @runsscored := SUM(CASE WHEN @logic_last30 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_def,

            ###########  last10  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last10 := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate) = 1 THEN a.gid END) as gp_last10_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_def,

            @pa_out := COUNT(CASE WHEN @logic_last10 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_def,
            @outsmade := SUM(CASE WHEN @logic_last10 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_def,
            @hits := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_def,
            @onbases := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_def,
            @totbasesadv := SUM(CASE WHEN @logic_last10 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_def,
            @runsscored := SUM(CASE WHEN @logic_last10 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_def,

            ###########  std_sp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std_sp := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND sp_fl = 1) = 1 THEN a.gid END) as gp_std_sp_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_std_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_std_sp_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_sp_def,

            @pa_out := COUNT(CASE WHEN @logic_std_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_sp_def,
            @outsmade := SUM(CASE WHEN @logic_std_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_sp_def,
            @hits := COUNT(CASE WHEN @logic_std_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_sp_def,
            @onbases := COUNT(CASE WHEN @logic_std_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_sp_def,
            @totbasesadv := SUM(CASE WHEN @logic_std_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_sp_def,
            @runsscored := SUM(CASE WHEN @logic_std_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_sp_def,

            ###########  std_rp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_std_rp := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND sp_fl = 0) = 1 THEN a.gid END) as gp_std_rp_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_std_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_std_rp_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_std_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_rp_def,

            @pa_out := COUNT(CASE WHEN @logic_std_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_rp_def,
            @outsmade := SUM(CASE WHEN @logic_std_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_rp_def,
            @hits := COUNT(CASE WHEN @logic_std_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_rp_def,
            @onbases := COUNT(CASE WHEN @logic_std_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_rp_def,
            @totbasesadv := SUM(CASE WHEN @logic_std_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_rp_def,
            @runsscored := SUM(CASE WHEN @logic_std_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_rp_def,

            ###########  last60_sp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last60_sp := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last60_sp_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_last60_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_sp_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last60_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_sp_def,

            @pa_out := COUNT(CASE WHEN @logic_last60_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_sp_def,
            @outsmade := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_sp_def,
            @hits := COUNT(CASE WHEN @logic_last60_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_sp_def,
            @onbases := COUNT(CASE WHEN @logic_last60_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_sp_def,
            @totbasesadv := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_sp_def,
            @runsscored := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_sp_def,

            ###########  last30_sp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last30_sp := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last30_sp_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_last30_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_sp_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last30_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_sp_def,

            @pa_out := COUNT(CASE WHEN @logic_last30_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_sp_def,
            @outsmade := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_sp_def,
            @hits := COUNT(CASE WHEN @logic_last30_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_sp_def,
            @onbases := COUNT(CASE WHEN @logic_last30_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_sp_def,
            @totbasesadv := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_sp_def,
            @runsscored := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_sp_def,

            ###########  last10_sp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last10_sp := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last10_sp_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_last10_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_sp_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last10_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_sp_def,

            @pa_out := COUNT(CASE WHEN @logic_last10_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_sp_def,
            @outsmade := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_sp_def,
            @hits := COUNT(CASE WHEN @logic_last10_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_sp_def,
            @onbases := COUNT(CASE WHEN @logic_last10_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_sp_def,
            @totbasesadv := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_sp_def,
            @runsscored := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_sp_def,

            ###########  last60_rp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last60_rp := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last60_rp_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_last60_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_rp_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last60_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_rp_def,

            @pa_out := COUNT(CASE WHEN @logic_last60_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_rp_def,
            @outsmade := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_rp_def,
            @hits := COUNT(CASE WHEN @logic_last60_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_rp_def,
            @onbases := COUNT(CASE WHEN @logic_last60_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_rp_def,
            @totbasesadv := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_rp_def,
            @runsscored := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_rp_def,

            ###########  last30_rp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last30_rp := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last30_rp_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_last30_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_rp_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last30_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_rp_def,

            @pa_out := COUNT(CASE WHEN @logic_last30_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_rp_def,
            @outsmade := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_rp_def,
            @hits := COUNT(CASE WHEN @logic_last30_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_rp_def,
            @onbases := COUNT(CASE WHEN @logic_last30_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_rp_def,
            @totbasesadv := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_rp_def,
            @runsscored := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_rp_def,

            ###########  last10_rp  ############

            -- COUNTING STATS 

            @gp := COUNT(DISTINCT CASE WHEN @logic_last10_rp := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last10_rp_def,
            @i := COUNT(DISTINCT CASE WHEN @logic_last10_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_rp_def,
            @pa := COUNT(DISTINCT CASE WHEN @logic_last10_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_rp_def,

            @pa_out := COUNT(CASE WHEN @logic_last10_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_rp_def,
            @outsmade := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_rp_def,
            @hits := COUNT(CASE WHEN @logic_last10_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_rp_def,
            @onbases := COUNT(CASE WHEN @logic_last10_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_rp_def,
            @totbasesadv := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_rp_def,
            @runsscored := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_rp_def,

            NULL, -- for createddate
            NULL, -- for lastmodifieddate
            NULL -- for autoincrement PK

            FROM
            analbase_atbat as a
            JOIN pfx_game as g
            ON g.gid = a.gid

            WHERE g.game_type = 'R'
            AND YEAR(@analdate) = YEAR(a.game_date) 
            AND a.game_date <= @analdate

            GROUP BY
            @analdate,
            CASE WHEN a.home_bat_fl = 1 THEN g.away_id ELSE g.home_id END

            HAVING COUNT(DISTINCT CASE WHEN (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.gid END) > 0
            
            
            ;
        """

        curA.execute(query3)
        cnx.commit()

        affectedrows += curA.rowcount

        print('   ',activedate, str(datetime.datetime.now() - starttime))

    curA.close()
    cnx.close()
    print('    mySQL DB connection closed')
    print('table updated: anal_team_counting_def')
    
    return affectedrows


# In[ ]:

def anal_team_rate():
    
    print('update starting: anal_team_rate')
    
    dates, cnx, curA = get_missingdates("anal_team_rate")
    
    affectedrows = 0   
    
    for date in dates:
    
        starttime = datetime.datetime.now()

        activedate = date[0]

        query2 = "SET @analdate := '"+str(activedate)+"';"
        curA.execute(query2)
        
        
        query3 = ("""
        
            INSERT INTO anal_team_rate

            SELECT DISTINCT

            anal_game_date_off as anal_game_date,
            team_id_off as team_id,
            team_name_off as team_name,

            ##### std_off ######

            @outsmade_pa := outsmade_std_off/pa_std_off as outsmade_pa_std_off,
            @hits_pa := hits_std_off/pa_std_off as hits_pa_std_off,
            @obp := onbases_std_off/pa_std_off as obp_std_off,
            @tba_pa := totbasesadv_std_off/pa_std_off as tba_pa_std_off,
            @tba_o := totbasesadv_std_off/outsmade_std_off as tba_o_std_off,
            @runsscored_pa := runsscored_std_off/pa_std_off as runsscored_pa_std_off,

            ##### std_def ######

            @outsmade_pa := outsmade_std_def/pa_std_def as outsmade_pa_std_def,
            @hits_pa := hits_std_def/pa_std_def as hits_pa_std_def,
            @obp := onbases_std_def/pa_std_def as obp_std_def,
            @tba_pa := totbasesadv_std_def/pa_std_def as tba_pa_std_def,
            @tba_o := totbasesadv_std_def/outsmade_std_def as tba_o_std_def,
            @runsscored_pa := runsscored_std_def/pa_std_def as runsscored_pa_std_def,

            ##### std_sp_off ######

            @outsmade_pa := outsmade_std_sp_off/pa_std_sp_off as outsmade_pa_std_sp_off,
            @hits_pa := hits_std_sp_off/pa_std_sp_off as hits_pa_std_sp_off,
            @obp := onbases_std_sp_off/pa_std_sp_off as obp_std_sp_off,
            @tba_pa := totbasesadv_std_sp_off/pa_std_sp_off as tba_pa_std_sp_off,
            @tba_o := totbasesadv_std_sp_off/outsmade_std_sp_off as tba_o_std_sp_off,
            @runsscored_pa := runsscored_std_sp_off/pa_std_sp_off as runsscored_pa_std_sp_off,

            ##### std_sp_def ######

            @outsmade_pa := outsmade_std_sp_def/pa_std_sp_def as outsmade_pa_std_sp_def,
            @hits_pa := hits_std_sp_def/pa_std_sp_def as hits_pa_std_sp_def,
            @obp := onbases_std_sp_def/pa_std_sp_def as obp_std_sp_def,
            @tba_pa := totbasesadv_std_sp_def/pa_std_sp_def as tba_pa_std_sp_def,
            @tba_o := totbasesadv_std_sp_def/outsmade_std_sp_def as tba_o_std_sp_def,
            @runsscored_pa := runsscored_std_sp_def/pa_std_sp_def as runsscored_pa_std_sp_def,

            ##### std_rp_off ######

            @outsmade_pa := outsmade_std_rp_off/pa_std_rp_off as outsmade_pa_std_rp_off,
            @hits_pa := hits_std_rp_off/pa_std_rp_off as hits_pa_std_rp_off,
            @obp := onbases_std_rp_off/pa_std_rp_off as obp_std_rp_off,
            @tba_pa := totbasesadv_std_rp_off/pa_std_rp_off as tba_pa_std_rp_off,
            @tba_o := totbasesadv_std_rp_off/outsmade_std_rp_off as tba_o_std_rp_off,
            @runsscored_pa := runsscored_std_rp_off/pa_std_rp_off as runsscored_pa_std_rp_off,

            ##### std_rp_def ######

            @outsmade_pa := outsmade_std_rp_def/pa_std_rp_def as outsmade_pa_std_rp_def,
            @hits_pa := hits_std_rp_def/pa_std_rp_def as hits_pa_std_rp_def,
            @obp := onbases_std_rp_def/pa_std_rp_def as obp_std_rp_def,
            @tba_pa := totbasesadv_std_rp_def/pa_std_rp_def as tba_pa_std_rp_def,
            @tba_o := totbasesadv_std_rp_def/outsmade_std_rp_def as tba_o_std_rp_def,
            @runsscored_pa := runsscored_std_rp_def/pa_std_rp_def as runsscored_pa_std_rp_def,

            ##### std_home_off ######

            @outsmade_pa := outsmade_std_home_off/pa_std_home_off as outsmade_pa_std_home_off,
            @hits_pa := hits_std_home_off/pa_std_home_off as hits_pa_std_home_off,
            @obp := onbases_std_home_off/pa_std_home_off as obp_std_home_off,
            @tba_pa := totbasesadv_std_home_off/pa_std_home_off as tba_pa_std_home_off,
            @tba_o := totbasesadv_std_home_off/outsmade_std_home_off as tba_o_std_home_off,
            @runsscored_pa := runsscored_std_home_off/pa_std_home_off as runsscored_pa_std_home_off,

            ##### std_home_def ######

            @outsmade_pa := outsmade_std_home_def/pa_std_home_def as outsmade_pa_std_home_def,
            @hits_pa := hits_std_home_def/pa_std_home_def as hits_pa_std_home_def,
            @obp := onbases_std_home_def/pa_std_home_def as obp_std_home_def,
            @tba_pa := totbasesadv_std_home_def/pa_std_home_def as tba_pa_std_home_def,
            @tba_o := totbasesadv_std_home_def/outsmade_std_home_def as tba_o_std_home_def,
            @runsscored_pa := runsscored_std_home_def/pa_std_home_def as runsscored_pa_std_home_def,

            ##### std_away_off ######

            @outsmade_pa := outsmade_std_away_off/pa_std_away_off as outsmade_pa_std_away_off,
            @hits_pa := hits_std_away_off/pa_std_away_off as hits_pa_std_away_off,
            @obp := onbases_std_away_off/pa_std_away_off as obp_std_away_off,
            @tba_pa := totbasesadv_std_away_off/pa_std_away_off as tba_pa_std_away_off,
            @tba_o := totbasesadv_std_away_off/outsmade_std_away_off as tba_o_std_away_off,
            @runsscored_pa := runsscored_std_away_off/pa_std_away_off as runsscored_pa_std_away_off,

            ##### std_away_def ######

            @outsmade_pa := outsmade_std_away_def/pa_std_away_def as outsmade_pa_std_away_def,
            @hits_pa := hits_std_away_def/pa_std_away_def as hits_pa_std_away_def,
            @obp := onbases_std_away_def/pa_std_away_def as obp_std_away_def,
            @tba_pa := totbasesadv_std_away_def/pa_std_away_def as tba_pa_std_away_def,
            @tba_o := totbasesadv_std_away_def/outsmade_std_away_def as tba_o_std_away_def,
            @runsscored_pa := runsscored_std_away_def/pa_std_away_def as runsscored_pa_std_away_def,

            ##### last60_off ######

            @outsmade_pa := outsmade_last60_off/pa_last60_off as outsmade_pa_last60_off,
            @hits_pa := hits_last60_off/pa_last60_off as hits_pa_last60_off,
            @obp := onbases_last60_off/pa_last60_off as obp_last60_off,
            @tba_pa := totbasesadv_last60_off/pa_last60_off as tba_pa_last60_off,
            @tba_o := totbasesadv_last60_off/outsmade_last60_off as tba_o_last60_off,
            @runsscored_pa := runsscored_last60_off/pa_last60_off as runsscored_pa_last60_off,

            ##### last60_def ######

            @outsmade_pa := outsmade_last60_def/pa_last60_def as outsmade_pa_last60_def,
            @hits_pa := hits_last60_def/pa_last60_def as hits_pa_last60_def,
            @obp := onbases_last60_def/pa_last60_def as obp_last60_def,
            @tba_pa := totbasesadv_last60_def/pa_last60_def as tba_pa_last60_def,
            @tba_o := totbasesadv_last60_def/outsmade_last60_def as tba_o_last60_def,
            @runsscored_pa := runsscored_last60_def/pa_last60_def as runsscored_pa_last60_def,

            ##### last30_off ######

            @outsmade_pa := outsmade_last30_off/pa_last30_off as outsmade_pa_last30_off,
            @hits_pa := hits_last30_off/pa_last30_off as hits_pa_last30_off,
            @obp := onbases_last30_off/pa_last30_off as obp_last30_off,
            @tba_pa := totbasesadv_last30_off/pa_last30_off as tba_pa_last30_off,
            @tba_o := totbasesadv_last30_off/outsmade_last30_off as tba_o_last30_off,
            @runsscored_pa := runsscored_last30_off/pa_last30_off as runsscored_pa_last30_off,

            ##### last30_def ######

            @outsmade_pa := outsmade_last30_def/pa_last30_def as outsmade_pa_last30_def,
            @hits_pa := hits_last30_def/pa_last30_def as hits_pa_last30_def,
            @obp := onbases_last30_def/pa_last30_def as obp_last30_def,
            @tba_pa := totbasesadv_last30_def/pa_last30_def as tba_pa_last30_def,
            @tba_o := totbasesadv_last30_def/outsmade_last30_def as tba_o_last30_def,
            @runsscored_pa := runsscored_last30_def/pa_last30_def as runsscored_pa_last30_def,

            ##### last10_off ######

            @outsmade_pa := outsmade_last10_off/pa_last10_off as outsmade_pa_last10_off,
            @hits_pa := hits_last10_off/pa_last10_off as hits_pa_last10_off,
            @obp := onbases_last10_off/pa_last10_off as obp_last10_off,
            @tba_pa := totbasesadv_last10_off/pa_last10_off as tba_pa_last10_off,
            @tba_o := totbasesadv_last10_off/outsmade_last10_off as tba_o_last10_off,
            @runsscored_pa := runsscored_last10_off/pa_last10_off as runsscored_pa_last10_off,

            ##### last10_def ######

            @outsmade_pa := outsmade_last10_def/pa_last10_def as outsmade_pa_last10_def,
            @hits_pa := hits_last10_def/pa_last10_def as hits_pa_last10_def,
            @obp := onbases_last10_def/pa_last10_def as obp_last10_def,
            @tba_pa := totbasesadv_last10_def/pa_last10_def as tba_pa_last10_def,
            @tba_o := totbasesadv_last10_def/outsmade_last10_def as tba_o_last10_def,
            @runsscored_pa := runsscored_last10_def/pa_last10_def as runsscored_pa_last10_def,

            ##### last60_sp_off ######

            @outsmade_pa := outsmade_last60_sp_off/pa_last60_sp_off as outsmade_pa_last60_sp_off,
            @hits_pa := hits_last60_sp_off/pa_last60_sp_off as hits_pa_last60_sp_off,
            @obp := onbases_last60_sp_off/pa_last60_sp_off as obp_last60_sp_off,
            @tba_pa := totbasesadv_last60_sp_off/pa_last60_sp_off as tba_pa_last60_sp_off,
            @tba_o := totbasesadv_last60_sp_off/outsmade_last60_sp_off as tba_o_last60_sp_off,
            @runsscored_pa := runsscored_last60_sp_off/pa_last60_sp_off as runsscored_pa_last60_sp_off,

            ##### last60_sp_def ######

            @outsmade_pa := outsmade_last60_sp_def/pa_last60_sp_def as outsmade_pa_last60_sp_def,
            @hits_pa := hits_last60_sp_def/pa_last60_sp_def as hits_pa_last60_sp_def,
            @obp := onbases_last60_sp_def/pa_last60_sp_def as obp_last60_sp_def,
            @tba_pa := totbasesadv_last60_sp_def/pa_last60_sp_def as tba_pa_last60_sp_def,
            @tba_o := totbasesadv_last60_sp_def/outsmade_last60_sp_def as tba_o_last60_sp_def,
            @runsscored_pa := runsscored_last60_sp_def/pa_last60_sp_def as runsscored_pa_last60_sp_def,

            ##### last60_rp_off ######

            @outsmade_pa := outsmade_last60_rp_off/pa_last60_rp_off as outsmade_pa_last60_rp_off,
            @hits_pa := hits_last60_rp_off/pa_last60_rp_off as hits_pa_last60_rp_off,
            @obp := onbases_last60_rp_off/pa_last60_rp_off as obp_last60_rp_off,
            @tba_pa := totbasesadv_last60_rp_off/pa_last60_rp_off as tba_pa_last60_rp_off,
            @tba_o := totbasesadv_last60_rp_off/outsmade_last60_rp_off as tba_o_last60_rp_off,
            @runsscored_pa := runsscored_last60_rp_off/pa_last60_rp_off as runsscored_pa_last60_rp_off,

            ##### last60_rp_def ######

            @outsmade_pa := outsmade_last60_rp_def/pa_last60_rp_def as outsmade_pa_last60_rp_def,
            @hits_pa := hits_last60_rp_def/pa_last60_rp_def as hits_pa_last60_rp_def,
            @obp := onbases_last60_rp_def/pa_last60_rp_def as obp_last60_rp_def,
            @tba_pa := totbasesadv_last60_rp_def/pa_last60_rp_def as tba_pa_last60_rp_def,
            @tba_o := totbasesadv_last60_rp_def/outsmade_last60_rp_def as tba_o_last60_rp_def,
            @runsscored_pa := runsscored_last60_rp_def/pa_last60_rp_def as runsscored_pa_last60_rp_def,

            ##### last30_sp_off ######

            @outsmade_pa := outsmade_last30_sp_off/pa_last30_sp_off as outsmade_pa_last30_sp_off,
            @hits_pa := hits_last30_sp_off/pa_last30_sp_off as hits_pa_last30_sp_off,
            @obp := onbases_last30_sp_off/pa_last30_sp_off as obp_last30_sp_off,
            @tba_pa := totbasesadv_last30_sp_off/pa_last30_sp_off as tba_pa_last30_sp_off,
            @tba_o := totbasesadv_last30_sp_off/outsmade_last30_sp_off as tba_o_last30_sp_off,
            @runsscored_pa := runsscored_last30_sp_off/pa_last30_sp_off as runsscored_pa_last30_sp_off,

            ##### last30_sp_def ######

            @outsmade_pa := outsmade_last30_sp_def/pa_last30_sp_def as outsmade_pa_last30_sp_def,
            @hits_pa := hits_last30_sp_def/pa_last30_sp_def as hits_pa_last30_sp_def,
            @obp := onbases_last30_sp_def/pa_last30_sp_def as obp_last30_sp_def,
            @tba_pa := totbasesadv_last30_sp_def/pa_last30_sp_def as tba_pa_last30_sp_def,
            @tba_o := totbasesadv_last30_sp_def/outsmade_last30_sp_def as tba_o_last30_sp_def,
            @runsscored_pa := runsscored_last30_sp_def/pa_last30_sp_def as runsscored_pa_last30_sp_def,

            ##### last30_rp_off ######

            @outsmade_pa := outsmade_last30_rp_off/pa_last30_rp_off as outsmade_pa_last30_rp_off,
            @hits_pa := hits_last30_rp_off/pa_last30_rp_off as hits_pa_last30_rp_off,
            @obp := onbases_last30_rp_off/pa_last30_rp_off as obp_last30_rp_off,
            @tba_pa := totbasesadv_last30_rp_off/pa_last30_rp_off as tba_pa_last30_rp_off,
            @tba_o := totbasesadv_last30_rp_off/outsmade_last30_rp_off as tba_o_last30_rp_off,
            @runsscored_pa := runsscored_last30_rp_off/pa_last30_rp_off as runsscored_pa_last30_rp_off,

            ##### last30_rp_def ######

            @outsmade_pa := outsmade_last30_rp_def/pa_last30_rp_def as outsmade_pa_last30_rp_def,
            @hits_pa := hits_last30_rp_def/pa_last30_rp_def as hits_pa_last30_rp_def,
            @obp := onbases_last30_rp_def/pa_last30_rp_def as obp_last30_rp_def,
            @tba_pa := totbasesadv_last30_rp_def/pa_last30_rp_def as tba_pa_last30_rp_def,
            @tba_o := totbasesadv_last30_rp_def/outsmade_last30_rp_def as tba_o_last30_rp_def,
            @runsscored_pa := runsscored_last30_rp_def/pa_last30_rp_def as runsscored_pa_last30_rp_def,

            ##### last10_sp_off ######

            @outsmade_pa := outsmade_last10_sp_off/pa_last10_sp_off as outsmade_pa_last10_sp_off,
            @hits_pa := hits_last10_sp_off/pa_last10_sp_off as hits_pa_last10_sp_off,
            @obp := onbases_last10_sp_off/pa_last10_sp_off as obp_last10_sp_off,
            @tba_pa := totbasesadv_last10_sp_off/pa_last10_sp_off as tba_pa_last10_sp_off,
            @tba_o := totbasesadv_last10_sp_off/outsmade_last10_sp_off as tba_o_last10_sp_off,
            @runsscored_pa := runsscored_last10_sp_off/pa_last10_sp_off as runsscored_pa_last10_sp_off,

            ##### last10_sp_def ######

            @outsmade_pa := outsmade_last10_sp_def/pa_last10_sp_def as outsmade_pa_last10_sp_def,
            @hits_pa := hits_last10_sp_def/pa_last10_sp_def as hits_pa_last10_sp_def,
            @obp := onbases_last10_sp_def/pa_last10_sp_def as obp_last10_sp_def,
            @tba_pa := totbasesadv_last10_sp_def/pa_last10_sp_def as tba_pa_last10_sp_def,
            @tba_o := totbasesadv_last10_sp_def/outsmade_last10_sp_def as tba_o_last10_sp_def,
            @runsscored_pa := runsscored_last10_sp_def/pa_last10_sp_def as runsscored_pa_last10_sp_def,

            ##### last10_rp_off ######

            @outsmade_pa := outsmade_last10_rp_off/pa_last10_rp_off as outsmade_pa_last10_rp_off,
            @hits_pa := hits_last10_rp_off/pa_last10_rp_off as hits_pa_last10_rp_off,
            @obp := onbases_last10_rp_off/pa_last10_rp_off as obp_last10_rp_off,
            @tba_pa := totbasesadv_last10_rp_off/pa_last10_rp_off as tba_pa_last10_rp_off,
            @tba_o := totbasesadv_last10_rp_off/outsmade_last10_rp_off as tba_o_last10_rp_off,
            @runsscored_pa := runsscored_last10_rp_off/pa_last10_rp_off as runsscored_pa_last10_rp_off,

            ##### last10_rp_def ######

            @outsmade_pa := outsmade_last10_rp_def/pa_last10_rp_def as outsmade_pa_last10_rp_def,
            @hits_pa := hits_last10_rp_def/pa_last10_rp_def as hits_pa_last10_rp_def,
            @obp := onbases_last10_rp_def/pa_last10_rp_def as obp_last10_rp_def,
            @tba_pa := totbasesadv_last10_rp_def/pa_last10_rp_def as tba_pa_last10_rp_def,
            @tba_o := totbasesadv_last10_rp_def/outsmade_last10_rp_def as tba_o_last10_rp_def,
            @runsscored_pa := runsscored_last10_rp_def/pa_last10_rp_def as runsscored_pa_last10_rp_def,
        
            NULL,
            NULL,
            NULL

            FROM anal_team_counting_def as d
            JOIN anal_team_counting_off as o
            ON o.anal_game_date_off = d.anal_game_date_def
            AND o.team_id_off = d.team_id_def

            WHERE d.anal_game_date_def = @analdate + interval '1' day
            AND o.anal_game_date_off = @analdate + interval '1' day
        """)   

        curA.execute(query3)
        cnx.commit()
    

        affectedrows += curA.rowcount

        print('   ',activedate, str(datetime.datetime.now() - starttime))

    curA.close()
    cnx.close()
    print('    mySQL DB connection closed')
    print('table updated: anal_team_rate')    
    
    return affectedrows


# In[ ]:



