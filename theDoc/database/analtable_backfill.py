import datetime
from theDoc.database import mlb_analtablesupdate as mlbtab

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
    try:
        resultmsg += update_anal_table(tablename,tablefunc,analupdates)
    except Exception as e:
        msg = 'table - {} - anal update failed; error: {}'.format(tablename, e)
        print(msg)
        resultmsg += msg
    
script_endtime = datetime.datetime.now()
script_totaltime = script_endtime - script_starttime

endmsg = '    success - '+str(datetime.datetime.now())+' script total runtime = '+str(script_totaltime)
print(endmsg)
