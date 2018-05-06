import mysql.connector
import sys
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


CURRENT_DATE = datetime.datetime.now().date()



batter_columnarray = [
    #'anal_game_date',
    #'batter',
    #'pname',
    'outsmade_pa_std',
    'hits_pa_std',
    'avg_std',
    'obp_std',
    'sba_pa_std',
    'sba_o_std',
    'rba_pa_std',
    'rba_o_std',
    'tba_pa_std',
    'tba_o_std',
    'k_pa_std',
    'klook_pa_std',
    'kswing_pa_std',
    'walk_pa_std',
    'singles_pa_std',
    'doubles_pa_std',
    'triples_pa_std',
    'homeruns_pa_std',
    'sacs_pa_std',
    'groundballs_pa_std',
    'linedrives_pa_std',
    'outsmade_pa_last60',
    'hits_pa_last60',
    'avg_last60',
    'obp_last60',
    'sba_pa_last60',
    'sba_o_last60',
    'rba_pa_last60',
    'rba_o_last60',
    'tba_pa_last60',
    'tba_o_last60',
    'k_pa_last60',
    'klook_pa_last60',
    'kswing_pa_last60',
    'walk_pa_last60',
    'singles_pa_last60',
    'doubles_pa_last60',
    'triples_pa_last60',
    'homeruns_pa_last60',
    'sacs_pa_last60',
    'groundballs_pa_last60',
    'linedrives_pa_last60',
    'outsmade_pa_last30',
    'hits_pa_last30',
    'avg_last30',
    'obp_last30',
    'sba_pa_last30',
    'sba_o_last30',
    'rba_pa_last30',
    'rba_o_last30',
    'tba_pa_last30',
    'tba_o_last30',
    'k_pa_last30',
    'klook_pa_last30',
    'kswing_pa_last30',
    'walk_pa_last30',
    'singles_pa_last30',
    'doubles_pa_last30',
    'triples_pa_last30',
    'homeruns_pa_last30',
    'sacs_pa_last30',
    'groundballs_pa_last30',
    'linedrives_pa_last30',
    'outsmade_pa_last10',
    'hits_pa_last10',
    'avg_last10',
    'obp_last10',
    'sba_pa_last10',
    'sba_o_last10',
    'rba_pa_last10',
    'rba_o_last10',
    'tba_pa_last10',
    'tba_o_last10',
    'k_pa_last10',
    'klook_pa_last10',
    'kswing_pa_last10',
    'walk_pa_last10',
    'singles_pa_last10',
    'doubles_pa_last10',
    'triples_pa_last10',
    'homeruns_pa_last10',
    'sacs_pa_last10',
    'groundballs_pa_last10',
    'linedrives_pa_last10',
    'outsmade_pa_std2',
    'hits_pa_std2',
    'avg_std2',
    'obp_std2',
    'sba_pa_std2',
    'sba_o_std2',
    'rba_pa_std2',
    'rba_o_std2',
    'tba_pa_std2',
    'tba_o_std2',
    'k_pa_std2',
    'klook_pa_std2',
    'kswing_pa_std2',
    'walk_pa_std2',
    'singles_pa_std2',
    'doubles_pa_std2',
    'triples_pa_std2',
    'homeruns_pa_std2',
    'sacs_pa_std2',
    'groundballs_pa_std2',
    'linedrives_pa_std2',
    'outsmade_pa_std3',
    'hits_pa_std3',
    'avg_std3',
    'obp_std3',
    'sba_pa_std3',
    'sba_o_std3',
    'rba_pa_std3',
    'rba_o_std3',
    'tba_pa_std3',
    'tba_o_std3',
    'k_pa_std3',
    'klook_pa_std3',
    'kswing_pa_std3',
    'walk_pa_std3',
    'singles_pa_std3',
    'doubles_pa_std3',
    'triples_pa_std3',
    'homeruns_pa_std3',
    'sacs_pa_std3',
    'groundballs_pa_std3',
    'linedrives_pa_std3',
    'outsmade_pa_std_rh',
    'hits_pa_std_rh',
    'avg_std_rh',
    'obp_std_rh',
    'sba_pa_std_rh',
    'sba_o_std_rh',
    'rba_pa_std_rh',
    'rba_o_std_rh',
    'tba_pa_std_rh',
    'tba_o_std_rh',
    'k_pa_std_rh',
    'klook_pa_std_rh',
    'kswing_pa_std_rh',
    'walk_pa_std_rh',
    'singles_pa_std_rh',
    'doubles_pa_std_rh',
    'triples_pa_std_rh',
    'homeruns_pa_std_rh',
    'sacs_pa_std_rh',
    'groundballs_pa_std_rh',
    'linedrives_pa_std_rh',
    'outsmade_pa_last60_rh',
    'hits_pa_last60_rh',
    'avg_last60_rh',
    'obp_last60_rh',
    'sba_pa_last60_rh',
    'sba_o_last60_rh',
    'rba_pa_last60_rh',
    'rba_o_last60_rh',
    'tba_pa_last60_rh',
    'tba_o_last60_rh',
    'k_pa_last60_rh',
    'klook_pa_last60_rh',
    'kswing_pa_last60_rh',
    'walk_pa_last60_rh',
    'singles_pa_last60_rh',
    'doubles_pa_last60_rh',
    'triples_pa_last60_rh',
    'homeruns_pa_last60_rh',
    'sacs_pa_last60_rh',
    'groundballs_pa_last60_rh',
    'linedrives_pa_last60_rh',
    'outsmade_pa_last30_rh',
    'hits_pa_last30_rh',
    'avg_last30_rh',
    'obp_last30_rh',
    'sba_pa_last30_rh',
    'sba_o_last30_rh',
    'rba_pa_last30_rh',
    'rba_o_last30_rh',
    'tba_pa_last30_rh',
    'tba_o_last30_rh',
    'k_pa_last30_rh',
    'klook_pa_last30_rh',
    'kswing_pa_last30_rh',
    'walk_pa_last30_rh',
    'singles_pa_last30_rh',
    'doubles_pa_last30_rh',
    'triples_pa_last30_rh',
    'homeruns_pa_last30_rh',
    'sacs_pa_last30_rh',
    'groundballs_pa_last30_rh',
    'linedrives_pa_last30_rh',
    'outsmade_pa_last10_rh',
    'hits_pa_last10_rh',
    'avg_last10_rh',
    'obp_last10_rh',
    'sba_pa_last10_rh',
    'sba_o_last10_rh',
    'rba_pa_last10_rh',
    'rba_o_last10_rh',
    'tba_pa_last10_rh',
    'tba_o_last10_rh',
    'k_pa_last10_rh',
    'klook_pa_last10_rh',
    'kswing_pa_last10_rh',
    'walk_pa_last10_rh',
    'singles_pa_last10_rh',
    'doubles_pa_last10_rh',
    'triples_pa_last10_rh',
    'homeruns_pa_last10_rh',
    'sacs_pa_last10_rh',
    'groundballs_pa_last10_rh',
    'linedrives_pa_last10_rh',
    'outsmade_pa_std2_rh',
    'hits_pa_std2_rh',
    'avg_std2_rh',
    'obp_std2_rh',
    'sba_pa_std2_rh',
    'sba_o_std2_rh',
    'rba_pa_std2_rh',
    'rba_o_std2_rh',
    'tba_pa_std2_rh',
    'tba_o_std2_rh',
    'k_pa_std2_rh',
    'klook_pa_std2_rh',
    'kswing_pa_std2_rh',
    'walk_pa_std2_rh',
    'singles_pa_std2_rh',
    'doubles_pa_std2_rh',
    'triples_pa_std2_rh',
    'homeruns_pa_std2_rh',
    'sacs_pa_std2_rh',
    'groundballs_pa_std2_rh',
    'linedrives_pa_std2_rh',
    'outsmade_pa_std3_rh',
    'hits_pa_std3_rh',
    'avg_std3_rh',
    'obp_std3_rh',
    'sba_pa_std3_rh',
    'sba_o_std3_rh',
    'rba_pa_std3_rh',
    'rba_o_std3_rh',
    'tba_pa_std3_rh',
    'tba_o_std3_rh',
    'k_pa_std3_rh',
    'klook_pa_std3_rh',
    'kswing_pa_std3_rh',
    'walk_pa_std3_rh',
    'singles_pa_std3_rh',
    'doubles_pa_std3_rh',
    'triples_pa_std3_rh',
    'homeruns_pa_std3_rh',
    'sacs_pa_std3_rh',
    'groundballs_pa_std3_rh',
    'linedrives_pa_std3_rh',
    'outsmade_pa_std_lh',
    'hits_pa_std_lh',
    'avg_std_lh',
    'obp_std_lh',
    'sba_pa_std_lh',
    'sba_o_std_lh',
    'rba_pa_std_lh',
    'rba_o_std_lh',
    'tba_pa_std_lh',
    'tba_o_std_lh',
    'k_pa_std_lh',
    'klook_pa_std_lh',
    'kswing_pa_std_lh',
    'walk_pa_std_lh',
    'singles_pa_std_lh',
    'doubles_pa_std_lh',
    'triples_pa_std_lh',
    'homeruns_pa_std_lh',
    'sacs_pa_std_lh',
    'groundballs_pa_std_lh',
    'linedrives_pa_std_lh',
    'outsmade_pa_last60_lh',
    'hits_pa_last60_lh',
    'avg_last60_lh',
    'obp_last60_lh',
    'sba_pa_last60_lh',
    'sba_o_last60_lh',
    'rba_pa_last60_lh',
    'rba_o_last60_lh',
    'tba_pa_last60_lh',
    'tba_o_last60_lh',
    'k_pa_last60_lh',
    'klook_pa_last60_lh',
    'kswing_pa_last60_lh',
    'walk_pa_last60_lh',
    'singles_pa_last60_lh',
    'doubles_pa_last60_lh',
    'triples_pa_last60_lh',
    'homeruns_pa_last60_lh',
    'sacs_pa_last60_lh',
    'groundballs_pa_last60_lh',
    'linedrives_pa_last60_lh',
    'outsmade_pa_last30_lh',
    'hits_pa_last30_lh',
    'avg_last30_lh',
    'obp_last30_lh',
    'sba_pa_last30_lh',
    'sba_o_last30_lh',
    'rba_pa_last30_lh',
    'rba_o_last30_lh',
    'tba_pa_last30_lh',
    'tba_o_last30_lh',
    'k_pa_last30_lh',
    'klook_pa_last30_lh',
    'kswing_pa_last30_lh',
    'walk_pa_last30_lh',
    'singles_pa_last30_lh',
    'doubles_pa_last30_lh',
    'triples_pa_last30_lh',
    'homeruns_pa_last30_lh',
    'sacs_pa_last30_lh',
    'groundballs_pa_last30_lh',
    'linedrives_pa_last30_lh',
    'outsmade_pa_last10_lh',
    'hits_pa_last10_lh',
    'avg_last10_lh',
    'obp_last10_lh',
    'sba_pa_last10_lh',
    'sba_o_last10_lh',
    'rba_pa_last10_lh',
    'rba_o_last10_lh',
    'tba_pa_last10_lh',
    'tba_o_last10_lh',
    'k_pa_last10_lh',
    'klook_pa_last10_lh',
    'kswing_pa_last10_lh',
    'walk_pa_last10_lh',
    'singles_pa_last10_lh',
    'doubles_pa_last10_lh',
    'triples_pa_last10_lh',
    'homeruns_pa_last10_lh',
    'sacs_pa_last10_lh',
    'groundballs_pa_last10_lh',
    'linedrives_pa_last10_lh',
    'outsmade_pa_std2_lh',
    'hits_pa_std2_lh',
    'avg_std2_lh',
    'obp_std2_lh',
    'sba_pa_std2_lh',
    'sba_o_std2_lh',
    'rba_pa_std2_lh',
    'rba_o_std2_lh',
    'tba_pa_std2_lh',
    'tba_o_std2_lh',
    'k_pa_std2_lh',
    'klook_pa_std2_lh',
    'kswing_pa_std2_lh',
    'walk_pa_std2_lh',
    'singles_pa_std2_lh',
    'doubles_pa_std2_lh',
    'triples_pa_std2_lh',
    'homeruns_pa_std2_lh',
    'sacs_pa_std2_lh',
    'groundballs_pa_std2_lh',
    'linedrives_pa_std2_lh',
    'outsmade_pa_std3_lh',
    'hits_pa_std3_lh',
    'avg_std3_lh',
    'obp_std3_lh',
    'sba_pa_std3_lh',
    'sba_o_std3_lh',
    'rba_pa_std3_lh',
    'rba_o_std3_lh',
    'tba_pa_std3_lh',
    'tba_o_std3_lh',
    'k_pa_std3_lh',
    'klook_pa_std3_lh',
    'kswing_pa_std3_lh',
    'walk_pa_std3_lh',
    'singles_pa_std3_lh',
    'doubles_pa_std3_lh',
    'triples_pa_std3_lh',
    'homeruns_pa_std3_lh',
    'sacs_pa_std3_lh',
    'groundballs_pa_std3_lh',
    'linedrives_pa_std3_lh'
    ]

batterpa_columnarray = [
    'pa_last10',
    'pa_last30',
    'pa_last60',
    'pa_std',
    'pa_std2',
    'pa_std3',
    
    'pa_last10_rh',
    'pa_last30_rh',
    'pa_last60_rh',
    'pa_std_rh',
    'pa_std2_rh',
    'pa_std3_rh',
    
    'pa_last10_lh',
    'pa_last30_lh',
    'pa_last60_lh',
    'pa_std_lh',
    'pa_std2_lh',
    'pa_std3_lh',
]

batterlines = ''
for col in batter_columnarray:
    batterlines += col+','
for col in batterpa_columnarray:
    batterlines += col+','

query = """SELECT anal_batter_rate.anal_game_date, anal_batter_rate.batter, """+batterlines+""" NULL as to_drop 
        FROM anal_batter_rate 
        JOIN anal_batter_counting
        ON anal_batter_rate.anal_game_date = anal_batter_counting.anal_game_date
        AND anal_batter_rate.batter = anal_batter_counting.batter

"""


#### EXTRACT BATTERS DATASET




try:
    cnx = mysql.connector.connect(user='dan', password='',
                                host='127.0.0.1',
                                database='mlb'
                         )
    print('Successfully connected to mySQL DB')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
        
curA = cnx.cursor()

#query0 = "SET SESSION optimizer_search_depth = 0;"
#curA.execute(query0)
#cnx.commit()


query_start = datetime.datetime.now()
print('about to execute query: '+str(query_start))
try:
    curA.execute(query)
except:
    sys.exit()
print('query executed - total time: '+str(datetime.datetime.now()-query_start))
    
cur_columns = curA.column_names

print('about to fetch results')

#batters_data = np.array(curA.fetchall())
def ResultIter(cursor, arraysize=10):
    # 'An iterator that uses fetchmany to keep memory usage down'
    n = 0
    while True:
        results = cursor.fetchmany(arraysize)
        n += 1
        print(n)
        if not results:
            break
        yield results

batters_data = None
chunknum = 0
for result in ResultIter(curA, arraysize=100000):
    chunknum += 1
    batters_data = pd.DataFrame(result)
    batters_data.columns = cur_columns
    if chunknum == 1:
        batters_data.to_csv('battersdataset{}.csv'.format(CURRENT_DATE), header=True, mode='w')
    else:
        batters_data.to_csv('battersdataset{}.csv'.format(CURRENT_DATE), header=False, mode='a')
    del batters_data
            
print('results fetched '+str(datetime.datetime.now())+' - total time: '+str(datetime.datetime.now()-query_start))


curA.close()
cnx.close()




#print(batters)
#print(batters_joins)
#print('SCRIPTEND',datetime.datetime.now()-starttime)




#### CLEAN BATTERS DATASET


df = pd.read_csv('battersdataset{}.csv'.format(CURRENT_DATE))

tuples = list(zip(df["anal_game_date"].apply(datetime.datetime.strptime,args=(["%Y-%m-%d"])),df["batter"]))
gamedateindex = pd.MultiIndex.from_tuples(tuples,names=['anal_game_date','batter'])
df = df.set_index(gamedateindex)
df = df.drop(['anal_game_date','batter'],axis=1)
df = df.drop(['Unnamed: 0','to_drop'],axis=1)


def rollup_withthresh(df,target_col,thresh_col,thresh_min,parent_col):
    if df[thresh_col] < thresh_min:
        if parent_col is None:
            return None
        else:
            return df[parent_col]
    else:
        return df[target_col]
        


cols = [
    'outsmade_pa',
    'hits_pa',
    'avg',
    'obp',
    'sba_pa',
    'sba_o',
    'rba_pa',
    'rba_o',
    'tba_pa',
    'tba_o',
    'k_pa',
    'klook_pa',
    'kswing_pa',
    'walk_pa',
    'singles_pa',
    'doubles_pa',
    'triples_pa',
    'homeruns_pa',
    'sacs_pa',
    'groundballs_pa',
    'linedrives_pa',
]

buckets = [
    'last10',
    'last30',
    'last60',
    'std',
    'std2',
    'std3',
    
    'last10_rh',
    'last30_rh',
    'last60_rh',
    'std_rh',
    'std2_rh',
    'std3_rh',
    
    'last10_lh',
    'last30_lh',
    'last60_lh',
    'std_lh',
    'std2_lh',
    'std3_lh'
]



target_buckets = np.flip(np.array(buckets),0)
parent_buckets = np.array([None] + list(target_buckets[:-1]))
parent_buckets[6] = None
parent_buckets[12] = None
thresh_mins = {  
                'last10':20,
                'last30':60,
                'last60':120,
                'std':60,
                'std2':60,
                'std3':60,

                'last10_rh':15,
                'last30_rh':45,
                'last60_rh':90,
                'std_rh':45,
                'std2_rh':45,
                'std3_rh':45,

                'last10_lh':10,
                'last30_lh':30,
                'last60_lh':60,
                'std_lh':30,
                'std2_lh':30,
                'std3_lh':30
}

for target_bucket, parent_bucket in zip(target_buckets, parent_buckets):
    
    print(target_bucket)
    starttime = datetime.datetime.now()

    target_list = []
    parent_list = []
    for col in cols:
        target_list.append(col+'_'+target_bucket)
        if parent_bucket is None:
            parent_list.append(None)
        else:
            parent_list.append(col+'_'+parent_bucket)
    thresh_min = thresh_mins[target_bucket]
    for target_col, parent_col in zip(target_list,parent_list):
        df[target_col] = df.apply(rollup_withthresh, axis=1, args=(target_col, 'pa_'+target_bucket, thresh_min, parent_col)) 
    
    print('time elapsed:', datetime.datetime.now()-starttime)
        
df.to_csv('battersdataset_clean_{}.csv'.format(CURRENT_DATE), header=True, mode='w')
del df

print('done!', datetime.datetime.now())



### IMPUTE CLEANED BATTERS DATASET



buckets = [
    'last10',
    'last30',
    'last60',
    'std',
    'std2',
    'std3',
    
    'last10_rh',
    'last30_rh',
    'last60_rh',
    'std_rh',
    'std2_rh',
    'std3_rh',
    
    'last10_lh',
    'last30_lh',
    'last60_lh',
    'std_lh',
    'std2_lh',
    'std3_lh'
]

pa_buckets = ['pa_'+b for b in buckets]
pa_buckets

print('make index')

battersdf = pd.read_csv('battersdataset_clean_{}.csv'.format(CURRENT_DATE))

tuples = list(zip(battersdf["anal_game_date"].apply(datetime.datetime.strptime,args=(["%Y-%m-%d"])),battersdf["batter"]))
gamedateindex = pd.MultiIndex.from_tuples(tuples,names=['anal_game_date','batter'])
battersdf = battersdf.set_index(gamedateindex)
battersdf = battersdf.drop(['anal_game_date','batter'],axis=1)
battersdf = battersdf.drop(pa_buckets,axis=1)

print('done')


print('fill na')

battersdf_quantiledf = battersdf.quantile(0.5)
battersdf_quantiledf.to_csv('battersdataset_clean_{}_quantile50.csv'.format(CURRENT_DATE))


battersdf_quantiledf = pd.read_csv('battersdataset_clean_{}_quantile50.csv'.format(CURRENT_DATE))
battersdf_quantiledf = battersdf_quantiledf.T.reset_index()
battersdf_quantiledf.columns = battersdf_quantiledf.loc[0]
battersdf_quantiledf = battersdf_quantiledf.drop(0,axis=0).reset_index(drop=True).loc[0]

battersdf = battersdf.fillna(battersdf_quantiledf)

print('done')


### JOIN BATTERS TO LINEUPS


print('join batters to lineups')

query = """
    SELECT * FROM anal_startinglineup;
    """

try:
    cnx = mysql.connector.connect(user='dan', password='',
                                host='127.0.0.1',
                                database='mlb',
                                buffered=False)
    print('Successfully connected to mySQL DB')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
        
curA = cnx.cursor()

query_start = datetime.datetime.now()
print('about to execute query: '+str(query_start))
try:
    curA.execute(query)
except:
    sys.exit()
print('query executed - total time: '+str(datetime.datetime.now()-query_start))
    
cur_columns = curA.column_names

print('about to fetch results')

lineups_data = pd.DataFrame(np.array(curA.fetchall()))
lineups_data.columns = cur_columns

curA.close()
cnx.close()

lineups_data = lineups_data.drop(['game_pk'],axis=1)
print('done!')



def set_MultiIndex(df,indcols):
    tuples = list(zip(df[indcols[0]],df[indcols[1]]))
    gamedateindex = pd.MultiIndex.from_tuples(tuples,names=['anal_game_date','batter'])
    df = df.set_index(gamedateindex)
    df = df.drop([indcols[1]],axis=1)
    return df

def get_battersCols(oldcolumns,side,bat):
    newcols = [col+"_"+side+"_"+str(bat) for col in oldcolumns]
    return newcols
  
batter_columnsets = {}
for side in ['home','away']:
    batter_columnsets[side] = {}
    for bat in range(1,10):
        starttime = datetime.datetime.now()
        indcols = ["game_date",side+"_"+str(bat)+"_id_"]
        lineups_data = set_MultiIndex(lineups_data,indcols)
        oldcols = battersdf.columns
        battersdf.columns = get_battersCols(oldcols,side,bat)
        batter_columnsets[side][bat] =  get_battersCols(oldcols,side,bat)+[side+"_"+str(bat)+"_bats"]
        lineups_data = lineups_data.join(battersdf,how='left')
        battersdf.columns = oldcols
        
        print(side+"_"+str(bat),'complete -',datetime.datetime.now()-starttime)
        
        lineups_data.columns = [col.replace(side+"_"+str(bat)+"_bats","bats_"+side+"_"+str(bat)) for col in list(lineups_data.columns)]
        
print('\nDONE!')

lineups_data.to_csv('tempcompletedataset_{}.csv'.format(CURRENT_DATE))
del lineups_data

print('temp complete file written')
print(datetime.datetime.now())


### EXTRACT PITCHERS DATA

print('extract pitchers data')


pitcher_columnarray = [
    #'anal_game_date',
    #'pitcher',
    'outsmade_pa_std',
    'hits_pa_std',
    'avg_std',
    'obp_std',
    'tba_pa_std',
    'tba_o_std',
    'k_pa_std',
    'klook_pa_std',
    'kswing_pa_std',
    'walk_pa_std',
    'singles_pa_std',
    'doubles_pa_std',
    'triples_pa_std',
    'homeruns_pa_std',
    'groundballs_pa_std',
    'linedrives_pa_std',
    'ball_p_std',
    'ball_pa_std',
    'strike_p_std',
    'strike_pa_std',
    'calledstrike_strike_std',
    'whiffstrike_strike_std',
    'foulstrike_strike_std',
    'inplay_strike_std',
    'calledstrike_pa_std',
    'whiffstrike_pa_std',
    'foulstrike_pa_std',
    'inplay_pa_std',
    'swing_p_std',
    'take_p_std',
    'swing_pa_std',
    'take_pa_std',
    'firstpstrike_pa_std',
    'secondpstrike_pa_std',
    'zoneedge_in2_p_std',
    'zoneedge_in4_p_std',
    'zoneedge_out2_p_std',
    'zoneedge_out4_p_std',
    'zonecorn_in2_p_std',
    'zonecorn_in4_p_std',
    'zonecorn_out2_p_std',
    'zonecorn_out4_p_std',
    'zone_mid3_p_std',
    'zone_mid6_p_std',
    'zone_bigmiss4_p_std',
    'fastball_p_std',
    'fastball_endspeed_p_std',
    'fastball_spinrate_p_std',
    'fastball_spindir_p_std',
    'fastball_pfx_x_p_std',
    'fastball_pfx_z_p_std',
    'fastball_mnorm_p_std',
    'fastball_adjmnorm_p_std',
    'curveball_p_std',
    'curveball_endspeed_p_std',
    'curveball_spinrate_p_std',
    'curveball_spindir_p_std',
    'curveball_pfx_x_p_std',
    'curveball_pfx_z_p_std',
    'curveball_mnorm_p_std',
    'curveball_adjmnorm_p_std',
    'slider_p_std',
    'slider_endspeed_p_std',
    'slider_spinrate_p_std',
    'slider_spindir_p_std',
    'slider_pfx_x_p_std',
    'slider_pfx_z_p_std',
    'slider_mnorm_p_std',
    'slider_adjmnorm_p_std',
    'changeup_p_std',
    'changeup_endspeed_p_std',
    'changeup_spinrate_p_std',
    'changeup_spindir_p_std',
    'changeup_pfx_x_p_std',
    'changeup_pfx_z_p_std',
    'changeup_mnorm_p_std',
    'changeup_adjmnorm_p_std',
    'outsmade_pa_std2',
    'hits_pa_std2',
    'avg_std2',
    'obp_std2',
    'tba_pa_std2',
    'tba_o_std2',
    'k_pa_std2',
    'klook_pa_std2',
    'kswing_pa_std2',
    'walk_pa_std2',
    'singles_pa_std2',
    'doubles_pa_std2',
    'triples_pa_std2',
    'homeruns_pa_std2',
    'groundballs_pa_std2',
    'linedrives_pa_std2',
    'ball_p_std2',
    'ball_pa_std2',
    'strike_p_std2',
    'strike_pa_std2',
    'calledstrike_strike_std2',
    'whiffstrike_strike_std2',
    'foulstrike_strike_std2',
    'inplay_strike_std2',
    'calledstrike_pa_std2',
    'whiffstrike_pa_std2',
    'foulstrike_pa_std2',
    'inplay_pa_std2',
    'swing_p_std2',
    'take_p_std2',
    'swing_pa_std2',
    'take_pa_std2',
    'firstpstrike_pa_std2',
    'secondpstrike_pa_std2',
    'zoneedge_in2_p_std2',
    'zoneedge_in4_p_std2',
    'zoneedge_out2_p_std2',
    'zoneedge_out4_p_std2',
    'zonecorn_in2_p_std2',
    'zonecorn_in4_p_std2',
    'zonecorn_out2_p_std2',
    'zonecorn_out4_p_std2',
    'zone_mid3_p_std2',
    'zone_mid6_p_std2',
    'zone_bigmiss4_p_std2',
    'fastball_p_std2',
    'fastball_endspeed_p_std2',
    'fastball_spinrate_p_std2',
    'fastball_spindir_p_std2',
    'fastball_pfx_x_p_std2',
    'fastball_pfx_z_p_std2',
    'fastball_mnorm_p_std2',
    'fastball_adjmnorm_p_std2',
    'curveball_p_std2',
    'curveball_endspeed_p_std2',
    'curveball_spinrate_p_std2',
    'curveball_spindir_p_std2',
    'curveball_pfx_x_p_std2',
    'curveball_pfx_z_p_std2',
    'curveball_mnorm_p_std2',
    'curveball_adjmnorm_p_std2',
    'slider_p_std2',
    'slider_endspeed_p_std2',
    'slider_spinrate_p_std2',
    'slider_spindir_p_std2',
    'slider_pfx_x_p_std2',
    'slider_pfx_z_p_std2',
    'slider_mnorm_p_std2',
    'slider_adjmnorm_p_std2',
    'changeup_p_std2',
    'changeup_endspeed_p_std2',
    'changeup_spinrate_p_std2',
    'changeup_spindir_p_std2',
    'changeup_pfx_x_p_std2',
    'changeup_pfx_z_p_std2',
    'changeup_mnorm_p_std2',
    'changeup_adjmnorm_p_std2',
    'outsmade_pa_std3',
    'hits_pa_std3',
    'avg_std3',
    'obp_std3',
    'tba_pa_std3',
    'tba_o_std3',
    'k_pa_std3',
    'klook_pa_std3',
    'kswing_pa_std3',
    'walk_pa_std3',
    'singles_pa_std3',
    'doubles_pa_std3',
    'triples_pa_std3',
    'homeruns_pa_std3',
    'groundballs_pa_std3',
    'linedrives_pa_std3',
    'ball_p_std3',
    'ball_pa_std3',
    'strike_p_std3',
    'strike_pa_std3',
    'calledstrike_strike_std3',
    'whiffstrike_strike_std3',
    'foulstrike_strike_std3',
    'inplay_strike_std3',
    'calledstrike_pa_std3',
    'whiffstrike_pa_std3',
    'foulstrike_pa_std3',
    'inplay_pa_std3',
    'swing_p_std3',
    'take_p_std3',
    'swing_pa_std3',
    'take_pa_std3',
    'firstpstrike_pa_std3',
    'secondpstrike_pa_std3',
    'zoneedge_in2_p_std3',
    'zoneedge_in4_p_std3',
    'zoneedge_out2_p_std3',
    'zoneedge_out4_p_std3',
    'zonecorn_in2_p_std3',
    'zonecorn_in4_p_std3',
    'zonecorn_out2_p_std3',
    'zonecorn_out4_p_std3',
    'zone_mid3_p_std3',
    'zone_mid6_p_std3',
    'zone_bigmiss4_p_std3',
    'fastball_p_std3',
    'fastball_endspeed_p_std3',
    'fastball_spinrate_p_std3',
    'fastball_spindir_p_std3',
    'fastball_pfx_x_p_std3',
    'fastball_pfx_z_p_std3',
    'fastball_mnorm_p_std3',
    'fastball_adjmnorm_p_std3',
    'curveball_p_std3',
    'curveball_endspeed_p_std3',
    'curveball_spinrate_p_std3',
    'curveball_spindir_p_std3',
    'curveball_pfx_x_p_std3',
    'curveball_pfx_z_p_std3',
    'curveball_mnorm_p_std3',
    'curveball_adjmnorm_p_std3',
    'slider_p_std3',
    'slider_endspeed_p_std3',
    'slider_spinrate_p_std3',
    'slider_spindir_p_std3',
    'slider_pfx_x_p_std3',
    'slider_pfx_z_p_std3',
    'slider_mnorm_p_std3',
    'slider_adjmnorm_p_std3',
    'changeup_p_std3',
    'changeup_endspeed_p_std3',
    'changeup_spinrate_p_std3',
    'changeup_spindir_p_std3',
    'changeup_pfx_x_p_std3',
    'changeup_pfx_z_p_std3',
    'changeup_mnorm_p_std3',
    'changeup_adjmnorm_p_std3',
    'outsmade_pa_last60',
    'hits_pa_last60',
    'avg_last60',
    'obp_last60',
    'tba_pa_last60',
    'tba_o_last60',
    'k_pa_last60',
    'klook_pa_last60',
    'kswing_pa_last60',
    'walk_pa_last60',
    'singles_pa_last60',
    'doubles_pa_last60',
    'triples_pa_last60',
    'homeruns_pa_last60',
    'groundballs_pa_last60',
    'linedrives_pa_last60',
    'ball_p_last60',
    'ball_pa_last60',
    'strike_p_last60',
    'strike_pa_last60',
    'calledstrike_strike_last60',
    'whiffstrike_strike_last60',
    'foulstrike_strike_last60',
    'inplay_strike_last60',
    'calledstrike_pa_last60',
    'whiffstrike_pa_last60',
    'foulstrike_pa_last60',
    'inplay_pa_last60',
    'swing_p_last60',
    'take_p_last60',
    'swing_pa_last60',
    'take_pa_last60',
    'firstpstrike_pa_last60',
    'secondpstrike_pa_last60',
    'zoneedge_in2_p_last60',
    'zoneedge_in4_p_last60',
    'zoneedge_out2_p_last60',
    'zoneedge_out4_p_last60',
    'zonecorn_in2_p_last60',
    'zonecorn_in4_p_last60',
    'zonecorn_out2_p_last60',
    'zonecorn_out4_p_last60',
    'zone_mid3_p_last60',
    'zone_mid6_p_last60',
    'zone_bigmiss4_p_last60',
    'fastball_p_last60',
    'fastball_endspeed_p_last60',
    'fastball_spinrate_p_last60',
    'fastball_spindir_p_last60',
    'fastball_pfx_x_p_last60',
    'fastball_pfx_z_p_last60',
    'fastball_mnorm_p_last60',
    'fastball_adjmnorm_p_last60',
    'curveball_p_last60',
    'curveball_endspeed_p_last60',
    'curveball_spinrate_p_last60',
    'curveball_spindir_p_last60',
    'curveball_pfx_x_p_last60',
    'curveball_pfx_z_p_last60',
    'curveball_mnorm_p_last60',
    'curveball_adjmnorm_p_last60',
    'slider_p_last60',
    'slider_endspeed_p_last60',
    'slider_spinrate_p_last60',
    'slider_spindir_p_last60',
    'slider_pfx_x_p_last60',
    'slider_pfx_z_p_last60',
    'slider_mnorm_p_last60',
    'slider_adjmnorm_p_last60',
    'changeup_p_last60',
    'changeup_endspeed_p_last60',
    'changeup_spinrate_p_last60',
    'changeup_spindir_p_last60',
    'changeup_pfx_x_p_last60',
    'changeup_pfx_z_p_last60',
    'changeup_mnorm_p_last60',
    'changeup_adjmnorm_p_last60',
    'outsmade_pa_last20',
    'hits_pa_last20',
    'avg_last20',
    'obp_last20',
    'tba_pa_last20',
    'tba_o_last20',
    'k_pa_last20',
    'klook_pa_last20',
    'kswing_pa_last20',
    'walk_pa_last20',
    'singles_pa_last20',
    'doubles_pa_last20',
    'triples_pa_last20',
    'homeruns_pa_last20',
    'groundballs_pa_last20',
    'linedrives_pa_last20',
    'ball_p_last20',
    'ball_pa_last20',
    'strike_p_last20',
    'strike_pa_last20',
    'calledstrike_strike_last20',
    'whiffstrike_strike_last20',
    'foulstrike_strike_last20',
    'inplay_strike_last20',
    'calledstrike_pa_last20',
    'whiffstrike_pa_last20',
    'foulstrike_pa_last20',
    'inplay_pa_last20',
    'swing_p_last20',
    'take_p_last20',
    'swing_pa_last20',
    'take_pa_last20',
    'firstpstrike_pa_last20',
    'secondpstrike_pa_last20',
    'zoneedge_in2_p_last20',
    'zoneedge_in4_p_last20',
    'zoneedge_out2_p_last20',
    'zoneedge_out4_p_last20',
    'zonecorn_in2_p_last20',
    'zonecorn_in4_p_last20',
    'zonecorn_out2_p_last20',
    'zonecorn_out4_p_last20',
    'zone_mid3_p_last20',
    'zone_mid6_p_last20',
    'zone_bigmiss4_p_last20',
    'fastball_p_last20',
    'fastball_endspeed_p_last20',
    'fastball_spinrate_p_last20',
    'fastball_spindir_p_last20',
    'fastball_pfx_x_p_last20',
    'fastball_pfx_z_p_last20',
    'fastball_mnorm_p_last20',
    'fastball_adjmnorm_p_last20',
    'curveball_p_last20',
    'curveball_endspeed_p_last20',
    'curveball_spinrate_p_last20',
    'curveball_spindir_p_last20',
    'curveball_pfx_x_p_last20',
    'curveball_pfx_z_p_last20',
    'curveball_mnorm_p_last20',
    'curveball_adjmnorm_p_last20',
    'slider_p_last20',
    'slider_endspeed_p_last20',
    'slider_spinrate_p_last20',
    'slider_spindir_p_last20',
    'slider_pfx_x_p_last20',
    'slider_pfx_z_p_last20',
    'slider_mnorm_p_last20',
    'slider_adjmnorm_p_last20',
    'changeup_p_last20',
    'changeup_endspeed_p_last20',
    'changeup_spinrate_p_last20',
    'changeup_spindir_p_last20',
    'changeup_pfx_x_p_last20',
    'changeup_pfx_z_p_last20',
    'changeup_mnorm_p_last20',
    'changeup_adjmnorm_p_last20'
    #'createddate',
    #'lastmodifieddate',
    #'autoincrement_PK'
    ]

pitcherpa_columnarray = [
    'pa_last20',
    'pa_last60',
    'pa_std',
    'pa_std2',
    'pa_std3'
]

pitcherlines = ''
for col in pitcher_columnarray:
    pitcherlines += col+','
for col in pitcherpa_columnarray:
    pitcherlines += col+','

query = """SELECT anal_pitcher_rate.anal_game_date, anal_pitcher_rate.pitcher, """+pitcherlines+""" NULL as to_drop 
        FROM anal_pitcher_rate 
        JOIN anal_pitcher_counting_p
        ON anal_pitcher_rate.anal_game_date = anal_pitcher_counting_p.anal_game_date
        AND anal_pitcher_rate.pitcher = anal_pitcher_counting_p.pitcher
        
"""




try:
    cnx = mysql.connector.connect(user='dan', password='',
                                host='127.0.0.1',
                                database='mlb',
                                buffered=False)
    print('Successfully connected to mySQL DB')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
        
curA = cnx.cursor()

#query0 = "SET SESSION optimizer_search_depth = 0;"
#curA.execute(query0)
#cnx.commit()

query_start = datetime.datetime.now()
print('about to execute query: '+str(query_start))
try:
    curA.execute(query)
except:
    sys.exit()
print('query executed - total time: '+str(datetime.datetime.now()-query_start))
    
cur_columns = curA.column_names

print('about to fetch results')

#pitchers_data = np.array(curA.fetchall())
def ResultIter(cursor, arraysize=10):
    # 'An iterator that uses fetchmany to keep memory usage down'
    n = 0
    while True:
        results = cursor.fetchmany(arraysize)
        n += 1
        print(n)
        if not results:
            break
        yield results

pitchers_data = None
chunknum = 0
for result in ResultIter(curA, arraysize=100000):
    chunknum += 1
    pitchers_data = pd.DataFrame(result)
    pitchers_data.columns = cur_columns
    if chunknum == 1:
        pitchers_data.to_csv('pitchersdataset{}.csv'.format(CURRENT_DATE), header=True, mode='w')
    else:
        pitchers_data.to_csv('pitchersdataset{}.csv'.format(CURRENT_DATE), header=False, mode='a')
    del pitchers_data
            
print('results fetched '+str(datetime.datetime.now())+' - total time: '+str(datetime.datetime.now()-query_start))

curA.close()
cnx.close()


print('clean pitchers dataset')

df = pd.read_csv('pitchersdataset{}.csv'.format(CURRENT_DATE))

tuples = list(zip(df["anal_game_date"].apply(datetime.datetime.strptime,args=(["%Y-%m-%d"])),df["pitcher"]))
gamedateindex = pd.MultiIndex.from_tuples(tuples,names=['anal_game_date','pitcher'])
df = df.set_index(gamedateindex)
df = df.drop(['anal_game_date','pitcher'],axis=1)
df = df.drop(['Unnamed: 0','to_drop'],axis=1)

buckets = [
    'last20',
    'last60',
    'std',
    'std2',
    'std3'
]



target_buckets = np.flip(np.array(buckets),0)
parent_buckets = np.array([None] + list(target_buckets[:-1]))


def rollup_withthresh(df,target_col,thresh_col,thresh_min,parent_col):
    if df[thresh_col] < thresh_min:
        if parent_col is None:
            return None
        else:
            return df[parent_col]
    else:
        return df[target_col]
    

        


cols = [
    'outsmade_pa',
    'hits_pa',
    'avg',
    'obp',
    'tba_pa',
    'tba_o',
    'k_pa',
    'klook_pa',
    'kswing_pa',
    'walk_pa',
    'singles_pa',
    'doubles_pa',
    'triples_pa',
    'homeruns_pa',
    'groundballs_pa',
    'linedrives_pa',
    'ball_p',
    'ball_pa',
    'strike_p',
    'strike_pa',
    'calledstrike_strike',
    'whiffstrike_strike',
    'foulstrike_strike',
    'inplay_strike',
    'calledstrike_pa',
    'whiffstrike_pa',
    'foulstrike_pa',
    'inplay_pa',
    'swing_p',
    'take_p',
    'swing_pa',
    'take_pa',
    'firstpstrike_pa',
    'secondpstrike_pa',
    'zoneedge_in2_p',
    'zoneedge_in4_p',
    'zoneedge_out2_p',
    'zoneedge_out4_p',
    'zonecorn_in2_p',
    'zonecorn_in4_p',
    'zonecorn_out2_p',
    'zonecorn_out4_p',
    'zone_mid3_p',
    'zone_mid6_p',
    'zone_bigmiss4_p',
    'fastball_p',
    'fastball_endspeed_p',
    'fastball_spinrate_p',
    'fastball_spindir_p',
    'fastball_pfx_x_p',
    'fastball_pfx_z_p',
    'fastball_mnorm_p',
    'fastball_adjmnorm_p',
    'curveball_p',
    'curveball_endspeed_p',
    'curveball_spinrate_p',
    'curveball_spindir_p',
    'curveball_pfx_x_p',
    'curveball_pfx_z_p',
    'curveball_mnorm_p',
    'curveball_adjmnorm_p',
    'slider_p',
    'slider_endspeed_p',
    'slider_spinrate_p',
    'slider_spindir_p',
    'slider_pfx_x_p',
    'slider_pfx_z_p',
    'slider_mnorm_p',
    'slider_adjmnorm_p',
    'changeup_p',
    'changeup_endspeed_p',
    'changeup_spinrate_p',
    'changeup_spindir_p',
    'changeup_pfx_x_p',
    'changeup_pfx_z_p',
    'changeup_mnorm_p',
    'changeup_adjmnorm_p',
]

buckets = [
    'last20',
    'last60',
    'std',
    'std2',
    'std3'
]



target_buckets = np.flip(np.array(buckets),0)
parent_buckets = np.array([None] + list(target_buckets[:-1]))
thresh_mins = {  
                'last20':40,
                'last60':70,
                'std':90,
                'std2':90,
                'std3':90
}

for target_bucket, parent_bucket in zip(target_buckets, parent_buckets):
    
    print(target_bucket)
    starttime = datetime.datetime.now()

    target_list = []
    parent_list = []
    for col in cols:
        target_list.append(col+'_'+target_bucket)
        if parent_bucket is None:
            parent_list.append(None)
        else:
            parent_list.append(col+'_'+parent_bucket)
    thresh_min = thresh_mins[target_bucket]
    for target_col, parent_col in zip(target_list,parent_list):
        df[target_col] = df.apply(rollup_withthresh, axis=1, args=(target_col, 'pa_'+target_bucket, thresh_min, parent_col)) 
    
    print('time elapsed:', datetime.datetime.now()-starttime)
        
df.to_csv('pitchersdataset_clean_{}.csv'.format(CURRENT_DATE), header=True, mode='w')
del df



buckets = [
    'last20',
    'last60',
    'std',
    'std2',
    'std3'
]

pa_buckets = ['pa_'+b for b in buckets]
pa_buckets


pitchersdf = pd.read_csv('pitchersdataset_clean_{}.csv'.format(CURRENT_DATE))

tuples = list(zip(pitchersdf["anal_game_date"].apply(datetime.datetime.strptime,args=(["%Y-%m-%d"])),pitchersdf["pitcher"]))
gamedateindex = pd.MultiIndex.from_tuples(tuples,names=['anal_game_date','pitcher'])
pitchersdf = pitchersdf.set_index(gamedateindex)
pitchersdf = pitchersdf.drop(['anal_game_date','pitcher'],axis=1)
pitchersdf = pitchersdf.drop(pa_buckets,axis=1)

print('done')


pitchersdf_quantiledf = pitchersdf.quantile(0.5)
pitchersdf_quantiledf.to_csv('pitchersdataset_clean_{}_quantile50.csv'.format(CURRENT_DATE))



pitchersdf_quantiledf = pd.read_csv('pitchersdataset_clean_{}_quantile50.csv'.format(CURRENT_DATE))
pitchersdf_quantiledf = pitchersdf_quantiledf.T.reset_index()
pitchersdf_quantiledf.columns = pitchersdf_quantiledf.loc[0]
pitchersdf_quantiledf = pitchersdf_quantiledf.drop(0,axis=0).reset_index(drop=True).loc[0]

pitchersdf = pitchersdf.fillna(pitchersdf_quantiledf)

print('done')


lineups_data = pd.read_csv('tempcompletedataset_{}'.format(CURRENT_DATE))


print('join pitchers to lineups')


def set_MultiIndex(df,indcols):
    tuples = list(zip(df[indcols[0]],df[indcols[1]]))
    gamedateindex = pd.MultiIndex.from_tuples(tuples,names=['anal_game_date','pitcher'])
    df = df.set_index(gamedateindex)
    df = df.drop([indcols[1]],axis=1)
    return df

def get_pitchersCols(oldcolumns,side):
    newcols = [col+"_"+side+"_sp" for col in oldcolumns]
    return newcols
  
pitcher_columnsets = {}
for side in ['home','away']:
    starttime = datetime.datetime.now()
    indcols = ["game_date",side+"_sp_id_"]
    lineups_data = set_MultiIndex(lineups_data,indcols)
    oldcols = pitchersdf.columns
    pitchersdf.columns = get_pitchersCols(oldcols,side)
    pitcher_columnsets[side] =  get_pitchersCols(oldcols,side)+["rl_"+side+"_sp"]
    lineups_data = lineups_data.join(pitchersdf,how='left')
    pitchersdf.columns = oldcols
        
    print(side,'complete -',datetime.datetime.now()-starttime)
        
lineups_data.columns = [col.replace('home_sp_rl','rl_home_sp') for col in list(lineups_data.columns)]
lineups_data.columns = [col.replace('away_sp_rl','rl_away_sp') for col in list(lineups_data.columns)]

print('\nDONE!')



lineups_data.to_csv('tempcompletedataset_{}.csv'.format(CURRENT_DATE))
print('temp complete written to file')



print('export team and general features, and targets')

team_columnarray_off = [
        
    'outsmade_pa_std_off',
    'hits_pa_std_off',
    'obp_std_off',
    'tba_pa_std_off',
    'tba_o_std_off',
    'runsscored_pa_std_off',
    'outsmade_pa_std_sp_off',
    'hits_pa_std_sp_off',
    'obp_std_sp_off',
    'tba_pa_std_sp_off',
    'tba_o_std_sp_off',
    'runsscored_pa_std_sp_off',
    'outsmade_pa_std_rp_off',
    'hits_pa_std_rp_off',
    'obp_std_rp_off',
    'tba_pa_std_rp_off',
    'tba_o_std_rp_off',
    'runsscored_pa_std_rp_off',
    'outsmade_pa_std_home_off',
    'hits_pa_std_home_off',
    'obp_std_home_off',
    'tba_pa_std_home_off',
    'tba_o_std_home_off',
    'runsscored_pa_std_home_off',
    'outsmade_pa_std_away_off',
    'hits_pa_std_away_off',
    'obp_std_away_off',
    'tba_pa_std_away_off',
    'tba_o_std_away_off',
    'runsscored_pa_std_away_off',
    'outsmade_pa_last60_off',
    'hits_pa_last60_off',
    'obp_last60_off',
    'tba_pa_last60_off',
    'tba_o_last60_off',
    'runsscored_pa_last60_off',
    'outsmade_pa_last30_off',
    'hits_pa_last30_off',
    'obp_last30_off',
    'tba_pa_last30_off',
    'tba_o_last30_off',
    'runsscored_pa_last30_off',
    'outsmade_pa_last10_off',
    'hits_pa_last10_off',
    'obp_last10_off',
    'tba_pa_last10_off',
    'tba_o_last10_off',
    'runsscored_pa_last10_off',
    'outsmade_pa_last60_rp_off',
    'hits_pa_last60_rp_off',
    'obp_last60_rp_off',
    'tba_pa_last60_rp_off',
    'tba_o_last60_rp_off',
    'runsscored_pa_last60_rp_off',
    'outsmade_pa_last60_sp_off',
    'hits_pa_last60_sp_off',
    'obp_last60_sp_off',
    'tba_pa_last60_sp_off',
    'tba_o_last60_sp_off',
    'runsscored_pa_last60_sp_off',
    'outsmade_pa_last30_sp_off',
    'hits_pa_last30_sp_off',
    'obp_last30_sp_off',
    'tba_pa_last30_sp_off',
    'tba_o_last30_sp_off',
    'runsscored_pa_last30_sp_off',
    'outsmade_pa_last30_rp_off',
    'hits_pa_last30_rp_off',
    'obp_last30_rp_off',
    'tba_pa_last30_rp_off',
    'tba_o_last30_rp_off',
    'runsscored_pa_last30_rp_off',
    'outsmade_pa_last10_sp_off',
    'hits_pa_last10_sp_off',
    'obp_last10_sp_off',
    'tba_pa_last10_sp_off',
    'tba_o_last10_sp_off',
    'runsscored_pa_last10_sp_off',
    'outsmade_pa_last10_rp_off',
    'hits_pa_last10_rp_off',
    'obp_last10_rp_off',
    'tba_pa_last10_rp_off',
    'tba_o_last10_rp_off',
    'runsscored_pa_last10_rp_off'
    ]
  
team_columnarray_def = [
    
    'outsmade_pa_std_def',
    'hits_pa_std_def',
    'obp_std_def',
    'tba_pa_std_def',
    'tba_o_std_def',
    'runsscored_pa_std_def',
    'outsmade_pa_std_sp_def',
    'hits_pa_std_sp_def',
    'obp_std_sp_def',
    'tba_pa_std_sp_def',
    'tba_o_std_sp_def',
    'runsscored_pa_std_sp_def',
    'outsmade_pa_std_rp_def',
    'hits_pa_std_rp_def',
    'obp_std_rp_def',
    'tba_pa_std_rp_def',
    'tba_o_std_rp_def',
    'runsscored_pa_std_rp_def',
    'outsmade_pa_std_home_def',
    'hits_pa_std_home_def',
    'obp_std_home_def',
    'tba_pa_std_home_def',
    'tba_o_std_home_def',
    'runsscored_pa_std_home_def',
    'outsmade_pa_std_away_def',
    'hits_pa_std_away_def',
    'obp_std_away_def',
    'tba_pa_std_away_def',
    'tba_o_std_away_def',
    'runsscored_pa_std_away_def',
    'outsmade_pa_last60_def',
    'hits_pa_last60_def',
    'obp_last60_def',
    'tba_pa_last60_def',
    'tba_o_last60_def',
    'runsscored_pa_last60_def',
    'outsmade_pa_last30_def',
    'hits_pa_last30_def',
    'obp_last30_def',
    'tba_pa_last30_def',
    'tba_o_last30_def',
    'runsscored_pa_last30_def',
    'outsmade_pa_last10_def',
    'hits_pa_last10_def',
    'obp_last10_def',
    'tba_pa_last10_def',
    'tba_o_last10_def',
    'runsscored_pa_last10_def',
    'outsmade_pa_last60_sp_def',
    'hits_pa_last60_sp_def',
    'obp_last60_sp_def',
    'tba_pa_last60_sp_def',
    'tba_o_last60_sp_def',
    'runsscored_pa_last60_sp_def',
    'outsmade_pa_last60_rp_def',
    'hits_pa_last60_rp_def',
    'obp_last60_rp_def',
    'tba_pa_last60_rp_def',
    'tba_o_last60_rp_def',
    'runsscored_pa_last60_rp_def',
    'outsmade_pa_last30_sp_def',
    'hits_pa_last30_sp_def',
    'obp_last30_sp_def',
    'tba_pa_last30_sp_def',
    'tba_o_last30_sp_def',
    'runsscored_pa_last30_sp_def',
    'outsmade_pa_last30_rp_def',
    'hits_pa_last30_rp_def',
    'obp_last30_rp_def',
    'tba_pa_last30_rp_def',
    'tba_o_last30_rp_def',
    'runsscored_pa_last30_rp_def',
    'outsmade_pa_last10_sp_def',
    'hits_pa_last10_sp_def',
    'obp_last10_sp_def',
    'tba_pa_last10_sp_def',
    'tba_o_last10_sp_def',
    'runsscored_pa_last10_sp_def',
    'outsmade_pa_last10_rp_def',
    'hits_pa_last10_rp_def',
    'obp_last10_rp_def',
    'tba_pa_last10_rp_def',
    'tba_o_last10_rp_def',
    'runsscored_pa_last10_rp_def'
    ]

sides = ['home','away']

teams = ''
teams_joins = ''

starttime = datetime.datetime.now()
print('SCRIPTSTART',starttime)

team_columnsets = {}

for side in sides:
        team_columnsets[side] = {}
        team_columnsets[side]['off'] = []
        team_columnsets[side]['def'] = []
        
        teams_joins += "LEFT JOIN anal_team_rate AS anal_team_rate_"+side+" ON anal_team_rate_"+side+".anal_game_date = g.game_date AND anal_team_rate_"+side+".team_id = g."+side+"_id "
        
        for col in team_columnarray_off + team_columnarray_def:
            line = "anal_team_rate_"+side+"."+col+" as "+col+"_"+side+"_team,"
            teams += line
            
        for col in team_columnarray_off:
            team_columnsets[side]['off'].append(col+"_"+side+"_team")
        for col in team_columnarray_def:
            team_columnsets[side]['def'].append(col+"_"+side+"_team")
            

#print(teams)
#print(teams_joins)
print('SCRIPTEND',datetime.datetime.now()-starttime)


query = """SELECT 
        g.gid,
        betting_results.homevsspread,
        betting_results.awayvsspread,
        betting_results.spread_winner,
        betting_results.spread_home_oddsrat,
        betting_results.spread_away_oddsrat,
        betting_results.money_winner,
        betting_results.money_home_oddsrat,
        betting_results.money_away_oddsrat,
        betting_results.total_winner,
        betting_results.total_over_oddsrat,
        betting_results.total_under_oddsrat,
        betting_results.spread_home_points,
        betting_results.spread_away_points,
        betting_results.total_points,
        g.game_date,
        TIME_TO_SEC(g.home_time) as home_time,
        g.home_timezone,
        g.away_timedif,
        g.stad_id,
        home_w/(home_w+home_l) as home_wp, 
        away_w/(away_w+away_l) as away_wp,
        
        forecastio_weather.temperature,
        forecastio_weather.apparentTemperature,
        forecastio_weather.humidity,
        forecastio_weather.pressure,
        forecastio_weather.dewPoint,
        forecastio_weather.cloudCover,
        forecastio_weather.uvIndex,
        forecastio_weather.precipIntensity,
        forecastio_weather.precipProbability,
        forecastio_weather.windVector_ns,
        forecastio_weather.windVector_ew,
        forecastio_weather.precipType_rain,
        forecastio_weather.precipType_snow,
        forecastio_weather.precipType_sleet,
        
        """+teams+"""
        
        NULL AS to_drop FROM anal_game AS g """+teams_joins+""" JOIN anal_startinglineup ON anal_startinglineup.game_date = g.game_date AND anal_startinglineup.gid = g.gid JOIN betting_results ON betting_results.gid = g.gid JOIN forecastio_weather ON forecastio_weather.gid = g.gid WHERE game_type = 'R' AND betting_results.book_id = 238"""          

#print(query)



import mysql.connector
import sys
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

try:
    cnx = mysql.connector.connect(user='dan', password='',
                                host='127.0.0.1',
                                database='mlb')
    print('Successfully connected to mySQL DB')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
        
curA = cnx.cursor()

query0 = "SET SESSION optimizer_search_depth = 0;"
curA.execute(query0)
cnx.commit()

query_start = datetime.datetime.now()
print('about to execute query: '+str(query_start))
try:
    curA.execute(query)
except:
    sys.exit()
    
cur_columns = curA.column_names

rawdata = np.array(curA.fetchall())
print('results fetched '+str(datetime.datetime.now())+' - total time: '+str(datetime.datetime.now()-query_start))
rawdata = pd.DataFrame(rawdata)
rawdata.columns = cur_columns

curA.close()
cnx.close()

rawdata.to_csv('partialdataset{}.csv'.format(CURRENT_DATE))

print('Success')



df = rawdata

df = df.set_index("gid",drop=False)
df = df.loc[~df.index.duplicated(keep='last')]

tuples = list(zip(df["game_date"],df["gid"]))
gamedateindex = pd.MultiIndex.from_tuples(tuples,names=['game_date','gid'])
df = df.set_index(gamedateindex,drop=True)
df = df.sort_index()

df = df.drop(["gid","game_date","to_drop"],axis=1)


df = lineups_data.join(df,how='inner')
df = df.drop(["pitcher","batter","anal_game_date.1","gid","game_date"],axis=1)

df.to_csv('completedataset{}.csv'.format(CURRENT_DATE))



print('New dataset finished')