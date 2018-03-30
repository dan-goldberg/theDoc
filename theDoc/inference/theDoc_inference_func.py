
# coding: utf-8

# In[9]:

import mysql.connector
import sys
import os
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn import preprocessing
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from theDoc.preprocessing import theDoc_dataset
from theDoc.database import mlb_analtablesupdate as mlbtab
from theDoc.models.theDoc_models import DocModels
from theDoc.utils import emailSend
from theDoc import settings

def run_inference(inference_gids_str):
    
    print("inference running on: {}".format(inference_gids_str))

    qry = theDoc_dataset.query(inference_gids_str=inference_gids_str)

    columnlist = {}
    columnlist["home"] = {}
    columnlist["away"] = {}

    for side in qry.sides:

        pitchers = []
        for col in qry.pitcher_columnarray + qry.pitcherhands_columnarray:
            line = col+"_"+side+"_sp"
            pitchers.append(line)

        columnlist[side]["sp"] = pitchers

        batters = []
        for batpos in qry.batpositions:
            for col in qry.batter_columnarray + qry.batterhands_columnarray:
                line = col+"_"+side+"_"+batpos
                batters.append(line)

        columnlist[side]["batters"] = batters

        teams = []
        for col in qry.team_columnarray_off:
            line = col+"_"+side+"_team"
            teams.append(line)

        columnlist[side]["teams_off"] = teams

        teams = []
        for col in qry.team_columnarray_def:
            line = col+"_"+side+"_team"
            teams.append(line)

        columnlist[side]["teams_def"] = teams

    columnlist["general"] = [
        "home_time",
        "home_timezone",
        "away_timedif",
        "stad_id",
        "home_wp",
        "away_wp",
        "temperature",
        "apparentTemperature",
        "humidity",
        "pressure",
        "dewPoint",
        "cloudCover",
        "uvIndex",
        "precipIntensity",
        "precipProbability",
        "windVector_ns",
        "windVector_ew",
        "precipType_rain",
        "precipType_snow",
        "precipType_sleet"

    ]

    cnx = mlbtab.mlb_connect()

    curA = cnx.cursor()

    query0 = "SET SESSION optimizer_search_depth = 0;"
    curA.execute(query0)
    cnx.commit()

    query_start = datetime.datetime.now()
    print('about to execute query: '+str(query_start))
    try:
        curA.execute(qry.inference_query)
    except:
        sys.exit()

    cur_columns = curA.column_names
    if len(cur_columns) == 1:
        print('ERROR - only 1 column. Exiting inference function')
        pass

    rawdata = np.array(curA.fetchall())
    print('results fetched '+str(datetime.datetime.now())+' - total time: '+str(datetime.datetime.now()-query_start))
    rawdata = pd.DataFrame(rawdata)
    rawdata.columns = cur_columns

    curA.close()
    cnx.close()

    filename = '{}/inference{}.csv'.format(settings.DATASETS_PATH, str(datetime.datetime.now().date()))
    rawdata.to_csv(filename)
    del rawdata

    df = pd.read_csv(filename)
    os.remove(filename)

    df = df.set_index("gid",drop=False)
    df = df.loc[~df.index.duplicated(keep='last')]

    tuples = list(zip(df["game_date"].apply(datetime.datetime.strptime,args=(["%Y-%m-%d"])),df["gid"]))
    target_gamedateindex = pd.MultiIndex.from_tuples(tuples,names=['game_date','gid'])
    df = df.set_index(target_gamedateindex)

    #df = df.drop(["gid","game_date","to_drop"],axis=1)
    df = df.drop(["to_drop"],axis=1)
    df = df.drop("Unnamed: 0",axis=1)
    df = df.drop(['game_date','gid'],axis=1)

    #df = df[:datetime.date(2016,10,2)]
    df = df.reset_index(drop=True)

    #df = df.merge(gameresults,how='left',on='gid')
    #df = df.drop(["gid","game_date_x","game_date_y"],axis=1)
    #df = df.dropna(axis=0,subset=["home_score","away_score"])

    df['is_target'] = 1




    temp_training_dataset = pd.read_csv('{}/completedataset20170620.csv'.format(settings.DATASETS_PATH))

    temp_training_dataset = temp_training_dataset.set_index("gid",drop=False)
    temp_training_dataset = temp_training_dataset.loc[~temp_training_dataset.index.duplicated(keep='last')]

    tuples = list(zip(temp_training_dataset["game_date"].apply(datetime.datetime.strptime,args=(["%Y-%m-%d"])),temp_training_dataset["gid"]))
    gamedateindex = pd.MultiIndex.from_tuples(tuples,names=['game_date','gid'])
    temp_training_dataset = temp_training_dataset.set_index(gamedateindex)
    temp_training_dataset = temp_training_dataset.sort_index()

    #df = df.drop(["gid","game_date","to_drop"],axis=1)
    temp_training_dataset = temp_training_dataset.drop(["to_drop"],axis=1)
    temp_training_dataset = temp_training_dataset.drop("Unnamed: 0",axis=1)
    temp_training_dataset = temp_training_dataset.drop(['game_date','gid'],axis=1)

    #df = df[:datetime.date(2016,10,2)]
    temp_training_dataset = temp_training_dataset.reset_index(drop=True)

    temp_training_dataset['is_target'] = 0



    training_outcomes = [
        'homevsspread',
        'awayvsspread',
        'spread_winner',
        'money_winner',
        'total_winner'
        ]

    outcome0_money = "money_winner_home"
    outcome1_money = "money_winner_away"

    inference_outcomes = [
        'spread_home_oddsrat',
        'spread_away_oddsrat', 
        'money_home_oddsrat',
        'money_away_oddsrat',  
        'total_over_oddsrat',
        'total_under_oddsrat', 
        'spread_away_points',
        'spread_home_points',
        'total_points',
        ]

    categoricals_general = [
        'home_timezone',
        'stad_id',
        "precipType_rain",
        "precipType_snow",
        "precipType_sleet"
        ]
    columnlist["general"] = list(set(columnlist["general"])-set(categoricals_general))
    categoricals_home_batters = [
        'bats_home_1',
        'bats_home_2',
        'bats_home_3',
        'bats_home_4',
        'bats_home_5',
        'bats_home_6',
        'bats_home_7',
        'bats_home_8',
        'bats_home_9'
        ]
    columnlist["home"]["batters"] = list(set(columnlist["home"]["batters"])-set(categoricals_home_batters))
    categoricals_away_batters = [
        'bats_away_1',
        'bats_away_2',
        'bats_away_3',
        'bats_away_4',
        'bats_away_5',
        'bats_away_6',
        'bats_away_7',
        'bats_away_8',
        'bats_away_9'
        ]
    columnlist["away"]["batters"] = list(set(columnlist["away"]["batters"])-set(categoricals_away_batters))
    categoricals_home_sp = [
        'rl_home_sp'
        ]
    columnlist["home"]["sp"] = list(set(columnlist["home"]["sp"])-set(categoricals_home_sp))
    categoricals_away_sp = [
        'rl_away_sp'
        ]
    columnlist["away"]["sp"] = list(set(columnlist["away"]["sp"])-set(categoricals_away_sp))

    categoricals = categoricals_general+categoricals_home_batters+categoricals_away_batters+categoricals_home_sp+categoricals_away_sp


    #Separate Target Vars

    targets = df[inference_outcomes]
    targets_cols = targets.columns

    df_cov = df.drop(inference_outcomes,axis=1)
    temp_training_dataset = temp_training_dataset.drop(inference_outcomes+training_outcomes,axis=1)

    #Separate Cont and Cat Vars

    cat_df = df_cov[categoricals+['is_target']]
    cat_temp_training_dataset = temp_training_dataset[categoricals+['is_target']]

    df_cont = df_cov.drop(categoricals,axis=1)
    df_cont_cols = df_cont.columns
    cont_temp_training_dataset = temp_training_dataset.drop(categoricals,axis=1)

    del temp_training_dataset

    #Encoding Categoricals

    encoding_set = pd.concat([cat_df,cat_temp_training_dataset],axis=0)

    columnlist["general"] = columnlist["general"] + list(pd.get_dummies(encoding_set[categoricals_general],columns=categoricals_general).columns)
    columnlist["home"]["batters"] = columnlist["home"]["batters"] + list(pd.get_dummies(encoding_set[categoricals_home_batters],columns=categoricals_home_batters).columns)
    columnlist["away"]["batters"] = columnlist["away"]["batters"] + list(pd.get_dummies(encoding_set[categoricals_away_batters],columns=categoricals_away_batters).columns)
    columnlist["home"]["sp"] = columnlist["home"]["sp"] + list(pd.get_dummies(encoding_set[categoricals_home_sp],columns=categoricals_home_sp).columns)
    columnlist["away"]["sp"] = columnlist["away"]["sp"] + list(pd.get_dummies(encoding_set[categoricals_away_sp],columns=categoricals_away_sp).columns)

    encoding_set = pd.get_dummies(encoding_set,columns=encoding_set[categoricals])
    cat_df = encoding_set[encoding_set['is_target'] == 1]
    cat_temp_training_dataset = encoding_set[encoding_set['is_target'] == 0] 

    cat_df_cols = cat_df.columns

    #Impute Missing Values - Categorical

    imp_cat = preprocessing.Imputer(missing_values='NaN', strategy='most_frequent', axis=0)
    imp_cat.fit(encoding_set)
    cat_df = imp_cat.transform(cat_df)
    cat_temp_training_dataset = imp_cat.transform(cat_temp_training_dataset)

    cat_arr = np.array(cat_df).T
    cat_temp_training_arr = np.array(cat_temp_training_dataset).T

    del cat_df
    del cat_temp_training_dataset

    #Impute Missing Values - Continuous

    imp_cont = preprocessing.Imputer(missing_values='NaN', strategy='mean', axis=0)
    imp_cont.fit(cont_temp_training_dataset)
    df_cont = imp_cont.transform(df_cont)
    cont_temp_training_dataset = imp_cont.transform(cont_temp_training_dataset)

    cont_arr = np.array(df_cont).T
    cont_temp_training_arr = np.array(cont_temp_training_dataset).T

    del df_cont
    del cont_temp_training_dataset


    #Combine Dummied Categoricals, Continuous Vars

    all_columns = np.concatenate([df_cont_cols,cat_df_cols],axis=0)

    covariates = np.concatenate((cont_arr,cat_arr)).T
    temp_training_covariates = np.concatenate((cont_temp_training_arr,cat_temp_training_arr)).T
    del cont_arr
    del cat_arr
    del cont_temp_training_arr
    del cat_temp_training_arr


    #Stardardize Cont Inference Set Using Training Set

    standardizer = preprocessing.StandardScaler()
    standardizer.fit(temp_training_covariates)
    del temp_training_covariates

    covariates = standardizer.transform(covariates)

    #Put Inference Covariates back into DataFrame

    covariates = pd.DataFrame(covariates)
    covariates.columns = all_columns
    covariates = covariates.drop('is_target',axis=1)




    ho_cols = (
        columnlist["general"] + 
        columnlist["home"]["batters"] +
        columnlist["home"]["teams_off"] +
        columnlist["away"]["sp"] +
        columnlist["away"]["teams_def"]
               )

    ao_cols = (
        columnlist["general"] + 
        columnlist["away"]["batters"] +
        columnlist["away"]["teams_off"] +
        columnlist["home"]["sp"] +
        columnlist["home"]["teams_def"]
               )

    X_ho = covariates[ho_cols]
    X_ao = covariates[ao_cols]
    X_ho["ishome"],X_ho["isaway"], X_ao["ishome"],X_ao["isaway"] = 1,0,0,1

    X_data = {'X_ho':np.array(X_ho),'X_ao':np.array(X_ao)}


    outcome_odds0_money = "money_home_oddsrat"
    outcome_odds1_money = "money_away_oddsrat"

    outcome_odds0_spread = "spread_home_oddsrat"
    outcome_odds1_spread = "spread_away_oddsrat"
    outcome0_points_spread = "spread_home_points"
    outcome1_points_spread = "spread_away_points"

    outcome_odds0_total = "total_over_oddsrat"
    outcome_odds1_total = "total_under_oddsrat"
    outcome_points_total = "total_points"

    odds_money = targets[[outcome_odds0_money,outcome_odds1_money]]

    points_spread = targets[[outcome0_points_spread,outcome1_points_spread]]
    odds_spread = targets[[outcome_odds0_spread,outcome_odds1_spread]]

    points_total = targets[[outcome_points_total]]
    odds_total = targets[[outcome_odds0_total,outcome_odds1_total]]



    task_ensembles = {'money':None,'total':None,'spread':None}

    class ensemble:

        def __init__(self,mod,trainloss=0,valloss=0,testloss=0,n=1):
            self.mean = mod
            self.trainloss = trainloss
            self.valloss = valloss
            self.testloss = testloss

            #self.moddic = {}
            #self.moddic[n] = mod

            self.num_mods = n

            #self.pred_var = mod*0

        def add_model(self,mod,n=1):
            self.mean = ((self.mean*self.num_mods) + mod*n)/(self.num_mods+n)
            #self.moddic[self.num_mods+1] = mod
            self.num_mods += n
            #sqdif = self.moddic[1]*0
            #for n in self.moddic:
            #    sqdif += (self.moddic[n]-self.mean)**2
            #self.pred_var = sqdif = sqdif/self.num_mods

        def add_losses(self,trainloss=0,valloss=0,testloss=0,n=1):
            self.trainloss = ((self.trainloss*self.num_mods) + trainloss*n)/(self.num_mods+n)
            self.valloss = ((self.valloss*self.num_mods) + valloss*n)/(self.num_mods+n)
            self.testloss = ((self.testloss*self.num_mods) + testloss*n)/(self.num_mods+n)

        def newarrayconcat(a,b):
            if a is None: a = b
            else: a = np.concatenate([a,b],axis=0)
            return a


    docModels = DocModels()
    docModels.build_allsavedmodels()



    def inference(model,X_data,task_ensembles):

        mod = model['model']
        mod.load_weights(model['weights_path'])

        probas = mod.predict({'ho_input':X_data['X_ho'],'ao_input':X_data['X_ao']}, batch_size=256, verbose=0)
        pred_dummyhomescore_df = pd.DataFrame(probas[0])
        pred_dummyhomescore_df.columns = qry.dummyhomescore_cols
        pred_dummyawayscore_df = pd.DataFrame(probas[1])
        pred_dummyawayscore_df.columns = qry.dummyawayscore_cols
        pred_dummyscore_df = pd.DataFrame(probas[2])
        pred_dummyscore_df.columns = qry.dummyscore_cols
        pred_dummytots_df = pd.DataFrame(probas[3])
        pred_dummytots_df.columns = qry.dummytots_cols
        pred_winner_df = pd.DataFrame(probas[4])
        pred_winner_df.columns = [outcome0_money,outcome1_money]
        probas_df = pred_dummyhomescore_df.join(pred_dummyawayscore_df).join(pred_dummyscore_df).join(pred_dummytots_df).join(pred_winner_df)

        # Merge individual model probabilities into ensemble dataframe

        if 'spread' in model['tasks']:
            if task_ensembles['spread'] is None: task_ensembles['spread'] = ensemble(pred_dummyscore_df)
            else: 
                task_ensembles['spread'].add_model(pred_dummyscore_df)

        if 'total' in model['tasks']:
            if task_ensembles['total'] is None: task_ensembles['total'] = ensemble(pred_dummytots_df)
            else: 
                task_ensembles['total'].add_model(pred_dummytots_df)

        if 'money' in model['tasks']:
            if task_ensembles['money'] is None: task_ensembles['money'] = ensemble(pred_winner_df)
            else: 
                task_ensembles['money'].add_model(pred_winner_df)

    for key in docModels.saved_models:
        inference(docModels.saved_models[key],X_data,task_ensembles)






    def cumprob_above(df,valuecol,dummylist):   #REMEMBER TO TAKE THE HOME SPREAD IF SPREAD AND IF DUMMY SCORES ARE HOMESCORE-AWAYSCORE
        colsinterest = []
        for n in dummylist:
            if n + df[valuecol] > 0:
                colsinterest.append(n)
        return np.sum(df[colsinterest])

    def cumprob_below(df,valuecol,dummylist):   #REMEMBER TO TAKE THE AWAY SPREAD IF SPREAD AND IF DUMMY SCORES ARE HOMESCORE-AWAYSCORE
                                                #ALSO IS THE TOTAL AS UNDER IN THIS CASE
        colsinterest = []
        for n in dummylist:
            if n - df[valuecol] < 0:
                colsinterest.append(n)
        return np.sum(df[colsinterest])

    def cumprob_over(df,valuecol,dummylist):   #REMEMBER TO TAKE THE TOTAL AS THE OVER IN THIS CASE
        colsinterest = []
        for n in dummylist:
            if n - df[valuecol] > 0:
                colsinterest.append(n)
        return np.sum(df[colsinterest])


    spread_df = task_ensembles['spread'].mean
    spread_df.columns = qry.dummyscores
    points_spread.columns, odds_spread.columns = ["points0","points1"], ["odds0","odds1"]
    spread_df = spread_df.join(points_spread).join(odds_spread)
    spread_df["probabilities0"] = spread_df.apply(cumprob_above,axis=1,valuecol="points0",dummylist=qry.dummyscores)
    spread_df["probabilities1"] = spread_df.apply(cumprob_below,axis=1,valuecol="points1",dummylist=qry.dummyscores)

    total_df = task_ensembles['total'].mean
    total_df.columns = qry.dummytots
    points_total.columns, odds_total.columns = ["points"], ["odds0","odds1"]
    total_df = total_df.join(points_total).join(odds_total)
    total_df["probabilities0"] = total_df.apply(cumprob_over,axis=1,valuecol="points",dummylist=qry.dummytots)
    total_df["probabilities1"] = total_df.apply(cumprob_below,axis=1,valuecol="points",dummylist=qry.dummytots)
    total_df["partition"] = total_df["probabilities0"]+total_df["probabilities1"]
    
    def totals_normalize_probs(df,col):
        if df["points"] % 1 == 0:
            return df[col]/df["partition"]
        else:
            return df[col]
    total_df["probabilities0"] = total_df.apply(totals_normalize_probs, axis=1, col="probabilities0")
    total_df["probabilities1"] = total_df.apply(totals_normalize_probs, axis=1, col="probabilities1")
    total_df = total_df.drop("partition",axis=1)

    money_df = task_ensembles['money'].mean
    money_df.columns = ["probabilities0","probabilities1"]
    odds_money.columns = ["odds0","odds1"]
    money_df = money_df.join(odds_money)


    def get_preds(df):
        if df['probabilities1'] > df['probabilities0']:
            return 1
        else:
            return 0

    def take_area(x,probs_col,odds_col):
        if x[odds_col] == None:
            return (x[probs_col]) * 2
        else:
            return (x[probs_col]) * x[odds_col]

    def take_bet(x,area0_col,area1_col):
        if x[area0_col] > x[area1_col]:
            return 0
        elif x[area1_col] > x[area0_col]:
            return 1
        else:
            return 0

    def take_betattr(x,area0_col,area1_col,return0_col,return1_col):
        if x[area0_col] > x[area1_col]:
            return x[return0_col]
        elif x[area1_col] > x[area0_col]:
            return x[return1_col]
        else:
            pass

    def take_predodds(x,preds_col):
        if x[preds_col] == 1:
            return x["odds1"]
        elif x[preds_col] == 0:
            return x["odds0"]
        else:
            pass


    def make_predsbets(df):
        df["predictions"] = df.apply(get_preds,axis=1)   
        df["area1"] = df.apply(take_area,axis=1,probs_col="probabilities1",odds_col="odds1")
        df["area0"] = df.apply(take_area,axis=1,probs_col="probabilities0",odds_col="odds0")
        df["bet"] = df.apply(take_bet, axis=1,area0_col="area0",area1_col="area1")
        df["betodds"] = df.apply(take_betattr, axis=1,area0_col="area0",area1_col="area1",return0_col="odds0",return1_col="odds1")
        df["betprobs"] = df.apply(take_betattr, axis=1,area0_col="area0",area1_col="area1",return0_col="probabilities0",return1_col="probabilities1")
        df["betarea"] = df.apply(take_betattr, axis=1,area0_col="area0",area1_col="area1",return0_col="area0",return1_col="area1")
        df["pred_odds"] = df.apply(take_predodds, axis=1, preds_col="predictions")
        df["pred_probs"] = df.apply(take_betattr, axis=1,area0_col="probabilities0",area1_col="probabilities1",return0_col="probabilities0",return1_col="probabilities1")

        return df

    spread_df = make_predsbets(spread_df)
    total_df = make_predsbets(total_df)    
    money_df = make_predsbets(money_df)

    spread_df = spread_df.set_index(target_gamedateindex)
    total_df = total_df.set_index(target_gamedateindex)
    money_df = money_df.set_index(target_gamedateindex)


    def readable_bet(x,task=None):
        if task == 'money':
            if x['bet'] == 1:
                return 'away team to win'
            elif x['bet'] == 0:
                return 'home team to win'

        elif task == 'spread':
            if x['bet'] == 1:
                return 'away team '+str(x['points1'])
            elif x['bet'] == 0:
                return 'home team '+str(x['points0'])

        elif task == 'total':
            if x['bet'] == 1:
                return 'under '+str(x['points'])
            elif x['bet'] == 0:
                return 'over '+str(x['points'])


    spread_df['choice'] = spread_df.apply(readable_bet,axis=1,task='spread')
    total_df['choice'] = total_df.apply(readable_bet,axis=1,task='total')
    money_df['choice'] = money_df.apply(readable_bet,axis=1,task='money')


    spread_anal_cols = ["points1","points0"]+["odds1","odds0"]+["probabilities1","probabilities0"]+["predictions","area1","area0","bet","choice"]
    total_anal_cols = ["points"]+["odds1","odds0"]+["probabilities1","probabilities0"]+["predictions","area1","area0","bet","choice"]
    money_anal_cols = ["odds1","odds0"]+["probabilities1","probabilities0"]+["predictions","area1","area0","bet","choice"]

    spread_pred = spread_df[spread_anal_cols].reset_index()
    total_pred = total_df[total_anal_cols].reset_index()
    money_pred = money_df[money_anal_cols].reset_index()

    
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

    spreadpreds_file = "{}/spreadpreds.csv".format(settings.DATASETS_PATH)
    totalpreds_file = "{}/totalpreds.csv".format(settings.DATASETS_PATH)
    moneypreds_file = "{}/moneypreds.csv".format(settings.DATASETS_PATH)

    spread_pred.drop("choice",axis=1).to_csv(spreadpreds_file,index=False)
    total_pred.drop("choice",axis=1).to_csv(totalpreds_file,index=False)
    money_pred.drop("choice",axis=1).to_csv(moneypreds_file,index=False)
    
    cnx = mlbtab.mlb_connect()
    curA = cnx.cursor()

    insert_csv(spreadpreds_file,"theDoc_logs_predsspread",curA,cnx)
    insert_csv(totalpreds_file,"theDoc_logs_predstotal",curA,cnx)
    insert_csv(moneypreds_file,"theDoc_logs_predsmoney",curA,cnx)

    curA.close()
    cnx.close()

    emailmsg = """
    <h1> ---- SPREAD ---- </h1>
    """+spread_pred.to_html()+"""
    <h1> ---- TOTAL ---- </h1>
    """+total_pred.to_html()+"""
    <h1> ---- MONEY ---- </h1>
    """+money_pred.to_html()

    emailSend.htmlEmailSend(subject='INCOMING BET',html=emailmsg)
    