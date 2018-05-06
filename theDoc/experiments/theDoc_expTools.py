import mysql.connector
import sys
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

from keras.models import Sequential, Model
from keras.layers import Dense, Dropout, Activation, normalization, recurrent, Input, wrappers, Masking, core
from keras.layers.merge import concatenate as mergeconcatenate
from keras.layers.merge import Concatenate as mergeConcatenate
from keras.layers.merge import Add as mergeadd
from keras.layers.merge import Multiply as mergeMultiply
from keras.layers.merge import Dot as mergeDot
from keras.regularizers import l2, l1
from keras.callbacks import EarlyStopping, ReduceLROnPlateau, ModelCheckpoint
from keras.optimizers import SGD, Adam
from keras.initializers import RandomNormal, RandomUniform, Ones, Zeros, VarianceScaling
from keras import backend as K
from scipy.signal import convolve, correlate
import tensorflow as tf
from tensorflow.python.framework import ops
from sklearn import calibration
from sklearn import model_selection
import os
from sklearn import linear_model
from sklearn import metrics
import sklearn.preprocessing as preprocessing
import sklearn.feature_extraction as feature_extraction

from theDoc.database import mlb_analtablesupdate as mlbtab
from theDoc.preprocessing import theDoc_dataset
from theDoc import settings



class GameResults:
    
    def __init__(self,get_results=True):
        
        assert get_results == True or get_results == False
        
        def dummy_num(df,columnname,num):
            if df[columnname] == num:
                return 1
            else:
                return 0
        
        if get_results == True:

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


            query_5 = "SELECT gid, game_date, home_team_id, away_team_id, away_score, home_score FROM pfx_miniscore;"
            curA.execute(query_5)
            cur_columns = curA.column_names
            gameresults = pd.DataFrame(np.array(curA.fetchall()))
            gameresults.columns = cur_columns

            gameresults["home_score-away_score"] = gameresults["home_score"] - gameresults["away_score"]
            gameresults["home_score+away_score"] = gameresults["home_score"] + gameresults["away_score"]

            dummyscore_cols = []
            dummyscores = np.linspace(-25,25,51)
            for num in dummyscores:
                newcolname = "hs-as_"+str(int(num))
                gameresults[newcolname] = gameresults.apply(dummy_num,axis=1,columnname="home_score-away_score",num=num)
                dummyscore_cols.append(newcolname)

            dummytots_cols = []
            dummytots = np.linspace(0,50,51)
            for num in dummytots:
                newcolname = "hs+as_"+str(int(num))
                gameresults[newcolname] = gameresults.apply(dummy_num,axis=1,columnname="home_score+away_score",num=num)
                dummytots_cols.append(newcolname)

            dummyhomescore_cols = []
            dummyhomescores = np.linspace(0,25,26)
            for num in dummyhomescores:
                newcolname = "hs_"+str(int(num))
                gameresults[newcolname] = gameresults.apply(dummy_num,axis=1,columnname="home_score",num=num)
                dummyhomescore_cols.append(newcolname)

            dummyawayscore_cols = []
            dummyawayscores = np.linspace(0,25,26)
            for num in dummyawayscores:
                newcolname = "as_"+str(int(num))
                gameresults[newcolname] = gameresults.apply(dummy_num,axis=1,columnname="away_score",num=num)
                dummyawayscore_cols.append(newcolname)

            curA.close()
            cnx.close()
            
        elif get_results == False:
            
            dummyscore_cols = []
            dummyscores = np.linspace(-25,25,51)
            for num in dummyscores:
                newcolname = "hs-as_"+str(int(num))
                dummyscore_cols.append(newcolname)

            dummytots_cols = []
            dummytots = np.linspace(0,50,51)
            for num in dummytots:
                newcolname = "hs+as_"+str(int(num))
                dummytots_cols.append(newcolname)

            dummyhomescore_cols = []
            dummyhomescores = np.linspace(0,25,26)
            for num in dummyhomescores:
                newcolname = "hs_"+str(int(num))
                dummyhomescore_cols.append(newcolname)

            dummyawayscore_cols = []
            dummyawayscores = np.linspace(0,25,26)
            for num in dummyawayscores:
                newcolname = "as_"+str(int(num))
                dummyawayscore_cols.append(newcolname)
                
            gameresults = None
            

        self.dummyscores = dummyscores 
        self.dummytots = dummytots
        self.dummyawayscores = dummyawayscores
        self.dummyhomescores = dummyhomescores
        self.dummyscore_cols = dummyscore_cols
        self.dummytots_cols = dummytots_cols
        self.dummyhomescore_cols = dummyhomescore_cols
        self.dummyawayscore_cols = dummyawayscore_cols
        self.gameresults = gameresults


class DataMaster:

    def __init__(self, version, expdir, datafilepath='{}/completedataset2018-04-08.csv'.format(settings.DATASETS_PATH)):
        
        self.version = version
        self.expdir = expdir

        qry = theDoc_dataset.query(training_gids_str='all')

        import mysql.connector
        import sys
        import datetime
        import pandas as pd
        import numpy as np
        import matplotlib.pyplot as plt

        dummyGameResults = GameResults()

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

        df = pd.read_csv(datafilepath)

        df = df.set_index("gid",drop=False)
        df = df.loc[~df.index.duplicated(keep='last')]

        tuples = list(zip(df["game_date"].apply(datetime.datetime.strptime,args=(["%Y-%m-%d"])),df["gid"]))
        gamedateindex = pd.MultiIndex.from_tuples(tuples,names=['game_date','gid'])
        df = df.set_index(gamedateindex)

        #df = df.drop(["gid","game_date","to_drop"],axis=1)
        #df = df.drop(["to_drop"],axis=1)
        df = df.drop("Unnamed: 0",axis=1)

        #df = df[:datetime.date(2016,10,2)]
        df = df.reset_index(drop=True)

        datamap = df[["gid","game_date"]]
        df = df.merge(dummyGameResults.gameresults,how='left',on='gid')
        df = df.drop(["gid","game_date_x","game_date_y"],axis=1)
        df = df.dropna(axis=0,subset=["home_score","away_score"])
        
        
        #shuffle order of samples
        df = df.sample(frac=1)


        import sklearn.preprocessing as preprocessing
        import sklearn.feature_extraction as feature_extraction

        outcomes = [
            'homevsspread',
            'awayvsspread',
            'spread_winner',
            'spread_home_oddsrat',
            'spread_away_oddsrat',
            'money_winner',
            'money_home_oddsrat',
            'money_away_oddsrat',
            'total_winner',
            'total_over_oddsrat',
            'total_under_oddsrat',
            'away_score',
            'home_score',
            'home_team_id',
            'away_team_id',
            'spread_away_points',
            'spread_home_points',
            'total_points',
            'home_score-away_score',
            'home_score+away_score'
            ] + dummyGameResults.dummyscore_cols + dummyGameResults.dummytots_cols + dummyGameResults.dummyhomescore_cols + dummyGameResults.dummyawayscore_cols

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

        index_copy = df.index

        #Separate Target Vars

        targets = df[outcomes]
        targets = pd.get_dummies(targets,columns=targets[["spread_winner","money_winner","total_winner"]])
        targets_cols = targets.columns

        df_cov = df.drop(outcomes,axis=1)

        #Separate Cont and Cat Vars

        cat_df = df_cov[categoricals]

        df_cont = df_cov.drop(categoricals,axis=1)
        df_cont_cols = df_cont.columns

        #Encoding Categoricals

        columnlist["general"] = columnlist["general"] + list(pd.get_dummies(cat_df[categoricals_general],columns=categoricals_general).columns)
        columnlist["home"]["batters"] = columnlist["home"]["batters"] + list(pd.get_dummies(cat_df[categoricals_home_batters],columns=categoricals_home_batters).columns)
        columnlist["away"]["batters"] = columnlist["away"]["batters"] + list(pd.get_dummies(cat_df[categoricals_away_batters],columns=categoricals_away_batters).columns)
        columnlist["home"]["sp"] = columnlist["home"]["sp"] + list(pd.get_dummies(cat_df[categoricals_home_sp],columns=categoricals_home_sp).columns)
        columnlist["away"]["sp"] = columnlist["away"]["sp"] + list(pd.get_dummies(cat_df[categoricals_away_sp],columns=categoricals_away_sp).columns)

        cat_df = pd.get_dummies(cat_df,columns=cat_df[categoricals])
        cat_df_cols = cat_df.columns

        #Impute Missing Values - Categorical

        imp_cat = preprocessing.Imputer(missing_values='NaN', strategy='most_frequent', axis=0)
        cat_df = imp_cat.fit_transform(cat_df)

        cat_arr = np.array(cat_df).T

        #Impute Missing Values - Continuous

        imp_cont = preprocessing.Imputer(missing_values='NaN', strategy='mean', axis=0)
        df_cont = imp_cont.fit_transform(df_cont)


        #Combine Dummied Categoricals, Continuous Vars

        all_columns = np.concatenate([df_cont_cols,cat_df_cols],axis=0)

        cont_arr = np.array(df_cont).T

        covariates = np.concatenate((cont_arr,cat_arr))
        covariates = covariates.T

        #Normalizing Individual Samples to Unit Norm

        #covariates = preprocessing.normalize(covariates, norm='l2')
        #covariates = pd.DataFrame(covariates)

        #put covariates and targets back into indexed dataframes

        covariates = pd.DataFrame(covariates)
        covariates.columns = all_columns

        #gamedateindex = pd.MultiIndex.from_tuples(index_copy,names=['game_date','gid'])
        #covariates = covariates.set_index(gamedateindex)
        #targets = targets.set_index(gamedateindex)

        #Use polynomial function to create interaction features:
        #st = datetime.datetime.now()
        #covariates = preprocessing.PolynomialFeatures(degree=2, interaction_only=False, include_bias=True).fit_transform(covariates)

        print('success',datetime.datetime.now())

        self.covariates = covariates
        self.targets = targets
        self.columnlist = columnlist



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
    
    
            
            
            
            
            
            
        




def standardize_aftersplit(X_train,X_test):
    
    #Standardizing Using Training Set
    scaler = preprocessing.StandardScaler().fit(X_train)
    #scaler = preprocessing.MinMaxScaler(feature_range=(0, 1), copy=True).fit(X_train)
    X_train_stand = scaler.transform(X_train)
    X_train_stand = pd.DataFrame(X_train_stand)
    X_test_stand = scaler.transform(X_test)
    X_test_stand = pd.DataFrame(X_test_stand)
    
    return X_train_stand, X_test_stand









class kfold_cv():
    
    def __init__(self):
        pass
    
    def run_kfold_cv(self,datamaster,n_splits=6):
        
        ho_cols = (
            datamaster.columnlist["general"] + 
            datamaster.columnlist["home"]["batters"] +
            datamaster.columnlist["home"]["teams_off"] +
            datamaster.columnlist["away"]["sp"] +
            datamaster.columnlist["away"]["teams_def"]
               )

        ao_cols = (
            datamaster.columnlist["general"] + 
            datamaster.columnlist["away"]["batters"] +
            datamaster.columnlist["away"]["teams_off"] +
            datamaster.columnlist["home"]["sp"] +
            datamaster.columnlist["home"]["teams_def"]
                   )
        
        
        outcome0_money = "money_winner_home"
        outcome1_money = "money_winner_away"
        outcome_odds0_money = "money_home_oddsrat"
        outcome_odds1_money = "money_away_oddsrat"

        outcome0_spread = "spread_winner_home"
        outcome1_spread = "spread_winner_away"
        outcome_odds0_spread = "spread_home_oddsrat"
        outcome_odds1_spread = "spread_away_oddsrat"
        outcome0_points_spread = "spread_home_points"
        outcome1_points_spread = "spread_away_points"

        outcome0_total = "total_winner_over"
        outcome1_total = "total_winner_under"
        outcome_odds0_total = "total_over_oddsrat"
        outcome_odds1_total = "total_under_oddsrat"
        outcome_points_total = "total_points"

        outcome0_runs = "home_score"
        outcome1_runs = "away_score"
        scores_cols = [outcome0_runs,outcome1_runs]

        runs_dif = "home_score-away_score"
        runs_tot = "home_score+away_score"
        scorefunc_cols = [runs_dif,runs_tot]

        dummyGameResults = GameResults(get_results=False)
        
        def fold_actions(X_train, X_test, y_train, y_test):
            

            X_train, X_test = standardize_aftersplit(X_train,X_test)
            X_train.columns, X_test.columns = datamaster.covariates.columns, datamaster.covariates.columns

            X_train_ho, X_train_ao, X_test_ho, X_test_ao = X_train[ho_cols].copy(), X_train[ao_cols].copy(), X_test[ho_cols].copy(), X_test[ao_cols].copy()
            X_train_ho["ishome"],X_train_ho["isaway"], X_test_ho["ishome"],X_test_ho["isaway"] = 1,0,1,0
            X_train_ao["ishome"],X_train_ao["isaway"], X_test_ao["ishome"],X_test_ao["isaway"] = 0,1,0,1

            X_train_ho, X_train_ao, X_test_ho, X_test_ao = X_train_ho.reset_index(drop=True), X_train_ao.reset_index(drop=True), X_test_ho.reset_index(drop=True), X_test_ao.reset_index(drop=True)
            y_train, y_test = y_train.reset_index(drop=True), y_test.reset_index(drop=True)

            import math

            num_trunced_samplestrain = math.floor(len(X_train_ho)/datamaster.hyp['batch_size']) * datamaster.hyp['batch_size']
            num_trunced_samplestest = math.floor(len(X_test_ho)/datamaster.hyp['batch_size']) * datamaster.hyp['batch_size']
            trunced_indextrain = np.random.permutation(list(X_train_ho.index))[:num_trunced_samplestrain]
            trunced_indextest = np.random.permutation(list(X_test_ho.index))[:num_trunced_samplestest]

            X_train_ho, X_train_ao, X_test_ho, X_test_ao = X_train_ho.iloc[trunced_indextrain], X_train_ao.iloc[trunced_indextrain], X_test_ho.iloc[trunced_indextest], X_test_ao.iloc[trunced_indextest]
            y_train, y_test = y_train.iloc[trunced_indextrain], y_test.iloc[trunced_indextest]

            X_train_ho, X_train_ao, X_test_ho, X_test_ao = np.array(X_train_ho), np.array(X_train_ao), np.array(X_test_ho), np.array(X_test_ao)


            y_train = y_train.reset_index(drop=True)
            y_test = y_test.reset_index(drop=True)

            ob0_money, ob1_money = len(y_train[y_train[outcome0_money] == 1])/len(y_train), len(y_train[y_train[outcome1_money] == 1])/len(y_train)
            class_weights_money = np.array([ob0_money,ob1_money])
            ob0_spread, ob1_spread = len(y_train[y_train[outcome0_spread] == 1])/len(y_train), len(y_train[y_train[outcome1_spread] == 1])/len(y_train)
            class_weights_spread = np.array([ob0_spread,ob1_spread])
            ob0_total, ob1_total = len(y_train[y_train[outcome0_total] == 1])/len(y_train), len(y_train[y_train[outcome1_total] == 1])/len(y_train)
            class_weights_total = np.array([ob0_total,ob1_total])  
            scores_bias = np.array([np.mean(y_train[outcome0_runs]),np.mean(y_train[outcome1_runs])])

            nonlocal y_test_spread_cv 
            nonlocal test_points_spread_cv 
            nonlocal test_odds_spread_cv
            nonlocal y_test_total_cv 
            nonlocal test_points_total_cv 
            nonlocal test_odds_total_cv
            nonlocal y_test_money_cv
            nonlocal test_odds_money_cv

            test_odds_money = np.array(y_test[[outcome_odds0_money,outcome_odds1_money]])
            test_odds_money_cv = ensemble.newarrayconcat(test_odds_money_cv,test_odds_money)
            y_test_money = np.array(y_test[[outcome0_money,outcome1_money]])
            y_test_money_cv = ensemble.newarrayconcat(y_test_money_cv,y_test_money)
            train_odds1_money = np.array(y_train[[outcome_odds0_money,outcome_odds1_money]])
            y_train_money = np.array(y_train[[outcome0_money,outcome1_money]])

            test_points_spread = np.array(y_test[[outcome0_points_spread,outcome1_points_spread]])
            test_points_spread_cv = ensemble.newarrayconcat(test_points_spread_cv,test_points_spread)
            test_odds_spread = np.array(y_test[[outcome_odds0_spread,outcome_odds1_spread]])
            test_odds_spread_cv = ensemble.newarrayconcat(test_odds_spread_cv,test_odds_spread)
            y_test_spread = np.array(y_test[[outcome0_spread,outcome1_spread]])
            y_test_spread_cv = ensemble.newarrayconcat(y_test_spread_cv,y_test_spread)
            train_points_spread = np.array(y_train[[outcome0_points_spread,outcome1_points_spread]])
            train_odds1_spread = np.array(y_train[[outcome_odds0_spread,outcome_odds1_spread]])
            y_train_spread = np.array(y_train[[outcome0_spread,outcome1_spread]])

            test_points_total = np.array(y_test[[outcome_points_total]])
            test_points_total_cv = ensemble.newarrayconcat(test_points_total_cv,test_points_total)
            test_odds_total = np.array(y_test[[outcome_odds0_total,outcome_odds1_total]])
            test_odds_total_cv = ensemble.newarrayconcat(test_odds_total_cv,test_odds_total)
            y_test_total = np.array(y_test[[outcome0_total,outcome1_total]])
            y_test_total_cv = ensemble.newarrayconcat(y_test_total_cv,y_test_total)
            train_points_total = np.array(y_train[[outcome_points_total]])
            train_odds1_total = np.array(y_train[[outcome_odds0_total,outcome_odds1_total]])
            y_train_total = np.array(y_train[[outcome0_total,outcome1_total]])

            y_test_dummyscores = np.array(y_test[dummyGameResults.dummyscore_cols])
            y_train_dummyscores = np.array(y_train[dummyGameResults.dummyscore_cols])

            y_test_dummytots = np.array(y_test[dummyGameResults.dummytots_cols])
            y_train_dummytots = np.array(y_train[dummyGameResults.dummytots_cols])  

            y_test_scores = np.array(y_test[scores_cols])
            y_train_scores = np.array(y_train[scores_cols])

            y_test_dummyhomescore = np.array(y_test[dummyGameResults.dummyhomescore_cols])
            y_train_dummyhomescore = np.array(y_train[dummyGameResults.dummyhomescore_cols])

            y_test_dummyawayscore = np.array(y_test[dummyGameResults.dummyawayscore_cols])
            y_train_dummyawayscore = np.array(y_train[dummyGameResults.dummyawayscore_cols]) 

            dummyscore_distro = pd.DataFrame((np.sum(y_train_dummyscores,axis=0)/len(y_train_dummyscores))).T
            dummyscore_distro = np.array(dummyscore_distro)[0]

            dummytots_distro = pd.DataFrame((np.sum(y_train_dummytots,axis=0)/len(y_train_dummytots))).T
            dummytots_distro = np.array(dummytots_distro)[0]

            dummyhomescore_distro = pd.DataFrame((np.sum(y_train_dummyhomescore,axis=0)/len(y_train_dummyhomescore))).T
            dummyhomescore_distro = np.array(dummyhomescore_distro)[0]

            dummyawayscore_distro = pd.DataFrame((np.sum(y_train_dummyawayscore,axis=0)/len(y_train_dummyawayscore))).T
            dummyawayscore_distro = np.array(dummyawayscore_distro)[0]

            num_hoao_inputfeatures = X_train_ho.shape[1]

            ## CREATE ENSEMBLE FROM MULTIPLE MODELS

            ens_score = None
            ens_tots = None
            ens_winner = None
            ens_base = None

            num_ensemble_models = datamaster.hyp['num_ensemblemodels']

            for ensemble_mod in range(num_ensemble_models):
                #print('ensemble model number:',ensemble_mod)
                

                seed = None
                np.random.seed(None)

                model = None

                num_hiddenunits = datamaster.hyp['num_hiddenunits']
                hidden_layers = datamaster.hyp['num_hiddenlayers']

                drop = datamaster.hyp['dropout']
                act = datamaster.hyp['non_linearity']
                bias_initializer = 'zeros'
                kernel_initializer=VarianceScaling(scale=1.0, mode='fan_in', distribution='normal', seed=None)
                kernel_regularizer = datamaster.hyp['weight_decay']
                activity_regularizer = l2(0.00)
                bnorm_kwargs = {'axis':-1, 'momentum':0.99, 'epsilon':0.001, 'center':True, 'scale':True
                                                 , 'beta_initializer':'zeros', 'gamma_initializer':'ones'
                                                 , 'moving_mean_initializer':'zeros'
                                                 , 'moving_variance_initializer':'ones', 'beta_regularizer':None
                                                 , 'gamma_regularizer':None, 'beta_constraint':None, 'gamma_constraint':None}

                dense_kwargs = {'kernel_initializer':kernel_initializer,'bias_initializer':bias_initializer, 'kernel_regularizer':kernel_regularizer}


                def output_bias(shape, dtype=None):
                    return class_weights
                def dummyscore_bias(shape, dtype=None):
                    return dummyscore_distro
                def dummytots_bias(shape, dtype=None):
                    return dummytots_distro
                def dummyhomescore_bias(shape, dtype=None):
                    return dummyhomescore_distro
                def dummyawayscore_bias(shape, dtype=None):
                    return dummyawayscore_distro
                def scores_bias_f(shape, dtype=None):
                    return scores_bias
                def winner_bias(shape, dtype=None):
                    return class_weights_money

                num_hoao_inputfeatures = X_train_ho.shape[1]

                try: assert hidden_layers % 2 != 0 or hidden_layers < 3
                except: 
                    print('ERROR: number of hidden layers must be odd or less than 3')
                    raise

                ###########################################################

                ### NEURAL ###
                ### NETWORK ###

                inputlayers = []

                VisibleLayer = {}
                for x in ['ho','ao']:
                    VisibleLayer[x] = Input(shape=(num_hoao_inputfeatures,), name=x+'_input')
                    inputlayers.append(VisibleLayer[x])

                HiddenCell = {}
                HiddenCell['layers'], HiddenCell['tensors'] = {}, {}
                for n in range(hidden_layers):
                    nn = str(n+1)
                    HiddenCell['layers']['rep'+nn] = Dense(num_hiddenunits, name = 'rep'+nn, **dense_kwargs)
                    HiddenCell['layers']['rep'+nn+'_norm'] = normalization.BatchNormalization(**bnorm_kwargs)
                    HiddenCell['layers']['rep'+nn+'_act'] = Activation(act)
                    HiddenCell['layers']['rep'+nn+'_drop'] = Dropout(drop)
                    if n > 0 and n%2 == 0: HiddenCell['layers']['rep'+nn+'_add'] = mergeadd()

                    for x in ['ho','ao']:
                        if n == 0: inp = VisibleLayer[x]
                        elif n < 3 or n%2 == 0: inp = HiddenCell['tensors'][x+'_rep'+str(n)+'_drop']
                        else: inp = HiddenCell['tensors'][x+'_rep'+str(n)+'_add']
                        HiddenCell['tensors'][x+'_rep'+nn] = HiddenCell['layers']['rep'+nn](inp)
                        HiddenCell['tensors'][x+'_rep'+nn+'_norm'] = HiddenCell['layers']['rep'+nn+'_norm'](HiddenCell['tensors'][x+'_rep'+nn])
                        HiddenCell['tensors'][x+'_rep'+nn+'_act'] = HiddenCell['layers']['rep'+nn+'_act'](HiddenCell['tensors'][x+'_rep'+nn+'_norm'])
                        #HiddenCell['tensors'][x+'_rep'+nn+'_act'] = HiddenCell['layers']['rep'+nn+'_act'](HiddenCell['tensors'][x+'_rep'+nn])     ### USE THIS FOR NO BATCH NORM
                        HiddenCell['tensors'][x+'_rep'+nn+'_drop'] = HiddenCell['layers']['rep'+nn+'_drop'](HiddenCell['tensors'][x+'_rep'+nn+'_act'])  ### USE THIS FOR DROPOUT
                        if n == 2: HiddenCell['tensors'][x+'_rep'+nn+'_add'] = HiddenCell['layers']['rep'+nn+'_add']([HiddenCell['tensors'][x+'_rep'+str(n-1)+'_drop'],HiddenCell['tensors'][x+'_rep'+nn+'_drop']])
                        elif n > 2 and n%2 == 0: HiddenCell['tensors'][x+'_rep'+nn+'_add'] = HiddenCell['layers']['rep'+nn+'_add']([HiddenCell['tensors'][x+'_rep'+str(n-1)+'_add'],HiddenCell['tensors'][x+'_rep'+nn+'_drop']])


                if hidden_layers < 4:
                    ho_repfin_drop = HiddenCell['tensors']['ho_rep'+str(hidden_layers)+'_drop']
                    ao_repfin_drop = HiddenCell['tensors']['ao_rep'+str(hidden_layers)+'_drop']
                else:
                    ho_repfin_drop = HiddenCell['tensors']['ho_rep'+str(hidden_layers)+'_add']
                    ao_repfin_drop = HiddenCell['tensors']['ao_rep'+str(hidden_layers)+'_add']

                output_score_layer = Dense(26, activation = 'softmax', bias_initializer='zeros', name='dummyscore')
                output_dummyhomescore = output_score_layer(ho_repfin_drop)
                output_dummyawayscore = output_score_layer(ao_repfin_drop)

                # Generate Extraction Matrix and Dot-Product

                convarr_len = 26

                iext_layer = {}
                for n in range(convarr_len):
                    iext_layer[n] = Input(shape=(convarr_len,), name='iext_'+str(n))
                    inputlayers.append(iext_layer[n])

                ones_layer = Input(shape=(convarr_len,), name='onescol')
                inputlayers.append(ones_layer)


                # Extract each part of image of distro into separate tensors

                homescore_img = {}
                for n in range(convarr_len):
                    homescore_img[n] = mergeMultiply()([iext_layer[n],output_dummyhomescore])
                    homescore_img[n] = mergeDot(-1)([homescore_img[n],ones_layer])
                awayscore_img = {}
                for n in range(convarr_len):
                    awayscore_img[n] = mergeMultiply()([iext_layer[n],output_dummyawayscore])
                    awayscore_img[n] = mergeDot(-1)([awayscore_img[n],ones_layer])


                # Arrange grid of discrete image-products for reference

                product_grid = np.array([None]*convarr_len*convarr_len).reshape(convarr_len,convarr_len)

                for n1 in range(convarr_len):
                    for n2 in range(convarr_len):
                        product_grid[n1,n2] = mergeMultiply()([homescore_img[n2],awayscore_img[n1]])


                # Discrete Correlation Function

                corr_arr = list(np.array([0]*((convarr_len*2)-1)))

                for n in range(convarr_len):
                    adding_list = []
                    for m in range(n+1):
                        adding_list.append(product_grid[m,n-m])
                    if len(adding_list) == 1:
                        corr_arr[n] = adding_list[0]
                    else:
                        corr_arr[n] = mergeadd()(adding_list)

                for n in range(convarr_len-1):
                    adding_list = []
                    for m in range(n+1):
                        adding_list.append(product_grid[convarr_len-1-m,convarr_len-1-n+m])
                    if len(adding_list) == 1:
                        corr_arr[((convarr_len*2)-2)-n] = adding_list[0]
                    else:
                        corr_arr[((convarr_len*2)-2)-n] = mergeadd()(adding_list)


                # Discrete Convolution Function

                conv_arr = list(np.array([0]*((convarr_len*2)-1)))

                for n in range(convarr_len):
                    adding_list = []
                    for m in range(n+1):
                        adding_list.append(product_grid[convarr_len-1-m,n-m])
                    if len(adding_list) == 1:
                        conv_arr[n] = adding_list[0]
                    else:
                        conv_arr[n] = mergeadd()(adding_list)

                for n in range(convarr_len-1):
                    adding_list = []
                    for m in range(n+1):
                        adding_list.append(product_grid[n-m,convarr_len-1-m])
                    if len(adding_list) == 1:
                        conv_arr[((convarr_len*2)-2)-n] = adding_list[0]
                    else:
                        conv_arr[((convarr_len*2)-2)-n] = mergeadd()(adding_list)


                output_dummytots = mergeConcatenate(axis=-1, name='dummytots')(corr_arr)
                output_dummyruns_pre = mergeConcatenate(axis=-1, name='dummydif_pre')(conv_arr)

                rep_concat = mergeconcatenate([ho_repfin_drop,ao_repfin_drop])

                output_winner_layer = Dense(2, activation='softmax',name='winner')
                output_winner = output_winner_layer(rep_concat)

                output_dummyruns_layer = Dense(51, activation='linear',name='dummydif',trainable=False)
                output_dummyruns = output_dummyruns_layer(output_dummyruns_pre)
                
                
                ###########################################################

                model = Model(inputs=inputlayers,outputs=[output_dummyhomescore, output_dummyawayscore,output_dummyruns,output_dummytots,output_winner])

                model.compile(loss='categorical_crossentropy',
                              optimizer=Adam(lr=datamaster.hyp['learning_rate'], beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.001)
                              #optimizer=SGD(lr=datamaster.hyp['learning_rate'], decay=0.001, momentum=datamaster.hyp['momentum'])
                              ,metrics=['accuracy']
                              ,loss_weights=datamaster.hyp['loss_weights'][0]
                             )

                # Set weights for untrainable layers
                
                def weightssetup(arr,p,x=np.arange(0,50),n=50):
                    
                    from scipy.stats import binom

                    bindis = binom.pmf(x, n, p)
                    half1 = bindis[0:25]
                    half2 = bindis[25:]

                    finbin = np.concatenate([half1,np.array([0]),half2],axis=0)
                    finbin = finbin.reshape(1,-1)
                    if arr is None:
                        arr = finbin
                    else:
                        arr = np.concatenate([arr,finbin],axis=0)

                    return arr


                dif_iden = np.identity(51)
                dif_iden[25] = weightssetup(arr=None,p=0.5,n=49)
                dif_zeros = np.zeros(51*2).reshape(2,51)
                dif_weights = dif_iden
                dif_biases = np.zeros(51).reshape(51,)
                output_dummyruns_layer.set_weights([dif_weights,dif_biases])      
                
                
                def theDoc_makeinputslist(pre_inputs=[],sample_size=1,convarr_len=26):

                    assert type(pre_inputs)==list
                    assert len(pre_inputs) > 0
                    inputtensors = pre_inputs

                    zero_0 = np.zeros(sample_size*convarr_len).reshape(sample_size,convarr_len)
                    ones_0 = np.ones(sample_size*convarr_len).reshape(sample_size,convarr_len)

                    iext = {}
                    for n in range(convarr_len):
                        iext[n] = zero_0.copy()
                        iext[n][:,n] = 1
                        inputtensors.append(iext[n])

                    inputtensors.append(ones_0)

                    return inputtensors

                finalX_train = theDoc_makeinputslist([X_train_ho,X_train_ao],sample_size=len(X_train_ho))
                finalX_test = theDoc_makeinputslist([X_test_ho,X_test_ao],sample_size=len(X_test_ho))


                # Fit Model
                
                val_split = math.floor((len(X_train_ho)/datamaster.hyp['batch_size'])*0.15)*datamaster.hyp['batch_size']/len(X_train_ho)

                hist = model.fit(
                           finalX_train
                          ,[y_train_dummyhomescore,y_train_dummyawayscore,y_train_dummyscores,y_train_dummytots,y_train_money]
                          ,epochs=200
                          ,batch_size=datamaster.hyp['batch_size']
                          ,initial_epoch=0
                          ,validation_split=val_split
                          ,verbose=0
                          ,callbacks=[
                                    EarlyStopping(monitor='val_loss', min_delta=0.001, patience=5, verbose=0, mode='auto')
                                    ,ReduceLROnPlateau(monitor='val_loss', factor=0.25, patience=2, verbose=0, mode='auto', epsilon=0.003, cooldown=0, min_lr=0)
                                    ,ModelCheckpoint(filepath=datamaster.expdir+datamaster.version+"_weights.{epoch:02d}.hdf5", monitor='val_loss', verbose=0, save_best_only=True, save_weights_only=True, mode='auto', period=1)
                    ]
                         )
                
                from math import isnan
                class FlowFlags: pass
                flow_flags = FlowFlags()
                flow_flags.divergence_indicator = isnan(hist.history['loss'][-1])
              

                # Load Optimal Epoch

                minepoch = np.argmin(hist.history['val_loss'])
                minepoch_sum = np.argmin(np.array(hist.history['val_dummyscore_loss']) + np.array(hist.history['val_dummydif_loss']) + np.array(hist.history['val_dummytots_loss']) + np.array(hist.history['val_winner_loss']))
                
                custepoch = 0
                if custepoch != 0:
                    loadepoch = custepoch
                else:
                    loadepoch = minepoch + 1

                if loadepoch < 10:
                    loadepochstr = str(0)+str(loadepoch)
                else: 
                    loadepochstr = str(loadepoch)
                    
                if flow_flags.divergence_indicator == False:
                    model.load_weights(datamaster.expdir+datamaster.version+"_weights."+loadepochstr+".hdf5") 

                trainloss = hist.history['loss'][minepoch]
                valloss = hist.history['val_loss'][minepoch]
                single_model_eval = model.evaluate(
                        finalX_test,
                        [y_test_dummyhomescore,y_test_dummyawayscore,y_test_dummyscores,y_test_dummytots,y_test_money],
                    verbose = 0
                    )
                testloss = single_model_eval[0]
                print('trainloss:',trainloss,'valloss:',valloss,'testloss:',testloss)
                
                dif_trainloss = hist.history['dummydif_loss'][minepoch]
                dif_valloss = hist.history['val_dummydif_loss'][minepoch]
                dif_testloss = single_model_eval[3]
                print('dif_trainloss:',dif_trainloss,'dif_valloss:',dif_valloss,'dif_testloss:',dif_testloss)
                
                tots_trainloss = hist.history['dummytots_loss'][minepoch]
                tots_valloss = hist.history['val_dummytots_loss'][minepoch]
                tots_testloss = single_model_eval[4]
                print('tots_trainloss:',tots_trainloss,'tots_valloss:',tots_valloss,'tots_testloss:',tots_testloss)
                
                winner_trainloss = hist.history['winner_loss'][minepoch]
                winner_valloss = hist.history['val_winner_loss'][minepoch]
                winner_testloss = single_model_eval[5]
                print('winner_trainloss:',winner_trainloss,'winner_valloss:',winner_valloss,'winner_testloss:',winner_testloss)


                # Put inferred probabilities into Dataframes


                probas = model.predict(finalX_test, batch_size=256, verbose=0)
                pred_dummyhomescore_df = pd.DataFrame(probas[0])
                pred_dummyhomescore_df.columns = dummyGameResults.dummyhomescore_cols
                pred_dummyawayscore_df = pd.DataFrame(probas[1])
                pred_dummyawayscore_df.columns = dummyGameResults.dummyawayscore_cols
                pred_dummyscore_df = pd.DataFrame(probas[2])
                pred_dummyscore_df.columns = dummyGameResults.dummyscore_cols
                pred_dummytots_df = pd.DataFrame(probas[3])
                pred_dummytots_df.columns = dummyGameResults.dummytots_cols
                pred_winner_df = pd.DataFrame(probas[4])
                pred_winner_df.columns = [outcome0_money,outcome1_money]
                probas_df = pred_dummyhomescore_df.join(pred_dummyawayscore_df).join(pred_dummyscore_df).join(pred_dummytots_df).join(pred_winner_df)

                del model
                
                # Merge individual model probabilities into ensemble dataframe

                if ens_score is None: ens_score = ensemble(pred_dummyscore_df,dif_trainloss,dif_valloss,dif_testloss)
                else: 
                    ens_score.add_model(pred_dummyscore_df)
                    ens_score.add_losses(dif_trainloss,dif_valloss,dif_testloss)

                if ens_tots is None: ens_tots = ensemble(pred_dummytots_df,tots_trainloss,tots_valloss,tots_testloss)
                else: 
                    ens_tots.add_model(pred_dummytots_df)
                    ens_score.add_losses(tots_trainloss,tots_valloss,tots_testloss)


                if ens_winner is None: ens_winner = ensemble(pred_winner_df,winner_trainloss,winner_valloss,winner_testloss)
                else: 
                    ens_winner.add_model(pred_winner_df)
                    ens_score.add_losses(winner_trainloss,winner_valloss,winner_testloss)
                    
                if ens_base is None: ens_base = ensemble(None,trainloss,valloss,testloss)
                else:
                    ens_base.add_losses(trainloss,valloss,testloss)

                
                if flow_flags.divergence_indicator == True:
                    print('model div ind:', flow_flags.divergence_indicator)
                    break
                  
                if trainloss > 100:
                    flow_flags.trainloss_break = True
                    break
                else:
                    flow_flags.trainloss_break = False

            return ens_score, ens_tots, ens_winner, ens_base, flow_flags   
        
        
        st = datetime.datetime.now()
        #print(model,'\n')

        kf = model_selection.KFold(n_splits)

        foldnum = 0
        self.ens_score_cv = None
        self.ens_tots_cv = None
        self.ens_winner_cv = None
        self.ens_base_cv = None

        y_test_spread_cv, test_points_spread_cv, test_odds_spread_cv = None, None, None
        y_test_total_cv, test_points_total_cv, test_odds_total_cv = None, None, None
        y_test_money_cv, test_odds_money_cv = None, None
        
        

        for train_index, test_index in kf.split(datamaster.covariates):
            print('fold number',foldnum)
            fold_st = datetime.datetime.now()

            X_train, X_test = datamaster.covariates.iloc[train_index], datamaster.covariates.iloc[test_index]
            y_train, y_test = datamaster.targets.iloc[train_index], datamaster.targets.iloc[test_index]

            ens_score, ens_tots, ens_winner, ens_base, flow_flags = fold_actions(X_train, X_test, y_train, y_test)

            ens_score.mean['foldnum'] = foldnum
            ens_tots.mean['foldnum'] = foldnum
            ens_winner.mean['foldnum'] = foldnum

            if self.ens_score_cv is None: self.ens_score_cv = ensemble(ens_score.mean,ens_score.trainloss,ens_score.valloss,ens_score.testloss)
            else: 
                self.ens_score_cv.mean = pd.concat([self.ens_score_cv.mean,ens_score.mean])
                self.ens_score_cv.add_losses(ens_score.trainloss,ens_score.valloss,ens_score.testloss)

            if self.ens_tots_cv is None: self.ens_tots_cv = ensemble(ens_tots.mean,ens_tots.trainloss,ens_tots.valloss,ens_tots.testloss)
            else: 
                self.ens_tots_cv.mean = pd.concat([self.ens_tots_cv.mean,ens_tots.mean])
                self.ens_tots_cv.add_losses(ens_tots.trainloss,ens_tots.valloss,ens_tots.testloss)

            if self.ens_winner_cv is None: self.ens_winner_cv = ensemble(ens_winner.mean,ens_winner.trainloss,ens_winner.valloss,ens_winner.testloss)
            else: 
                self.ens_winner_cv.mean = pd.concat([self.ens_winner_cv.mean,ens_winner.mean])
                self.ens_winner_cv.add_losses(ens_winner.trainloss,ens_winner.valloss,ens_winner.testloss)
                
            if self.ens_base_cv is None: self.ens_base_cv = ensemble(None,ens_base.trainloss,ens_base.valloss,ens_base.testloss)
            else:
                self.ens_base_cv.add_losses(ens_base.trainloss,ens_base.valloss,ens_base.testloss)

            #print('fold number {} complete'.format(foldnum),'script time:',datetime.datetime.now()-fold_st,'\n')
            foldnum += 1
            
            self.version = datamaster.version
            self.expdir = datamaster.expdir
            
            
            if flow_flags.divergence_indicator == True or flow_flags.trainloss_break == True:
                break
            
        self.dummyscore_df = self.ens_score_cv.mean[dummyGameResults.dummyscore_cols]
        self.dummyscore_df.columns = dummyGameResults.dummyscores
        y_test_spread_df, test_points_spread_df, test_odds_spread_df = pd.DataFrame(y_test_spread_cv), pd.DataFrame(test_points_spread_cv), pd.DataFrame(test_odds_spread_cv)
        y_test_spread_df.columns, test_points_spread_df.columns, test_odds_spread_df.columns = ["outcome0","outcome1"], ["points0","points1"], ["odds0","odds1"]
        self.dummyscore_df = self.dummyscore_df.join(y_test_spread_df).join(test_points_spread_df).join(test_odds_spread_df)

        self.dummytots_df = self.ens_tots_cv.mean[dummyGameResults.dummytots_cols]
        self.dummytots_df.columns = dummyGameResults.dummytots
        y_test_total_df, test_points_total_df, test_odds_total_df = pd.DataFrame(y_test_total_cv), pd.DataFrame(test_points_total_cv), pd.DataFrame(test_odds_total_cv)
        y_test_total_df.columns, test_points_total_df.columns, test_odds_total_df.columns = ["outcome0","outcome1"], ["points"], ["odds0","odds1"]
        self.dummytots_df = self.dummytots_df.join(y_test_total_df).join(test_points_total_df).join(test_odds_total_df)

        self.money_df = self.ens_winner_cv.mean[[outcome0_money,outcome1_money]]
        self.money_df.columns = ["probabilities0","probabilities1"]
        y_test_money_df, test_odds_money_df = pd.DataFrame(y_test_money_cv), pd.DataFrame(test_odds_money_cv)
        y_test_money_df.columns, test_odds_money_df.columns = ["outcome0","outcome1"], ["odds0","odds1"]
        self.money_df = self.money_df.join(y_test_money_df).join(test_odds_money_df)
        
        self.losses_cv = {
            'diftrain':self.ens_score_cv.trainloss,'difval':self.ens_score_cv.valloss,'diftest':self.ens_score_cv.testloss,
            'totstrain':self.ens_tots_cv.trainloss,'totsval':self.ens_tots_cv.valloss,'totstest':self.ens_tots_cv.testloss,
            'winnertrain':self.ens_winner_cv.trainloss,'winnerval':self.ens_winner_cv.valloss,'winnertest':self.ens_winner_cv.testloss,
            'basetrain':self.ens_base_cv.trainloss,'baseval':self.ens_base_cv.valloss,'basetest':self.ens_base_cv.testloss
        }
        
        kfold_cv.flow_flag = flow_flags.trainloss_break
        kfold_cv.hyp = datamaster.hyp

        print('cv runtime:',datetime.datetime.now()-st)


    
    
    
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


def betting_analysis(kfold_cv, rides=[1]):
    
    tempGR = GameResults(get_results=False)

    kfold_cv.dummyscore_df["probabilities0"] = kfold_cv.dummyscore_df.apply(cumprob_above,axis=1,valuecol="points0",dummylist=tempGR.dummyscores)
    kfold_cv.dummyscore_df["probabilities1"] = kfold_cv.dummyscore_df.apply(cumprob_below,axis=1,valuecol="points1",dummylist=tempGR.dummyscores)

    kfold_cv.dummytots_df["probabilities0"] = kfold_cv.dummytots_df.apply(cumprob_over,axis=1,valuecol="points",dummylist=tempGR.dummytots)
    kfold_cv.dummytots_df["probabilities1"] = kfold_cv.dummytots_df.apply(cumprob_below,axis=1,valuecol="points",dummylist=tempGR.dummytots)

    """   
    money_df["probabilities0"].plot.hist(bins=40,alpha=0.5,label="0")
    money_df["probabilities1"].plot.hist(bins=40,alpha=0.5,label="1")
    plt.legend()
    plt.title("money")
    plt.show()
    dummytots_df["probabilities0"].plot.hist(bins=40,alpha=0.5,label="0")
    dummytots_df["probabilities1"].plot.hist(bins=40,alpha=0.5,label="1")
    plt.legend()
    plt.title("total")
    plt.show()
    dummyscore_df["probabilities0"].plot.hist(bins=40,alpha=0.5,label="0")
    dummyscore_df["probabilities1"].plot.hist(bins=40,alpha=0.5,label="1")
    plt.legend()
    plt.title("spread")
    plt.show()"""

    def get_preds(df):
        if df['probabilities1'] > 0.50000:
            return 1
        else:
            return 0

    def ispush(df):
        if df['outcome0'] == 0 and df['outcome1'] == 0:
            return 1
        else:
            return 0


    kfold_cv.dummyscore_df['predictions'] = kfold_cv.dummyscore_df.apply(get_preds,axis=1)
    kfold_cv.dummytots_df['predictions'] = kfold_cv.dummytots_df.apply(get_preds,axis=1)
    kfold_cv.money_df['predictions'] = kfold_cv.money_df.apply(get_preds,axis=1)

    kfold_cv.dummyscore_df['ispush'] = kfold_cv.dummyscore_df.apply(ispush,axis=1)
    kfold_cv.dummytots_df['ispush'] = kfold_cv.dummytots_df.apply(ispush,axis=1)
    kfold_cv.money_df['ispush'] = kfold_cv.money_df.apply(ispush,axis=1)
    
    exp_outcomes_all = pd.DataFrame(kfold_cv.hyp)
    exp_outcomes_all['weight_decay'] = [{'l1':exp_outcomes_all['weight_decay'][0].l1.reshape(1)[0],'l2':exp_outcomes_all['weight_decay'][0].l2.reshape(1)[0]}]

    for ride in rides:
        #print('\n\n\n\n\n -------- RIDE PERCENTAGE = {}%'.format(ride),'--------')
        for bet_df, bet_dfname in zip([kfold_cv.money_df,kfold_cv.dummytots_df,kfold_cv.dummyscore_df],['MONEY','TOTAL','SPREAD']):

            #print("\n\n\n\n\n ----",bet_dfname,"----")

            results_df = bet_df.copy()

            rowsdropped = len(results_df[results_df.isnull().any(axis=1) == True]) + len(results_df[(results_df.isnull().any(axis=1) == False) & results_df['ispush'] == 1])
            #print('rows dropped:',rowsdropped)
            results_df = results_df[results_df['ispush'] == 0].dropna()
            results_df = results_df.reset_index()

            finalresults = results_df
            #finalresults, probcal_slope = prob_cal(finalresults,"probabilities1","probabilities0",type_="sigmoid")

            betprep(finalresults)

            #modeleval_pred(finalresults["outcome1"],finalresults["predictions"],finalresults["probabilities1"])
            
            auc = metrics.roc_auc_score(finalresults["outcome1"], finalresults["probabilities1"])
            allpred_payout, _ = payoutcalc(finalresults["outcome1"],finalresults["predictions"],finalresults["pred_odds"],strategy="uniform")

            #print('\n','All Predictions:')
            #modeleval_payouts(finalresults["outcome1"],finalresults["predictions"],finalresults["pred_odds"].fillna(2),finalresults["pred_probs"],ride_p=ride)
            
            #print('\n','Area - All Bets:')
            #modeleval_payouts(finalresults["outcome1"],finalresults["bet"],finalresults["betodds"].fillna(2),finalresults["betprobs"],ride_p=ride)

            thresh_payouts = areathresholdanalysis(finalresults,"outcome1","bet","betodds","betprobs","betarea",ride=ride)
            thresh_payouts = pd.DataFrame(thresh_payouts)
            thresh_payouts = thresh_payouts.set_index("thresh",drop=True)

            #print('basic:\n',thresh_payouts)
            
            exp_outcomes = thresh_payouts[["uni_p"]]
            valmax = exp_outcomes.max(axis=0)
            idxmax = exp_outcomes.idxmax(axis=0)
            
            exp_outcomes = exp_outcomes.T.reset_index(drop=True)
            exp_outcomes.columns = [bet_dfname + '_' + str(s) for s in exp_outcomes.columns]
            exp_outcomes[bet_dfname+'_valmax'] = valmax['uni_p']
            exp_outcomes[bet_dfname+'_idxmax'] = idxmax['uni_p']
            exp_outcomes[bet_dfname+'_allpred_payout'] = allpred_payout
            exp_outcomes[bet_dfname+'_auc'] = auc
            
            exp_outcomes_all = exp_outcomes_all.reset_index(drop=True).join(exp_outcomes.reset_index(drop=True))

            """
            print('\n','Empirical Area - All Bets:')
            modeleval_payouts(finalresults["outcome1"],finalresults["emp_bet"],finalresults["emp_betodds"].fillna(2),finalresults["emp_betprobs"],ride_p=ride)

            thresh_payouts = areathresholdanalysis(finalresults,"outcome1","emp_bet","emp_betodds","emp_betprobs","emp_betarea",ride=ride)

            thresh_payouts = pd.DataFrame(thresh_payouts)
            thresh_payouts = thresh_payouts.set_index("thresh",drop=True)

            print('empirical:\n',thresh_payouts)
            """
            
    exp_outcomes_all['flow_flag'] = kfold_cv.flow_flag
            
    kfold_cv.exp_outcomes_all = exp_outcomes_all

def write_results(kfold_cv):
    
    outfile = kfold_cv.expdir+kfold_cv.version+'cv_ensemble_randomsearch_testing.csv'
    
    with open(outfile,'a') as f:
        if os.stat(f.name).st_size==0:
            headerflag = True
        else:
            headerflag = False
        kfold_cv.exp_outcomes_all.to_csv(f,header=headerflag)
        

    print('\ndone!')
    
    
    
    
    
    
def roc_curve(y_test,y_score,n_classes=2):
    # Compute ROC curve and ROC area for each class
    fpr, tpr, _ = metrics.roc_curve(y_test, y_score)
    roc_auc = metrics.auc(fpr, tpr)

    plt.figure()
    lw = 2
    plt.plot(fpr, tpr, color='darkorange',
             lw=lw, label='ROC curve (area = %0.2f)' % roc_auc)
             #lw=lw, label='ROC curve (area = {0.2f})'.format(roc_auc))
    plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic')
    plt.legend(loc="lower right")
    plt.show()   
    
def payoutcalc(outcomes,bets,odds,bet_probs=None,strategy="uniform",ride_p=5):
    
    payout = 0
    nullodds = 0

    if strategy == "uniform":
        
        if len(outcomes) == 0:
            return 0,0
        
        else:

            for i in range(len(outcomes)):
                if outcomes[i] == bets[i]:
                    if np.isnan(odds[i]) == True:
                        nullodds += 1
                        payout + 1
                    else:
                        payout += (odds[i]-1)
                else:
                    payout -= 1

            return payout, (payout/len(outcomes))*100
    
    elif strategy == "ride":
        start_bank = 5000
        bank = 5000
        for i in range(len(outcomes)):
            bet = (ride_p/100)*bank
            if outcomes[i] == bets[i]:
                if np.isnan(odds[i]) == True:
                    nullodds += 1
                    bank += bet
                else:
                    bank += bet*(odds[i]-1)
            else:
                bank -= bet
                
        return bank-start_bank, ((bank-start_bank)/start_bank)*100
    
    elif strategy == "adjride":
        start_bank = 5000
        bank = 5000
        for i in range(len(outcomes)):
            if np.isnan(odds[i]) == True:
                nullodds += 1
                bet_p = 0.02
            else:
                if (bet_probs[i]*odds[i]) < 1:
                    bet_p = 0.01
                else:
                    bet_p = (((bet_probs[i]*odds[i])-1)/(odds[i]-1))/5
                if bet_p > 0.1:
                    bet_p = 0.1
            bet = bet_p*bank
            if outcomes[i] == bets[i]:
                if np.isnan(odds[i]) == True:
                    bank += bet
                else:
                    bank += bet*(odds[i]-1)
            else:
                bank -= bet
                
        return bank-start_bank, ((bank-start_bank)/start_bank)*100
                
    else:
        print("strategy not found")
                
    print("nullodds:",nullodds)
    

def prob_cal(results_df,predict_prob1_col,predict_prob0_col,type_="sigmoid"):
    
    all_probs = np.concatenate([results_df[predict_prob1_col],results_df[predict_prob0_col]])
    all_outcomes = np.concatenate([results_df["outcome1"],results_df["outcome0"]])
    
    all_probs = all_probs.reshape(-1,1)
    all_outcomes
    
    if type_ == "sigmoid":
        mod = linear_model.LogisticRegression (fit_intercept=True, penalty='l2')
    mod.fit(all_probs,all_outcomes)
    
    results_df = pd.DataFrame(results_df)  
    
    probcal_slope = (mod.predict_proba(1)[:,1] - mod.predict_proba(0)[:,1])[0]
    
    def safe_calibration(x,prob_col):
        safe_x_intercept = 0.5
        if probcal_slope < 1:
            if x[prob_col] >= safe_x_intercept:
                return mod.predict_proba(x[prob_col])[0,1]
            if x[prob_col] < safe_x_intercept:
                return x[prob_col]
        else:
            print("ERROR - probcal_slope > 1")
    
    if type_ == "sigmoid":
        results_df["emp_probabilities0"] = results_df.apply(safe_calibration,axis=1,prob_col=predict_prob0_col)
        results_df["emp_probabilities1"] = results_df.apply(safe_calibration,axis=1,prob_col=predict_prob1_col)

    plt.scatter(all_probs,all_outcomes)
    plt.plot(np.linspace(0,1,22),mod.predict_proba(np.linspace(0,1,22).reshape(-1, 1))[:,1])
    plt.plot([0,1],[0,1],'r--')
    plt.scatter(results_df[predict_prob0_col],results_df["emp_probabilities0"])
    plt.scatter(results_df[predict_prob1_col],results_df["emp_probabilities1"])
    plt.show()    
        
    #print(mod.predict_proba(np.linspace(0,1,2).reshape(-1, 1))[:,1])
    print('probcal_slope:',probcal_slope)
    
    return results_df, probcal_slope

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
    
def modeleval_pred(outcomes,preds,pred_probs1):
    
    #conf matrix and roc_curve based on model predictions (i.e. most likely predicted outcome)
    
    conf_matrix = pd.DataFrame( metrics.confusion_matrix(preds,outcomes) )
    print(conf_matrix)
    
    auc = metrics.roc_auc_score(outcomes, pred_probs1)
    roc_curve(outcomes, pred_probs1)


def modeleval_payouts(outcomes,bets,bet_odds,bet_probs,ride_p=1):
    
    #payout analysis based on bets (i.e. choice picks greater for payout_odds * predicted win probability)
    
    payout_uni, payout_uni_p = payoutcalc(outcomes,bets,bet_odds,strategy="uniform")
    print('Payout (Uniform Betting): $',payout_uni,'and',payout_uni_p,'%')
    payout_ride, payout_ride_p = payoutcalc(outcomes,bets,bet_odds,strategy="ride",ride_p=ride_p)
    print('Payout (Ride Betting): $',payout_ride,'and',payout_ride_p,'%')
    payout_adjride, payout_adjride_p = payoutcalc(outcomes,bets,bet_odds.fillna(2),bet_probs,strategy="adjride")
    print('Payout (AdjRide Betting): $',payout_adjride,'and',payout_adjride_p,'%')
    
def betprep(finalresults):
    
    finalresults["area1"] = finalresults.apply(take_area,axis=1,probs_col="probabilities1",odds_col="odds1")
    finalresults["area0"] = finalresults.apply(take_area,axis=1,probs_col="probabilities0",odds_col="odds0")
    finalresults["bet"] = finalresults.apply(take_bet, axis=1,area0_col="area0",area1_col="area1")
    finalresults["betodds"] = finalresults.apply(take_betattr, axis=1,area0_col="area0",area1_col="area1",return0_col="odds0",return1_col="odds1")
    finalresults["betprobs"] = finalresults.apply(take_betattr, axis=1,area0_col="area0",area1_col="area1",return0_col="probabilities0",return1_col="probabilities1")
    finalresults["betarea"] = finalresults.apply(take_betattr, axis=1,area0_col="area0",area1_col="area1",return0_col="area0",return1_col="area1")
    """
    finalresults["emp_area1"] = finalresults.apply(take_area,axis=1,probs_col="emp_probabilities1",odds_col="odds1")
    finalresults["emp_area0"] = finalresults.apply(take_area,axis=1,probs_col="emp_probabilities0",odds_col="odds0")
    finalresults["emp_bet"] = finalresults.apply(take_bet, axis=1,area0_col="emp_area0",area1_col="emp_area1")
    finalresults["emp_betodds"] = finalresults.apply(take_betattr, axis=1,area0_col="emp_area0",area1_col="emp_area1",return0_col="odds0",return1_col="odds1")
    finalresults["emp_betprobs"] = finalresults.apply(take_betattr, axis=1,area0_col="emp_area0",area1_col="emp_area1",return0_col="emp_probabilities0",return1_col="emp_probabilities1")
    finalresults["emp_betarea"] = finalresults.apply(take_betattr, axis=1,area0_col="emp_area0",area1_col="emp_area1",return0_col="emp_area0",return1_col="emp_area1")
    """
    finalresults["pred_odds"] = finalresults.apply(take_predodds, axis=1, preds_col="predictions")
    finalresults["pred_probs"] = finalresults.apply(take_betattr, axis=1,area0_col="probabilities0",area1_col="probabilities1",return0_col="probabilities0",return1_col="probabilities1")

    
def areathresholdanalysis(results_df,outcomes_col,bets_col,odds_col,probabilities_col,area_col,ride=5):
    area_thresholds = np.linspace(0.9,1.25,8)

    thresh_payouts = {"thresh":[],"uni":[],"ride"+str(ride):[],"adjride":[],"uni_p":[],"ride"+str(ride)+"_p":[],"adjride_p":[], "n_games":[]}
    for threshold in area_thresholds:
        thresh_payouts["thresh"].append(threshold)
        tempresults_df = results_df[results_df[area_col] >= threshold].reset_index()

        payout_uni, payout_uni_p = payoutcalc(tempresults_df[outcomes_col],tempresults_df[bets_col],tempresults_df[odds_col].fillna(2),strategy="uniform")
        thresh_payouts["uni"].append(np.round(payout_uni,2))
        thresh_payouts["uni_p"].append(np.round(payout_uni_p,2))

        payout_ride, payout_ride_p = payoutcalc(tempresults_df[outcomes_col],tempresults_df[bets_col],tempresults_df[odds_col].fillna(2),strategy="ride",ride_p=ride)
        thresh_payouts["ride"+str(ride)].append(np.round(payout_ride,2))
        thresh_payouts["ride"+str(ride)+"_p"].append(np.round(payout_ride_p,2))

        payout_adjride, payout_adjride_p = payoutcalc(tempresults_df[outcomes_col],tempresults_df[bets_col],tempresults_df[odds_col].fillna(2),tempresults_df[probabilities_col],strategy="adjride")
        thresh_payouts["adjride"].append(np.round(payout_adjride,2))
        thresh_payouts["adjride_p"].append(np.round(payout_adjride_p,2))
        
        n_games = len(tempresults_df)
        thresh_payouts["n_games"].append(n_games)
    
    """with open('foo.csv', 'a') as f:
        exp_outcomes"""

    """    
    plt.figure(1,tight_layout=True,figsize=(6,4))
    plt.subplot(2,3,3)
    plt.plot(thresh_payouts["thresh"], thresh_payouts["uni"])
    plt.title("uniform")
    plt.subplot(2,3,2)
    plt.plot(thresh_payouts["thresh"], thresh_payouts["ride"+str(ride)])
    plt.title("ride"+str(ride))
    plt.subplot(2,3,1)
    plt.plot(thresh_payouts["thresh"], thresh_payouts["adjride"])
    plt.title("adjride")
    plt.subplot(2,3,6)
    plt.plot(thresh_payouts["thresh"], thresh_payouts["uni_p"])
    plt.subplot(2,3,5)
    plt.plot(thresh_payouts["thresh"], thresh_payouts["ride"+str(ride)+"_p"])
    plt.subplot(2,3,4)
    plt.plot(thresh_payouts["thresh"], thresh_payouts["adjride_p"])
    plt.show()
    plt.close()
    """
    return thresh_payouts
   