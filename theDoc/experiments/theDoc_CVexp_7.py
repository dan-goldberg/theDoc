
# coding: utf-8

# In[1]:

import random
from keras.regularizers import l2, l1
import pandas as pd

from theDoc.experiments import theDoc_expTools
from theDoc import settings


## set version ##

VERSION = '7.0.5'

datamaster = theDoc_expTools.DataMaster(version=VERSION, expdir='{}/experiments/outcomes/'.format(settings.base_pkg_path))

print('Data init complete')

#### hyperparameters ####

hyp = {}
hyp['learning_rate'] = random.uniform(0.0005,0.02)
hyp['momentum'] = None #random.uniform(0.0,0.6)
hyp['batch_size'] = random.choice([32,64,128])
hyp['loss_weights'] = [{
                         'dummyscore': random.uniform(0.2,1)
                        ,'dummydif':       random.uniform(0.2,1)
                        ,'dummytots':      random.uniform(0.2,1)
                        ,'winner':         random.uniform(0.2,1)
                      }]
hyp['num_hiddenunits'] = random.randint(64,2048)
hyp['num_hiddenlayers'] = random.choice([2,3,5,7])
hyp['dropout'] = random.uniform(0.35,0.5)
hyp['non_linearity'] = random.choice(['relu','tanh'])
hyp['weight_decay'] = l2(random.uniform(0.0000,0.001))

hyp['num_ensemblemodels'] = 4

print(hyp,'\n')

datamaster.hyp = hyp

#### ####


# In[2]:

tempgameresults = theDoc_expTools.GameResults(get_results=False)

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

all_outcomes = [outcome0_money,outcome1_money,outcome_odds0_money,outcome_odds1_money
               ,outcome0_spread,outcome1_spread,outcome_odds0_spread,outcome_odds1_spread
               ,outcome0_total,outcome1_total,outcome_odds0_total,outcome_odds1_total
               ,outcome0_points_spread,outcome1_points_spread,outcome_points_total
               ] + scores_cols + tempgameresults.dummyscore_cols + tempgameresults.dummytots_cols + scorefunc_cols + tempgameresults.dummyhomescore_cols + tempgameresults.dummyawayscore_cols


datamap = datamaster.covariates.index
datamaster.targets = datamaster.targets[all_outcomes]

exp_outcomes_all_columns = [
    'batch_size', 
    'dropout', 
    'learning_rate', 
    'momentum',
    'loss_weights',
    'non_linearity', 
    'num_ensemblemodels', 
    'num_hiddenlayers',
    'num_hiddenunits', 
    'weight_decay', 
    'MONEY_0.9',
    'MONEY_0.9500000000000001', 
    'MONEY_1.0', 
    'MONEY_1.05', 
    'MONEY_1.1',
    'MONEY_1.15', 
    'MONEY_1.2', 
    'MONEY_1.25', 
    'MONEY_valmax', 
    'MONEY_idxmax',
    'MONEY_allpred_payout', 
    'MONEY_auc', 
    'TOTAL_0.9',
    'TOTAL_0.9500000000000001', 
    'TOTAL_1.0', 
    'TOTAL_1.05', 
    'TOTAL_1.1',
    'TOTAL_1.15', 
    'TOTAL_1.2', 
    'TOTAL_1.25', 
    'TOTAL_valmax', 
    'TOTAL_idxmax',
    'TOTAL_allpred_payout', 
    'TOTAL_auc', 
    'SPREAD_0.9',
    'SPREAD_0.9500000000000001', 
    'SPREAD_1.0', 
    'SPREAD_1.05', 
    'SPREAD_1.1',
    'SPREAD_1.15', 
    'SPREAD_1.2', 
    'SPREAD_1.25', 
    'SPREAD_valmax',
    'SPREAD_idxmax', 
    'SPREAD_allpred_payout', 
    'SPREAD_auc', 
    'basetest',
    'basetrain', 
    'baseval', 
    'diftest', 
    'diftrain', 
    'difval', 
    'totstest',
    'totstrain', 
    'totsval', 
    'winnertest', 
    'winnertrain', 
    'winnerval',
    'flow_flag'
    ]


# In[3]:




kfold = theDoc_expTools.kfold_cv()
kfold.run_kfold_cv(datamaster,n_splits=2)

kfold.money_df = kfold.money_df.sample(frac=1)
kfold.dummytots_df = kfold.dummytots_df.sample(frac=1)
kfold.dummyscore_df = kfold.dummyscore_df.sample(frac=1)

theDoc_expTools.betting_analysis(kfold)
kfold.exp_outcomes_all = kfold.exp_outcomes_all.join(pd.DataFrame(kfold.losses_cv,index=[0]))
kfold.exp_outcomes_all = kfold.exp_outcomes_all[exp_outcomes_all_columns]

theDoc_expTools.write_results(kfold)


# In[ ]:




# In[ ]:




# In[ ]:



