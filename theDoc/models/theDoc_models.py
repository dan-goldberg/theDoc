
# coding: utf-8

# In[39]:

import numpy as np
import sys
from keras.models import Sequential, Model
from keras.layers import Dense, Dropout, Activation, normalization, recurrent, Input, wrappers, Masking, core
from keras.layers.merge import concatenate as mergeconcatenate
from keras.layers.merge import Add as mergeadd
from keras.regularizers import l2, l1
from keras.callbacks import EarlyStopping, ReduceLROnPlateau, ModelCheckpoint
from keras.optimizers import SGD, Adam
from keras.initializers import RandomNormal, RandomUniform, Ones, Zeros, VarianceScaling
from keras import backend as K
from scipy.signal import convolve, correlate

from theDoc import settings

class DocModels:
    
    def __init__(self):
        
        self.hyperparameters_3_0 = {
            'num_inputfeatures':4045,
            'loss':'categorical_crossentropy',
            'learning_algo':Adam,   
            'learning_rate': 0.5,
            'batch_size': 64,
            'loss_weights':{'dummyhomescore':0.5,'dummyawayscore':0.5,'dummydif':0.0,'dummytots':0.0,'winner':0.0},
            'num_hiddenunits':64,
            'num_hiddenlayers':1,
            'dropout':0.5,
            'non_linearity':'relu',
            'weight_decay_type':l2,
            'weight_decay':0.005
        }
        self.hyperparameters_6_0 = {
            'num_inputfeatures':4045,
            'loss':'categorical_crossentropy',
            'learning_algo':SGD,   
            'learning_rate': 0.034950753,
            'batch_size': 128,
            'loss_weights':{'dummydif': 0.6752352820343319, 'winner': 0.7526669373556736, 'dummytots': 0.903580034385116, 'dummyscore': 1.3391695876980987},
            'num_hiddenunits':84,
            'num_hiddenlayers':3,
            'dropout':0.225192879,
            'non_linearity':'relu',
            'weight_decay_type':l2,
            'weight_decay':0.0089436136,
            'momentum':0.4
        }
        
        self.saved_models = {}
        
        # THE FOLLOWING MODELS COME FROM EXPERIMENT 3.0 (x4)
        self.saved_models[1] = {'model':'not built','build_func':self.build_model_100,'weights_path':'{}/1_weights.hdf5'.format(settings.MODELS_PATH),'tasks':{'total'},'hyperparameters':self.hyperparameters_3_0}
        self.saved_models[2] = {'model':'not built','build_func':self.build_model_100,'weights_path':'{}/2_weights.hdf5'.format(settings.MODELS_PATH),'tasks':{'total'},'hyperparameters':self.hyperparameters_3_0}
        self.saved_models[3] = {'model':'not built','build_func':self.build_model_100,'weights_path':'{}/3_weights.hdf5'.format(settings.MODELS_PATH),'tasks':{'total'},'hyperparameters':self.hyperparameters_3_0}
        self.saved_models[4] = {'model':'not built','build_func':self.build_model_100,'weights_path':'{}/4_weights.hdf5'.format(settings.MODELS_PATH),'tasks':{'total'},'hyperparameters':self.hyperparameters_3_0}
        
        
        # THE FOLLOWING MODELS COME FROM EXPERIMENT 6.0 - A (x4)
        self.saved_models[5] = {'model':'not built','build_func':self.build_model_200,'weights_path':'{}/5_weights.hdf5'.format(settings.MODELS_PATH),'tasks':{'money','total','spread'},'hyperparameters':self.hyperparameters_6_0}
        self.saved_models[6] = {'model':'not built','build_func':self.build_model_200,'weights_path':'{}/6_weights.hdf5'.format(settings.MODELS_PATH),'tasks':{'money','total','spread'},'hyperparameters':self.hyperparameters_6_0}
        self.saved_models[7] = {'model':'not built','build_func':self.build_model_200,'weights_path':'{}/7_weights.hdf5'.format(settings.MODELS_PATH),'tasks':{'money','total','spread'},'hyperparameters':self.hyperparameters_6_0}
        self.saved_models[8] = {'model':'not built','build_func':self.build_model_200,'weights_path':'{}/8_weights.hdf5'.format(settings.MODELS_PATH),'tasks':{'money','total','spread'},'hyperparameters':self.hyperparameters_6_0}
    
    def build_savedmodel(self,n):
    
        self.saved_models[n]['model'] = self.saved_models[n]['build_func'](self.saved_models[n]['hyperparameters'])
    
    def build_allsavedmodels(self):
    
        for n in self.saved_models:
            self.saved_models[n]['model'] = self.saved_models[n]['build_func'](self.saved_models[n]['hyperparameters'])
            
    def display_model(self,model):
        
        import pydotplus as pydot
        from IPython.display import SVG, display
        from keras.utils.vis_utils import model_to_dot

        display(SVG(model_to_dot(model).create(prog='dot', format='svg')))
        
        
        
        
        
        
    ######### MODELS ###########        
        
    
    
    def build_model_100(self,hyperparameters):
        
        seed = None
        np.random.seed(None)

        model = None

        num_hiddenunits = hyperparameters['num_hiddenunits']
        hidden_layers = hyperparameters['num_hiddenlayers']

        drop = hyperparameters['dropout']
        act = hyperparameters['non_linearity']
        #bias_initializer = 'zeros'
        kernel_initializer=RandomNormal(mean=0.0, stddev=0.0001, seed=None)
        kernel_regularizer = hyperparameters['weight_decay_type'](hyperparameters['weight_decay'])
        activity_regularizer = l2(0.00)
        bnorm_kwargs = {'axis':-1, 'momentum':0.99, 'epsilon':0.001, 'center':True, 'scale':True
                                         , 'beta_initializer':'zeros', 'gamma_initializer':'ones'
                                         , 'moving_mean_initializer':'zeros'
                                         , 'moving_variance_initializer':'ones', 'beta_regularizer':None
                                         , 'gamma_regularizer':None, 'beta_constraint':None, 'gamma_constraint':None}

        dense_kwargs = {'kernel_initializer':kernel_initializer, 'kernel_regularizer':kernel_regularizer}

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

        def batchcorrelate(ia,ib):
            assert ia.shape[0] == ib.shape[0]
            out = []
            for n in range(ia.shape[0]):
                a = ia[n,:]
                b = ib[n,:]
                out.append(correlate(a,b))
            return np.array(out)

        def batchconvolve(ia,ib):
            assert ia.shape[0] == ib.shape[0]
            out = []
            for n in range(ia.shape[0]):
                a = ia[n,:]
                b = ib[n,:]
                out.append(convolve(a,b))
            return np.array(out)

        def k_batchcorrelate(inp_list):
            out = K.tf.py_func(batchcorrelate,inp_list, K.tf.float32,stateful=False)
            out.set_shape((None,inp_list[0].shape[-1]+inp_list[1].shape[-1]-1))
            return out
        def k_batchcorrelate_shape(input_shape):
            return (None,input_shape[0][-1]+input_shape[1][-1] - 1)

        def k_batchconvolve(inp_list):
            out = K.tf.py_func(batchconvolve,inp_list, K.tf.float32,stateful=False)
            out.set_shape((None,inp_list[0].shape[-1]+inp_list[1].shape[-1]-1))
            return out

        def k_batchconvolve_shape(input_shape):
            return (None,input_shape[0][-1]+input_shape[1][-1] - 1)

        sys.setrecursionlimit(10000)

        num_hoao_inputfeatures = hyperparameters['num_inputfeatures']

        try: assert hidden_layers % 2 != 0
        except: 
            print('ERROR: number of hidden layers must be odd')
            raise

        ###########################################################

        ### NEURAL ###
        ### NETWORK ###

        VisibleLayer = {}
        for x in ['ho','ao']:
            VisibleLayer[x] = Input(shape=(num_hoao_inputfeatures,), name=x+'_input')

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

        output_dummyhomescore = Dense(26, activation = 'softmax', bias_initializer='zeros', name='dummyhomescore')(ho_repfin_drop)
        output_dummyawayscore = Dense(26, activation = 'softmax', bias_initializer='zeros', name='dummyawayscore')(ao_repfin_drop)

        batchcorrelate_layer = core.Lambda(k_batchcorrelate,output_shape=k_batchcorrelate_shape, name='dummydif')
        output_dummyruns = batchcorrelate_layer([output_dummyhomescore,output_dummyawayscore])

        batchconvolve_layer = core.Lambda(k_batchconvolve,output_shape=k_batchconvolve_shape, name='dummytots')
        output_dummytots = batchconvolve_layer([output_dummyhomescore,output_dummyawayscore])

        output_winner_layer = Dense(2, activation = 'linear',name='winner',trainable=False)
        output_winner = output_winner_layer(output_dummyruns)

        ###########################################################

        model = Model(inputs=[VisibleLayer['ho'],VisibleLayer['ao']],outputs=[output_dummyhomescore, output_dummyawayscore,output_dummyruns,output_dummytots,output_winner])

        model.compile(loss=hyperparameters['loss'],
                              optimizer=hyperparameters['learning_algo'](lr=hyperparameters['learning_rate'], beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.001)
                              ,metrics=['accuracy']
                              ,loss_weights=hyperparameters['loss_weights']
                              )
        
        # Set weights for untrainable layers

        ones = np.ones(25).reshape(-1,1)
        zeros = np.zeros(25).reshape(-1,1)
        bottom = np.concatenate([ones,zeros],axis=1)
        top = np.concatenate([zeros,ones],axis=1)
        middle = np.array([0,0]).reshape(1,2)

        winner_weights = np.concatenate([top,middle,bottom],axis=0)
        winner_biases = middle.reshape(2,)
        output_winner_layer.set_weights([winner_weights,winner_biases])            
        
        return model    
    
    
    
    
    def build_model_200(self,hyperparameters):

        seed = None
        np.random.seed(None)

        model = None

        num_hiddenunits = hyperparameters['num_hiddenunits']
        hidden_layers = hyperparameters['num_hiddenlayers']

        drop = hyperparameters['dropout']
        act = hyperparameters['non_linearity']
        #bias_initializer = 'zeros'
        kernel_initializer=VarianceScaling(scale=1.0, mode='fan_in', distribution='normal', seed=None)
        kernel_regularizer = hyperparameters['weight_decay_type'](hyperparameters['weight_decay'])
        activity_regularizer = l2(0.00)
        bnorm_kwargs = {'axis':-1, 'momentum':0.99, 'epsilon':0.001, 'center':True, 'scale':True
                                         , 'beta_initializer':'zeros', 'gamma_initializer':'ones'
                                         , 'moving_mean_initializer':'zeros'
                                         , 'moving_variance_initializer':'ones', 'beta_regularizer':None
                                         , 'gamma_regularizer':None, 'beta_constraint':None, 'gamma_constraint':None}

        dense_kwargs = {'kernel_initializer':kernel_initializer, 'kernel_regularizer':kernel_regularizer}

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

        def batchcorrelate(ia,ib):
            assert ia.shape[0] == ib.shape[0]
            out = []
            for n in range(ia.shape[0]):
                a = ia[n,:]
                b = ib[n,:]
                out.append(correlate(a,b))
            return np.array(out)

        def batchconvolve(ia,ib):
            assert ia.shape[0] == ib.shape[0]
            out = []
            for n in range(ia.shape[0]):
                a = ia[n,:]
                b = ib[n,:]
                out.append(convolve(a,b))
            return np.array(out)

        def k_batchcorrelate(inp_list):
            out = K.tf.py_func(batchcorrelate,inp_list, K.tf.float32,stateful=False)
            out.set_shape((None,inp_list[0].shape[-1]+inp_list[1].shape[-1]-1))
            return out
        def k_batchcorrelate_shape(input_shape):
            return (None,input_shape[0][-1]+input_shape[1][-1] - 1)

        def k_batchconvolve(inp_list):
            out = K.tf.py_func(batchconvolve,inp_list, K.tf.float32,stateful=False)
            out.set_shape((None,inp_list[0].shape[-1]+inp_list[1].shape[-1]-1))
            return out

        def k_batchconvolve_shape(input_shape):
            return (None,input_shape[0][-1]+input_shape[1][-1] - 1)

        sys.setrecursionlimit(10000)

        num_hoao_inputfeatures = hyperparameters['num_inputfeatures']

        try: assert hidden_layers % 2 != 0
        except: 
            print('ERROR: number of hidden layers must be odd')
            raise

        ###########################################################

        ### NEURAL ###
        ### NETWORK ###

        VisibleLayer = {}
        for x in ['ho','ao']:
            VisibleLayer[x] = Input(shape=(num_hoao_inputfeatures,), name=x+'_input')

        HiddenCell = {}
        HiddenCell['layers'], HiddenCell['tensors'] = {}, {}
        for n in range(hidden_layers):
            nn = str(n+1)
            HiddenCell['layers']['rep'+nn] = Dense(num_hiddenunits, name = 'rep'+nn, **dense_kwargs)
            #HiddenCell['layers']['rep'+nn+'_norm'] = normalization.BatchNormalization(**bnorm_kwargs)
            HiddenCell['layers']['rep'+nn+'_act'] = Activation(act)
            HiddenCell['layers']['rep'+nn+'_drop'] = Dropout(drop)
            if n > 0 and n%2 == 0: HiddenCell['layers']['rep'+nn+'_add'] = mergeadd()

            for x in ['ho','ao']:
                if n == 0: inp = VisibleLayer[x]
                elif n < 3 or n%2 == 0: inp = HiddenCell['tensors'][x+'_rep'+str(n)+'_drop']
                else: inp = HiddenCell['tensors'][x+'_rep'+str(n)+'_add']
                HiddenCell['tensors'][x+'_rep'+nn] = HiddenCell['layers']['rep'+nn](inp)
                #HiddenCell['tensors'][x+'_rep'+nn+'_norm'] = HiddenCell['layers']['rep'+nn+'_norm'](HiddenCell['tensors'][x+'_rep'+nn])
                #HiddenCell['tensors'][x+'_rep'+nn+'_act'] = HiddenCell['layers']['rep'+nn+'_act'](HiddenCell['tensors'][x+'_rep'+nn+'_norm'])
                HiddenCell['tensors'][x+'_rep'+nn+'_act'] = HiddenCell['layers']['rep'+nn+'_act'](HiddenCell['tensors'][x+'_rep'+nn])     ### USE THIS FOR NO BATCH NORM
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

        batchcorrelate_layer = core.Lambda(k_batchcorrelate,output_shape=k_batchcorrelate_shape, name='dummydif_pre')
        output_dummyruns_pre = batchcorrelate_layer([output_dummyhomescore,output_dummyawayscore])
        output_dummyruns_mid = Dense(256, activation = act ,name='dummydif_mid')(output_dummyruns_pre)
        output_dummyruns = Dense(51, activation = 'softmax',name='dummydif')(output_dummyruns_mid)

        batchconvolve_layer = core.Lambda(k_batchconvolve,output_shape=k_batchconvolve_shape, name='dummytots')
        output_dummytots = batchconvolve_layer([output_dummyhomescore,output_dummyawayscore])

        output_winner_layer = Dense(2, activation = 'linear',name='winner_pre',trainable=False)
        output_winner_pre = output_winner_layer(output_dummyruns_pre)
        output_winner_mid = Dense(48, activation = act, name = 'winner_mid')(output_winner_pre)
        output_winner = Dense(2, activation = 'softmax', name = 'winner')(output_winner_mid)

        ###########################################################
        
        model = Model(inputs=[VisibleLayer['ho'],VisibleLayer['ao']],outputs=[output_dummyhomescore, output_dummyawayscore,output_dummyruns,output_dummytots,output_winner])

        model.compile(loss=hyperparameters['loss'],
                              optimizer=hyperparameters['learning_algo'](lr=hyperparameters['learning_rate'], decay=0.001, momentum=hyperparameters['momentum'])
                              ,metrics=['accuracy']
                              ,loss_weights=hyperparameters['loss_weights']
                              )

        # Set weights for untrainable layers

        ones = np.ones(25).reshape(-1,1)
        zeros = np.zeros(25).reshape(-1,1)
        bottom = np.concatenate([ones,zeros],axis=1)
        top = np.concatenate([zeros,ones],axis=1)
        middle = np.array([0,0]).reshape(1,2)

        winner_weights = np.concatenate([top,middle,bottom],axis=0)
        winner_biases = middle.reshape(2,)
        output_winner_layer.set_weights([winner_weights,winner_biases])         
        
        return model

