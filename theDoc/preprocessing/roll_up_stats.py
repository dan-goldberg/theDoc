import numpy as np
import pandas as pd



class Rollup(object):
    
    def __init__(self, columns, buckets, top_level_buckets, minimum_thresholds):
        self.target_buckets, self.parent_buckets = self.prepare_target_and_parent_buckets(buckets, top_level_buckets)
        self.minimum_thresholds = minimum_thresholds
        self.columns = columns
        
        
    def rollup(self, df, side='', detail=''):
        """
        Modifies df in place
        """
        for target_bucket, parent_bucket in zip(self.target_buckets, self.parent_buckets):
            self.bucket_rollup(df, target_bucket, parent_bucket, side, detail)
        return True
            
        
    def bucket_rollup(self, df, target_bucket, parent_bucket, side='', detail=''):
        target_list = []
        parent_list = []
        minimum_threshold = self.minimum_thresholds[target_bucket]
        for col in self.columns:
            if side == '' or detail == '':
                target_list.append("{}_{}".format(col, target_bucket))
            else:
                target_list.append("{}_{}_{}_{}".format(col, target_bucket, side, detail))
            if parent_bucket is None:
                parent_list.append(None)
            else:
                if side == '' or detail == '':
                    parent_list.append(col+'_'+parent_bucket)
                else:
                    parent_list.append("{}_{}_{}_{}".format(col, parent_bucket, side, detail))
        for target_col, parent_col in zip(target_list,parent_list):
            if side == '' or detail == '':
                thresh_col = 'pa_{}'.format(target_bucket)
            else:
                thresh_col = 'pa_{}_{}_{}'.format(target_bucket, side, detail)
            df[target_col] = df.apply(self.rollup_withthresh, axis=1, args=(target_col, thresh_col, minimum_threshold, parent_col)) 
           

    def rollup_withthresh(self, df, target_col, thresh_col, thresh_min, parent_col):
        if df[thresh_col] or 0 < thresh_min:
            if parent_col is None:
                return None
            else:
                return df[parent_col]
        else:
            return df[target_col] 
        
        
    def prepare_target_and_parent_buckets(self, buckets, top_level_buckets):
        """
        - buckets must be in groups from most granular to most general i.e. 
            [
            'last10_days', 'last30_days', 'last60_days',
            'last10_days_rh', 'last30_days_rh', 'last60_days_rh',
            'last10_days_lh', 'last30_days_lh', 'last60_days_lh'
            ]
        - top_level_buckets specifies buckets without parents
        """
        target_buckets = np.flip(np.array(buckets),0)
        parent_buckets = np.roll(target_buckets, 1)
        parent_buckets[np.isin(target_buckets, top_level_buckets)] = None
        
        def none_string_to_none(value):
            if value == 'None':
                return None
            else:
                return value
        parent_buckets = [none_string_to_none(bucket) for bucket in parent_buckets]
        return target_buckets, parent_buckets
    

class RollupFactory(object):
    
    def __init__(self, kind, **kwargs):
        
        if kind == 'custom':
            self.columns = columns
            self.buckets = buckets
            self.top_level_buckets = top_level_buckets
            self.minimum_thresholds = minimum_thresholds
            
        elif kind == 'batter':
    
            self.columns = [
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

            self.buckets = [
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

            self.top_level_buckets = [
                'std3',
                'std3_rh',
                'std3_lh'
            ]

            self.minimum_thresholds = {  
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
            
        elif kind == 'pitcher':
            
            self.columns = [
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

            self.buckets = [
                'last20',
                'last60',
                'std',
                'std2',
                'std3'
            ]
            
            self.top_level_buckets = [
                'std3'
            ]
            
            self.minimum_thresholds = {
                'last20':40,
                'last60':70,
                'std':90,
                'std2':90,
                'std3':90
            }

        
    def make_rollup(self):
        return Rollup(self.columns, self.buckets, self.top_level_buckets, self.minimum_thresholds)
        
        

