#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  7 16:55:02 2018

@author: luis.dealba
"""
#%%
 
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa

#%%

df = pd.DataFrame({'one': [-1, np.nan, 2.5],
'two': ['foo', 'bar', 'baz'],
'three': [True, False, True]},
index=list('abc'))


#%%
table = pa.Table.from_pandas(df) 


#%%
