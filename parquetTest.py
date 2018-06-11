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
import os


#%%

os.chdir('/Users/luis.dealba/X/training')
#%%

df = pd.DataFrame({'one': [-1, np.nan, 2.5],
'two': ['foo', 'bar', 'baz'],
'three': [True, False, True]},
index=list('abc'))


#%%
table = pa.Table.from_pandas(df) 


#%%
pq.write_table(table, 'parquetTable.test')


#%% 
f = pq.ParquetFile('commits.parquet')

table = pq.read_table('commits.parquet', columns=['age','delay'])

f.num_row_groups

tmp = f.read_row_group(0).to_pandas()

#%%

p = table.to_pandas()


#%%
test = pq.read_table(table)


#%% Create a parquet with 4 row groups

df = pd.DataFrame(np.random.randint(0,10000,size=(10000, 4)), columns=list('ABCD'))

writer = pq.ParquetWriter('example2.parquet', pa.Table.from_pandas(df).schema)

#%%
for i in range(4):
    s = 2500
    f = s * i
    t = (s * (i+1)) - 1
    writer.write_table( pa.Table.from_pandas(df.iloc[f:t,]) )

#%%
writer.close()


#%%

pf2 = pq.ParquetFile('example2.parquet')
pf2.num_row_groups

tmp = pf2.read_row_group(1).to_pandas()







