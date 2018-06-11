#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 11 14:52:41 2018

@author: luis.dealba
"""
#%%
from kafka import KafkaProducer

#%%


#bootstrap_servers='localhost:9092'
producer = KafkaProducer(bootstrap_servers='localhost:9092')

#%%

for i in range(1):
    producer.send('pTest', b'fin mensaje')

#%%
    
from kafka import KafkaConsumer

#%%

consumer = KafkaConsumer('pTest', bootstrap_servers='localhost:9092')


#%%
for msg in consumer:
    print(msg)
    if ( (msg.value)==b'cerrar' ):
        break