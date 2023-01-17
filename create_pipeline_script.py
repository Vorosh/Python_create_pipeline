#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import getopt
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

if __name__ == '__main__':

    #Задаём входные параметры
    unixOptions = 'sdt:edt:'
    gnuOptions = ['start_dt=', 'end_dt=']
    
    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:] #excluding script name
    
    try:
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    except 
        getopt.error as err:
        print (str(err))
        sys.exit(2)
    
    start_dt = ''
    end_dt = ''
    for currentArgument, currentValue in arguments:
        if currentArgument in ('-sdt', '--start_dt'):
            start_dt = currentValue
        elif currentArgument in ('-edt', '--end_dt'):
            end_dt = currentValue
    
    db_config = {'user': 'my_user',         
                 'pwd': 'my_user_password', 
                 'host': 'localhost',       
                 'port': 5432,              
                 'db': 'games'}             
    
    connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                     db_config['pwd'],
                                 db_config['host'],
                                 db_config['port'],
                                 db_config['db'])
    engine = create_engine(connection_string)
    
    # Теперь выберем из таблицы только те строки,
    # которые были выпущены между start_dt и end_dt
    query = ''' SELECT *
            FROM data_raw
            WHERE year_of_release::TIMESTAMP BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
        '''.format(start_dt, end_dt)
    
    data_raw = pd.io.sql.read_sql(query, con = engine, index_col = 'game_id')

    columns_numeric = ['na_players', 'eu_players', 'jp_players', 'other_players',
                       'critic_score', 'user_score']
    columns_datetime = ['year_of_release']
    for column in columns_numeric: data_raw[column] = pd.to_numeric(data_raw[column], errors='coerce')
    for column in columns_datetime: data_raw[column] = pd.to_datetime(data_raw[column])
    
    data_raw['total_copies_sold'] = data_raw[['na_players',
                          'eu_players',
                          'jp_players',
                          'other_players']].sum(axis = 1)
    
    agg_games_year = data_raw.groupby('year_of_release').agg({'critic_score': 'mean',
                                  'user_score': 'mean',
                                  'total_copies_sold': 'sum'})
    
    agg_games_year = agg_games_year.rename(columns = {'critic_score': 'avg_critic_score',
                                                      'user_score': 'avg_user_score'})
    
    agg_games_year = agg_games_year.reset_index()
    
    #Удаляем старые записи между start_dt и end_dt
    query = '''DELETE FROM agg_games_year 
                   WHERE year_of_release BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
            '''.format(start_dt, end_dt)
    engine.execute(query)

    agg_games_year.to_sql(name = 'agg_games_year', con = engine, if_exists = 'append', index = False)