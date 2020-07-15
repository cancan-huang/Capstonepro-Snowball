
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import pickle
import glob,os

# 最简单需求：随时查找单个股票的全部信息
# 可能需要的函数：输入股票的代号，可以看到数据范围的，unique值，对数据有一个基本的认识？排除一些太老了的股票
def find_single_stock(rawdf,colindex,cds):
    '''
    用来寻找单个股票的全部信息
    '''
    singlestock = rawdf.loc[rawdf[colindex] == cds]
    return singlestock


def find_old_stock(rawdf,times):
    '''
    此函数用于找到时间过于久远一定不要的股票
    同时将所有信息存成本地文档，进行筛选
    此函数也可以用来得到不要的股票的list，这样就可以再分割股票时，选择不储存这部分股票
    '''
    old_stocks = rawdf.loc[rawdf['post_md'] < times]
    old_stocks.to_csv('old_stocks.csv',encoding="utf_8_sig")
    return old_stocks

# 找到一定不要的股票list:通过人工的方式，剔除70个股票
# 已经找到要去掉的股票列表的，在进行分割之前，去掉确定排除的股票
# 将确定纳入考虑的股票进行分割
def find_use_stock(docs,num,rawdf):
    '''
    首先需要有一个文档，其中为数据透视表的统计结果
    注意文档中列名的设定，要将股票名称存为code，将频数存为count
    num为我们需要限定的频次
    rawdf是拥有所有股票名称的大表
    '''
    rmstockdf = pd.read_csv(docs)
    # find useless stocks list
    rm_stock_ls =  rmstockdf.loc[rmstockdf['count']>= num]['code'].tolist()
    # find all the stocks list
    all_stocks = rawdf['stock_code'].unique().tolist()
    # find the needed stocks list
    use_stock = []
    for i in all_stocks:
        if i not in rm_stock_ls:
            use_stock.append(i)
    return use_stock

# 确定要纳入考虑的股票被储存在use_stock中

# 创建独特用户的文档
def stock_unique_m(code_ls,rawdf):
    '''
    输入是要抓取的股票的代号
    输出是一个表，表中包括以下三列，分别是user_id，关注、粉丝和组合的link
    此函数要慎用，将产生大量的本地文件
    '''
    num = 1000
    for cs in code_ls:
        num += 1
        ddf = rawdf.loc[rawdf['stock_code'] == cs]
        ddf_new = ddf.drop_duplicates(['user_uid'])
        ddf_new['follow'] = 'https://xueqiu.com/u/' + ddf_new['user_uid'] + '#/follow'
        ddf_new['fans'] = 'https://xueqiu.com/u/' + ddf_new['user_uid'] + '#/fans'
        ddf_new['portfolio'] = 'https://xueqiu.com/u/' + ddf_new['user_uid'] + '#/portfolio'
        ddf_new = ddf_new.drop(['post_md','post_hm','content','retweet_num','reply_num','like_num'], axis=1)
        outfilename = str(num) + cs + '.csv'
        ddf_new.to_csv(outfilename,encoding="utf_8_sig")

# 在完成情感分析后，对股票数据进行分割
def stock_sentiment(code_ls,rawdf):
    '''
    输入是要抓取的股票的代号的一个列表
    输出是完成情感分析之后的表
    此函数要慎用，将产生大量的本地文件
    '''
    num = 1
    for cs in code_ls:
        num += 1
        ddf = rawdf.loc[rawdf['stock_code'] == cs]
        outfilename = str(num) + cs + '.csv'
        ddf.to_csv(outfilename,encoding="utf_8_sig")

# 优先跑follower 因为很有可能这些大V用户是互相关注的，但是粉丝基本都是新用户，所以可能借鉴意义并不大
# 因此对资源进行优先分配到following的抓取


# 在全表数据上面对股票进行情感判定和抽样，跑ml的模型复杂的爬取逻辑）
# 对以上数据进行处理和分析
