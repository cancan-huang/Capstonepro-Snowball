#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import pickle
import glob,os

# 第一步：从文件夹中读取数据
# import data from files
def impor_data(docpath,docs):
    '''
    固定的path路径是：r'./popcmdata'
    docs = "popularposts1-150*.csv"或者其他，可以用正则表达式进行替换
    结果是一张大表
    '''
    path=docpath # the location of the dataset
    file=glob.glob(os.path.join(path,docs)) # filename
    # to concat the dataframes
    dl= []
    for f in file:
        dl.append(pd.read_csv(f))
    stock_commentsp1 =pd.concat(dl,axis = 0, ignore_index = True)
    return stock_commentsp1

# 第二步： 对数据进行处理
# data preprocess：评论数据总表处理 part1：

def raw_dataprocess(rawdf):
    '''
    input: the data being processed
    output: processed new dataframe
    此函数处理之后的数据，还存在着日期信息不能用的问题
    '''
    # get the stock code
    rawdf['stock_code'] = rawdf['页面网址'].str[21:29]
    # get the crowd member uid
    rawdf['user_uid'] =  rawdf['用户_链接'].str[19:]
    # change the column name
    rawdf.rename(columns={'用户':'user_name', '内容':'content','来源':'post_time','时间':'retweet_num','时间2':'reply_num','时间2':'reply_num','时间3':'like_num'}, inplace = True)
    rawdf_new =  rawdf[['stock_code','user_name','user_uid','post_time','content','retweet_num','reply_num','like_num']]
    # extract numbers from several columns
    cg_ls = ['retweet_num','reply_num','like_num']
    for i in cg_ls:
        rawdf_new[i] = rawdf_new[i].str.extract(r'(\d+)')
    # split the comment time
    rawdf_new['post_md'] = rawdf_new['post_time'].str.split(' ',expand = True).iloc[:,0]
    rawdf_new['post_hm'] = rawdf_new['post_time'].str.split(' ',expand = True).iloc[:,1]
    rawdf_new1 = rawdf_new[['stock_code','user_name','user_uid','post_md','post_hm','content','retweet_num','reply_num','like_num']]
    return  rawdf_new1


# 第二步，将时间数据变得可用
def date_process(datadf):
    '''
    处理时间数据，在上面所得到的rawdf的基础上进行处理
    具体的所需要替换的字段，可能需要根据实际的情况进行修改
    时间字段的处理要根据所采集时间和日期来进行变化
    '''
    # 此行后续的时间也需要修改
    datadf['post_md'] = datadf['post_md'].str.replace(r"修改于", "").str.replace(r"今天", "07-05").str.replace(r"昨天", "07-04").str.replace(r"分钟前", "").str.replace(r"小时前", "")
    for i in range(0,len(datadf['post_md'])):
        if len(datadf['post_md'][i]) == 5:
            datadf['post_md'][i] = '2020-' + datadf['post_md'][i]
        elif len(datadf['post_md'][i]) < 3:
            datadf['post_md'][i] = '2020-07-05'  # 后续时间需要修改
    datadf['post_md'] = pd.to_datetime(datadf['post_md'])
    return datadf # 得到被完整处理之后的数据
