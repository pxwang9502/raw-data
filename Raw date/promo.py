# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 17:03:38 2018
wangpx
@author: jm04
"""

import pandas as pd
import numpy as np
import pymysql.cursors
import time
from datetime import *
from dateutil.parser import parse 
import random

#生成节假日power
def date_power(current_date):
    conn = pymysql.connect(host='rm-uf6f7sl8tkx54io55.mysql.rds.aliyuncs.com',
		port=3306,
		user='jm04',
		password='Arc@201804',
		db= 'originaldata',
		charset='utf8mb4',
		cursorclass=pymysql.cursors.DictCursor)
    power = pd.read_sql("select power from festival where date = '%s'"%(current_date),conn)
    a = len(power['power'])
    if a != 0:
        power = power['power'][0]
    else:
        power = 1
    return(power)


#生成促销的宽表，包括销量变化情况（上升，下降），销售额，商品积压情况，商品上下架情况
def promo_product(current_date):
    two_weeks_ago = current_date - timedelta(days = 14) #用于计算前个星期销量
    one_weeks_ago = current_date - timedelta(days = 7)#用于计算上个星期销量
    up_date = current_date - timedelta(days = 5) #用于筛选五天之内上架的产品
    off_date = current_date + timdelta(days = 5) #用于筛选五天之内下架的产品
    conn = pymysql.connect(host='rm-uf6f7sl8tkx54io55.mysql.rds.aliyuncs.com',
                           port=3306,
                           user='jm04',
                           password='Arc@201804',
                           db= 'originaldata',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)    
    product = pd.read_sql("select\
                              p.product_key\
                              ,p.upshelf_date\
                              ,p.offshelf_date\
                              ,ql.quantity_last_week\
                              ,qt.quantity_two_week\
                              ,i.status\
                        from\
                            product as p\
                            left join (select \
                                           sod1.product_key\
                                           ,sum(sod1.quantity) as quantity_last_week\
                                        from\
                                            sales_orders as so1\
                                            left join sales_order_details as sod1\
                                            on so1.order_key = sod1.order_key\
                                        where \
                                            so1.order_Date between '%s' and '%s'\
                                        group by sod1.product_key\
                                        ) as ql\
                            on p.product_key = ql.product_key\
                            left join (select\
                                           sod2.product_key\
                                           ,sum(sod2.quantity) as quantity_two_week\
                                        from\
                                             sales_orders as so2\
                                             left join sales_order_details as sod2\
                                             on so2.order_key = sod2.order_key\
                                        where\
                                            so2.order_Date between '%s' and '%s'\
                                        group by sod2.product_key\
                                        ) as qt\
                            on p.product_key = qt.product_key\
                            left join inventory as i\
                            on p.product_key = i.product_key\
                        where\
                            p.upshelf_date <= '%s' and p.off_shelf_date >= '%s'\
                        order by \
                            ql.quantity_last_week"%(one_week_ago,current_date,one_week_ago,two_week_ago,current_data,current_date),conn)
    product = product.fillna('')
    change = [] #产品销量情况 ，上升下降
    up = []
    off = []
    a = 0
    b = len(product)
    while a < b:
        last_quantity = product['quantity_last_week'][a]
        two_quantity = product['quantity_two_week'][a]
        if last_quantity != '' and two_quantity != '':
            if last_quantity >= two_quantity:
                change.append('up')
            else:
                change.append('down')
        else:
            change.append('')
        up_shelf = product['upshelf_date'][a]
        if up_shelf > up_date:
            up.append(1)
        else:
            up.append('')
        off_shelf = product['off_shelf_date'][a]
        if off_shelf < off_date:
            off.append(1)
        else:
            off.append('')
        a = a + 1
    product['change'] = change
    product['up'] = up
    product['off'] = off
    return(product)
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    