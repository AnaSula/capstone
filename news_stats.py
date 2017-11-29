

import pandas as pd
import numpy as np
import re
from urllib import parse
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())

'''
read news_stats data 

query general: "select email, timestamp, event, url, marketing_campaign_name as newsletter_title
from newsletter  
where date_format(from_unixtime(timestamp), '%Y-%m-%d') 
between '2017-08-08' and '2017-08-2017' "

________________________________________________________

query opens: select count(*) as opens, date_format(from_unixtime(timestamp), '%m-%d-%Y') as newsletter_date, marketing_campaign_name as newsletter_title
from newsletter  
where date_format(from_unixtime(timestamp), '%Y-%m-%d') 
between '2017-10-09' and '2017-10-15' and event='open' group by marketing_campaign_id
order by opens desc limit 5

_______________________________________________________

query top 20 openers: select count(*) as opens, email
from newsletter  
where date_format(from_unixtime(timestamp), '%Y-%m-%d') 
between '2017-10-09' and '2017-10-15' and event='open' AND
email not like ('%@cwt.com') and email not in ('jeremy.m.barth@gmail.com', 'jules@evolvelawnow.com')
group by email
order by opens desc limit 20

_______________________________________________________

query top 20 clickers: select count(*) as clicks, email
from newsletter  
where date_format(from_unixtime(timestamp), '%Y-%m-%d') 
between '2017-10-09' and '2017-10-15' and event='click' AND
email not like ('%@cwt.com') and email not in ('jeremy.m.barth@gmail.com', 'jules@evolvelawnow.com')
group by email
order by clicks desc limit 20
'''


stats=pd.read_csv("~/desktop/newsletter.csv", sep=';')

stats=stats[stats.event== 'click']

#print(stats.dtypes)

#convert epoch timestamp to date

stats['click_date']=pd.to_datetime(stats['timestamp'], unit='s').dt.date



#subset news items only




#stats_news=stats.loc[stats['url'].str.contains("https://www.findknowdo.com/news/")==True]
stats_news=stats[stats.url.str.contains("https://www.findknowdo.com/news/")==True]




#cleanup url field

stats_news['news_title']=stats_news['url'].str.replace('\\?utm_campaign=Cabinet%20Newsletter&utm_source=Newsletter&utm_medium=Email', '')

stats_news['news_title']=stats_news['news_title'].str.replace('https://www.findknowdo.com/' , '')

#aggregate

stats_agg=stats_news.groupby(['news_title', 'newsletter_title']).size().reset_index(name='clicks')


#clean up url

url=pd.Series(stats_agg['news_title'])

for i in url:
	i=parse.unquote(i)

stats_agg['url']=url


#read the news items with metadata
news_db=pd.read_csv("~/desktop/news_nodes.csv")

#join datasets

q="select distinct title, newsletter_title, clicks, group_concat(comment_author) as comment_author, date_created, image, quote, search_weight as placement from stats_agg as a  join news_db as b  on a.url=b.url group by title;"

join=pysqldf(q)


#replace missing values
join['image'] = join['image'].apply(lambda x: 'No' if  np.isnan(x) else 'Yes')
join['quote'] = join['quote'].apply(lambda x: 'No' if  pd.isnull(x) else 'Yes')

join=join.sort_values('date_created')

join.to_csv("~/desktop/newsletter_stats/news_stats_10_23_10_29_2017.csv")


