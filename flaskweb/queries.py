get_all_categories = """select * from cameo_categories order by 1"""

get_all_subcategories = """select * from cameo_subcategories order by 1"""

get_countries_with_most_events = """select country_code, total from top_events order by total desc limit 10"""

most_mentions_per_month = """select val, total, year_month from results where query_name='top_mentions' order by total desc limit 10"""

avg_tone_per_month = """select year_month::text, avg("avg_tone") as tone from top_mentions group by year_month"""

types_across_time = """select 
cameo_categories.description,
sum(total),
to_char(to_timestamp (substr(year_month,5,2)::text, 'MM'), 'TMmon') as m	
from 
top_mentions 
left join cameo_categories on substr(event_code,1,2)=cameo_categories.code
where substr(year_month,1,4)='2019' and code is not null
group by 
cameo_categories.description,
year_month"""

top_channels = """select * from top_channels limit 3"""

top_words = """select * from news_to_show order by count desc limit 200"""
