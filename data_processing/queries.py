mostmentions_query = """select
	events.GLOBALEVENTID,
	count(mentions.GLOBALEVENTID) as total,
	events.MonthYear as year_month,
	events.AvgTone as avg_tone,
	events.EventCode as event_code,
	events.year_month_int as year_month_int
from events
left join mentions on events.GLOBALEVENTID=mentions.GLOBALEVENTID
group by
	events.GLOBALEVENTID,
	events.year_month_int,
	events.MonthYear,
	events.AvgTone,
	events.EventCode
order by count(mentions.GLOBALEVENTID) desc"""

events_query = """select
	a.country_code,
	a.total_events + b.total_events as total
	from
(select
	Actor1CountryCode as country_code,
	count(*) as total_events
from events
where Actor1CountryCode is not null
group by Actor1CountryCode) a inner join
(select
	Actor2CountryCode as country_code,
	count(*) as total_events
from events
where Actor2CountryCode is not null
group by Actor2CountryCode) b
on a.country_code = b.country_code
order by a.total_events + b.total_events desc"""

top_channels_query = """select MentionSourceName as mention_source_name,
count(MentionSourceName) as total from mentions group by MentionSourceName
order by count(*) desc limit 10"""