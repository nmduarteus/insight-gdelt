CREATE TABLE news_to_show (
	word varchar NULL,
	total_count int4 NULL
);

CREATE TABLE top_channels (
	mention_source_name varchar NULL,
	total int4 NULL
);

CREATE TABLE top_events (
	country_code varchar(3) NOT NULL,
	total int8 NOT NULL
);

CREATE TABLE top_mentions (
	globaleventid int8 NOT NULL,
	total int8 NOT NULL,
	year_month varchar(6) NOT NULL,
	avg_tone int8 NOT NULL,
	event_code varchar(32) NOT NULL,
	year_month_int int4 NULL
);
CREATE INDEX top_mentions_year_month_idx ON top_mentions USING btree (year_month);

CREATE MATERIALIZED VIEW avg_tone_view
AS
    select year_month::text, avg("avg_tone") as tone from top_mentions group by year_month
WITH DATA;


CREATE MATERIALIZED VIEW news_to_show_view
AS
    select news_to_show.word, news_to_show.count from news_to_show order by count desc limit 200
WITH DATA;
