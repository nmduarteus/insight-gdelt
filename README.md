# GDELT +

# Introduction

GDELT is a very powerful dataset that is described as: 
> "an initiative to construct a catalog of human societal-scale behavior and beliefs across all countries of the world, connecting every person, organization, location, count, theme, news source, and event across the planet into a single massive network that captures what's happening around the world, what its context is and who's involved, and how the world is feeling about it, every single day."

# GDELT - What is it ?
  - Global Database of Events, Language, and Tone (GDELT)
  - A Global Database of Society
  - Monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages
  - Identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events driving our global society every second of every day

# Problem
**Problem:**
  - A lot of analysis is already done to provide the data and classify if with a CAMEO code.
  - BUT there is data missing that is useful for further analysis - The content of each news (only URLs are provided)
  - Plus, one would have to use BigQuery or build a pipeline to get the data

**Solution:**

  - Enriched Data: Enrich the data by getting all the news' articles and bring them in play.
  - Make it easy: Let's provide a simple and easy way to access the data and keep track of data versions.
  - Make it visirual: Provide a simple dashboard with trends in the news
 

# Pipeline & Tech Stack

![flow](https://drive.google.com/uc?export=view&id=1UnUkaHOI_4hQ6UOorLhtGFuc-IgV14XA)

# Challenges
**Queries' Performance:**

- 300Gb of data with heavy joins
    - Filter early
    - Select only what is needed
    - Used hashed columns to join
    
**Queries' Performance:**

- 15 min to download, scrape, clean and process data
    - Find and tweak good news' scrapers
    - Make usage of auto scaling groups
    - Improve write process to PostgreSLQ
    
**Free Quilt Limitations:**

- No S3, No Parquet for public registry
    - Build dataframes and append them on the fly to Quilt
    - Used CSV instead of Parquet (but to be changed)
    
# Slides & Demo
- Project slides can be found [here](http://bit.ly/gdeltplus)
- Dashboard can be found [here](http://datadigest.club)
- Jupyter Notebook can be found [here](http://www.datadigest.club:8888/notebooks/jup/GDELT%2B.ipynb)    
    
   
