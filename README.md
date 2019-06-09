# GDELT +

# Introduction

GDELT is a very powerful dataset that is described as 
> "an initiative to construct a catalog of human societal-scale behavior and beliefs across all countries of the world, connecting every person, organization, location, count, theme, news source, and event across the planet into a single massive network that captures what's happening around the world, what its context is and who's involved, and how the world is feeling about it, every single day."

GDELT, although containing data computed based on powerful algorithms, is missing some useful information - the articles themselves. 

# GDELT - What is it ?
  - Global Database of Events, Language, and Tone (GDELT)
  - A Global Database of Society
  - Monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages
  - Identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events driving our global society every second of every day

  - Import a HTML file and watch it magically convert to Markdown
  - Drag and drop images (requires your Dropbox account be linked)

# Problem and Use Case
**Problem:**
  - A lot of analysis is already done to provide the data and classify if with a CAMEO code.
  - BUT there is data missing that is useful for further analysis - The content of each news (only URLs are provided)
  - Plus, one would have to use BigQuery or build a pipeline to get the data

**Use case:**

  - I’m a data scientist that wants to do a deeper analysis on a certain subset and I would like to easily push the data into my code. Also, for my analysis, I would require the full context of each news - ie, the text of such news.

# Proposed Solution

![flow](https://drive.google.com/uc?export=view&id=1kO6rtvgA0saNdfwkbM9lSjCoueZXegcf)

# Challenges

- Find or create a good news scrapper that would allow to get all this data from the different websites
- Build API or module to provide the most common queries to create abstraction layer to end user
- Digest and store massive amounts of data
