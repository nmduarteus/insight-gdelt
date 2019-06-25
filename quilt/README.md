# GDELT +

# What is it

GDELT is a very powerful dataset that is described as:
> "an initiative to construct a catalog of human societal-scale behavior and beliefs across all countries of the world, connecting every person, organization, location, count, theme, news source, and event across the planet into a single massive network that captures what's happening around the world, what its context is and who's involved, and how the world is feeling about it, every single day."

GDELT, although containing data computed based on powerful algorithms, is missing some useful information - the articles themselves. 

The data here presented has been enriched with the articles in order to provide better insights and analysis to what is happening around the world.

# Installation
Install quilt:
```sh
$ pip install quilt==2.9.15
```

Then install this package:
```sh
$ quilt install nmduarte/gdelt
```

# Usage

After installing the package, you just need to import it like a package
```python
from quilt.data.nmduarte import gdelt
```
# Examples
Data is provided as panda dasets. Below, exampkles on how to get data.
## Events data
```python
events = gdelt.events()
```
## Mentions data
```python
mentions = gdelt.mentions()
```

## News data
```python
news = gdelt.news()
```
