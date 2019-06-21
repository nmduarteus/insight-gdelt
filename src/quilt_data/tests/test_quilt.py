import quilt
from quilt.data.nmduarte import gdelt3
import pandas as pd


# get all the data from quilt
events = gdelt3.data.events()
data_with_news = gdelt3.data.data_with_news()
data_master = gdelt3.data.data_with_news_master()

# generate new data from the day
print(data_with_news)
# initialize list of lists

data = [['boi', 'do ','mato'], ['eu', 'sou','o ze'], ['querias', 'nao','era']]

# Create the pandas DataFrame
df = pd.DataFrame(data, columns=['SOURCEURL', 'NewsText','HashURL'])

final = data_with_news.append(df)

#print(data_with_news)
#print(df)
#print(final)

#gdelt3._set(["data","data_with_news"],df)
gdelt3._set(["data", "data_with_news"], df)
#quilt.build("nmduarte/gdelt3/data/data_with_news",final)

# print(ola['GLOBALEVENTID'])
quilt.push("nmduarte/gdelt3/data/data_with_news", is_public=True, is_team=False)
