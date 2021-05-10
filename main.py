import pandas as pd
import datetime
from elasticsearch import Elasticsearch, helpers

readData = pd.read_csv(r"C:\Users\eden.golan\netflix_titles.CSV", parse_dates=['date_added'])
# readData.info()

df = pd.DataFrame(data=readData)
df = df[(df['date_added']).dt.year >= 2016]
df = df.assign(current_date=datetime.datetime.now(),
               number_of_catagories=df['listed_in'].apply(lambda n: len(n.split(','))))

df.loc[df['duration'].str.contains('min'), 'duration_in_seconds'] = \
    df['duration'].str.replace(r'\D+', '', regex=True).astype(int) * 60
df.loc[df['duration'].str.contains('Season'), 'duration_in_seconds'] = \
    df['duration'].str.replace(r'\D+', '', regex=True).astype(int) * 18000

avgPerDirector = df.pivot_table(index=['director'], values=['duration_in_seconds'], aggfunc='mean')

df["rating"].fillna('No Data', inplace=True)

df1 = df.to_dict('records')
# es.indices.create(index='netflix_show')
es = Elasticsearch()


def generator(data):
    for c, line in enumerate(data):
        yield {
            '_index': 'netflix_show',
            '_type': 'dataset',
            '_id': line.get('show_id'),
            '_source': {
                'show_id': line.get('show_id',),
                'duration_in_seconds': line.get('duration_in_seconds'),
                'rating': line.get('rating'),
                'type': line.get('type'),
                'release_year': line.get('release_year')
            }
        }


# gen = generator(df1)

try:
    res = helpers.bulk(es, generator(df1))
    print('Working')
except Exception as e:
    print('Not Working: ', e)
