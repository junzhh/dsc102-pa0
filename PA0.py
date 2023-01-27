# include your import statements here
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
import json


def PA0(user_reviews_csv):
    client = Client()
    client = client.restart()

    # ensure to use the parameter name i.e user_reviews_csv instead of hardcoding the filename inside read_csv func
    # for eg. dd.read_csv('user_reviews_Release.csv') is hardcoding and incorrect
    # instead leave it as dd.read_csv(user_reviews_csv) that is the parameter as set in the function signature.
    df = dd.read_csv(user_reviews_csv)
    df2 = df.drop(['reviewText','summary','unixReviewTime','reviewerName'],axis=1)
    df2['reviewing_since']=df2['reviewTime'].apply(lambda x: x[-4:],meta=('reviewTime', object)).astype(int)
    df2['helpful_votes']=df2['helpful'].apply(lambda x:x.strip('][').split(', ')[0],meta=('helpful', object)).astype(int)
    df2['total_votes']=df2['helpful'].apply(lambda x:x.strip('][').split(', ')[1],meta=('helpful', object)).astype(int)
    n1=df2.groupby('reviewerID').agg({"asin": "count",'overall':'mean','reviewing_since':'min','helpful_votes':'sum','total_votes':'sum'})
    n1=n1.reset_index()
    n1 = n1.rename(columns={'asin':'number_products_rated','overall':"avg_ratings"})

    # ensure that you have replaced <YOUR_USERS_DATAFRAME> with your final dataframe.
    submit = n1.describe().compute().round(2)    
    with open('results_PA0.json', 'w') as outfile: json.dump(json.loads(submit.to_json()), outfile)

# do not type anything outside this function, we would be calling your function from a different python notebook
# and passing in our hidden dataset as the parameter, which should then function correctly.