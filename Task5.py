from pymongo import MongoClient
import dask.bag as daskb
import pandas as pd
import numpy as np
N = 5
if __name__ == "__main__":
    client: MongoClient = MongoClient('127.0.0.1', 27017)
    db = client['Assignment1']
    tweets = db['Tweets']
    keyword_list = []
    for t in tweets.find({}, {"keyword": 1}):
        keywords = t['keyword']
        keywords = keywords.split(",")
        for keyword in keywords:
            keyword_list.append(keyword)
    # remove any duplicate keywords
    keyword_list = list(dict.fromkeys(keyword_list))
    words_list = []
    count_once = []
    with open('tweet_text.txt') as f:
        contents = f.readlines()
        for line in contents:
            temp_list = line.split()
            if len(temp_list) > 0:
                words_list = words_list + temp_list
            for word in temp_list:
                counted = []
                if word not in counted:
                    count_once.append(word)
                    counted.append(word)
    words = daskb.from_sequence(words_list)
    word_count = dict(words.frequencies())
    doc_freq = dict(daskb.from_sequence(
        count_once).frequencies())
    keyword_count = {k: v for k, v in word_count.items()
                     if k in keyword_list}
    keyword_doc_freq = {k: v for k, v in doc_freq.items()
                        if k in keyword_list}
#    df = pd.DataFrame.from_dict([word_count])
    df = pd.DataFrame(keyword_count.items(),
                      columns=['Word', 'Frequency'])
    total_words_count = words.count().compute()
    df['Term-Frequency'] = df['Frequency'] / total_words_count
    df1 = pd.DataFrame(keyword_doc_freq.items(),
                       columns=['Word', 'Document-Appearance'])
    df = df.merge(df1, how='left', on='Word')
    no_of_doc = 10000
    df['idf'] = np.log(no_of_doc/df['Document-Appearance'])
    df['tfidf'] = df['Term-Frequency'] * df['idf']
    df = df.sort_values(by=['tfidf'], ascending=False)
    # Show only word and tfidf
    df1 = df[['Word', 'tfidf']]
    print(df1.head(N))
