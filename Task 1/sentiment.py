from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re


def entities_to_csv(entities):
    csv = ""
    for entity in entities:
        temp = str(entity.text) + "," + str(entity.label_)
        csv = csv + temp + "\n"
    return csv


def remove_hastags(text):
    result = re.sub("#[a-zA-Z0-9_]+", "", text)
    return result


def remove_mentions(text):
    result = re.sub("@[a-zA-Z0-9_]+", "", text)
    return result


def remove_links(text):
    result = re.sub(r"http\S+", "", text)
    result = re.sub(r"www.\S+", "", result)
    return result


def clean_tweet(text):
    temp = remove_hastags(text)
    temp = remove_mentions(temp)
    temp = remove_links(temp)
    return temp


def sentiment_score(sentence):
    sid_obj = SentimentIntensityAnalyzer()
    sentiment_dict = sid_obj.polarity_scores(sentence)
    negative_sent_percent = sentiment_dict['neg']*100
    neutral_sent_precent = sentiment_dict['neu']*100
    positive_sent_precent = sentiment_dict['pos']*100
    csv = ""
    csv = csv + "Negative" + "," + str(negative_sent_percent) + "%" + "\n"
    csv = csv + "Neutral" + "," + str(neutral_sent_precent) + "%" + "\n"
    csv = csv + "Postiive" + "," + str(positive_sent_precent) + "%" + "\n"
    csv = csv + "Overall" + ","
    if sentiment_dict['compound'] >= 0.05:
        csv = csv + "Positive"
    elif sentiment_dict['compound'] <= -0.05:
        csv = csv + "Negative"
    else:
        csv = csv + "Neutral"
    csv = csv + "\n"
    return csv


if __name__ == "__main__":
    client: MongoClient = MongoClient('127.0.0.1', 27017)
    db = client['Assignment1']
    tweets = db['Tweets']
    csv = ""
    for t in tweets.find({}, {"body": 1, "link": 1}):
        tweet_text = t['body']
        tweet_text = clean_tweet(tweet_text)
        csv = sentiment_score(tweet_text)
        tweets.update_one({"link": t["link"]},
                          {"$set": {"Sentiment": csv}})
