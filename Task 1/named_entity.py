from pymongo import MongoClient
import spacy
import re


WORD_RE = re.compile(r"[a-zA-Z0-9']+")


def entities_to_csv(entities):
    csv = ""
    for entity in entities:
        entity_text = " ".join(WORD_RE.findall(str(entity.text)))
        if len(entity_text) > 1:
            temp = entity_text + "," + str(entity.label_)
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


if __name__ == "__main__":
    client: MongoClient = MongoClient('127.0.0.1', 27017)
    db = client['Assignment1']
    tweets = db['Tweets']
    nlp = spacy.load("en_core_web_sm")
    csv = ""
    for t in tweets.find({}, {"body": 1, "link": 1}):
        tweet_text = t['body']
        tweet_text = clean_tweet(tweet_text)
        article = nlp(tweet_text)
        csv = entities_to_csv(article.ents)
        tweets.update_one({"link": t["link"]},
                          {"$set": {"Named Entities": csv}})
