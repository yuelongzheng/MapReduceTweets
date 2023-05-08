from pymongo import MongoClient
from keybert import KeyBERT
import re
import string

WORD_RE = re.compile(r"[a-zA-Z]+")


def remove_hashtags(text):
    return re.sub("#[a-zA-Z0-9_]+", "", text)


def remove_mentions(text):
    return re.sub("@[a-zA-Z0-9_]+", "", text)


def remove_links(text):
    result = re.sub(r"http\S+", "", text)
    result = re.sub(r"www.\S+", "", result)
    return result


def remove_punctuation(text):
    return text.translate(str.maketrans('', '', string.punctuation))


def remove_non_english(text):
    result = ""
    for word in text:
        if word.isascii():
            result = result + word
    return result


def clean_tweet(text):
    temp = remove_hashtags(text)
    temp = remove_mentions(temp)
    temp = remove_links(temp)
    temp = remove_punctuation(temp)
    temp = temp.lower()
    temp = remove_non_english(temp)
    return temp


if __name__ == "__main__":
    client: MongoClient = MongoClient('127.0.0.1', 27017)
    db = client['Assignment1']
    tweets = db['Tweets']
    kw_model = KeyBERT()
    csv = ""

    for t in tweets.find({}, {"body": 1, "link": 1}):
        tweet_text = t['body']
        tweet_text = clean_tweet(tweet_text)
        keyword_list = kw_model.extract_keywords(tweet_text,
                                                 keyphrase_ngram_range=(
                                                     1, 1),
                                                 stop_words='english')
        temp_list = []
        for keyword, number in keyword_list:
            temp_list.append(keyword)
        csv = ','.join(temp_list)
        tweets.update_one({"link": t["link"]}, {"$set": {"keyword": csv}})
