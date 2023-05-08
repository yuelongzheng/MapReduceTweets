from pymongo import MongoClient
import re
import string


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


def write_text(text, filename):
    f = open(filename, "w+")
    f.writelines(text)
    f.close()


if __name__ == "__main__":
    client: MongoClient = MongoClient('127.0.0.1', 27017)
    db = client['Assignment1']
    tweets = db['Tweets']
    tweet_text = ""
    tweet_location = ""
    tweet_id = ""
    for t in tweets.find({}, {"id": 1, "body": 1, "actor": 1}):
        if 'location' in t['actor']:
            temp_location = t['actor']['location']['displayName']
            tweet_location = tweet_location + str(temp_location) + "\n"
        temp_text = clean_tweet(t['body'])
        tweet_text = tweet_text + temp_text + '\n'
        temp_id = t['id']
        id_split = temp_id.split(":")
        # Just get the numbers at the end
        temp_id = id_split[len(id_split)-1]
        tweet_id = tweet_id + temp_id + '\n'
    write_text(tweet_text, "tweet_text.txt")
    write_text(tweet_location, "tweet_location.txt")
    write_text(tweet_id, "tweet_id.txt")
