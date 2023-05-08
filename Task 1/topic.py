import re
import nltk
import gensim
from pymongo import MongoClient
from gensim.models.ldamulticore import LdaMulticore
from gensim import corpora, models
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer, WordNetLemmatizer


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


def remove_punctuation(word):
    result = re.sub(r"[^\w]+", "", word)
    return result


def remove_stopwords(text):
    result = []
    text = word_tokenize(text)
    for word in text:
        if word not in stopwords.words("english"):
            result.append(word)
    return result


def stem_words(text):
    ps = PorterStemmer()
    result = []
    for word in text:
        word = remove_punctuation(word)
        result.append(ps.stem(word))
    return result


def clean_tweet(text):
    temp = remove_hastags(text)
    temp = remove_mentions(temp)
    temp = remove_links(temp)
    lemmatizer = WordNetLemmatizer()
    clean_text = []
    stopword = stopwords.words("english")
    list = temp.split()
    for text in list:
        text = remove_punctuation(text)
        text = " ".join(
            [lemmatizer.lemmatize(word, pos='v')
             for word in word_tokenize(text, language='english')
             if not word in set(stopword) and len(word) > 3])
        clean_text.append(text)
    return clean_text
    # temp_list = remove_stopwords(temp)
    # temp_list = stem_words(temp_list)


def only_empty_strings(word_list):
    for word in word_list:
        if len(word) > 0:
            return False
    return True


def make_biagram(data, tokens):
    # higher threshold fewer phrases.
    bigram = gensim.models.Phrases(data, min_count=5, threshold=100)
    bigram_mod = gensim.models.phrases.Phraser(bigram)
    return [bigram_mod[doc] for doc in tokens]


def topic_modelling(data):
    # Tokens
    tokens = []
    for text in data:
        text = word_tokenize(text)
        tokens.append(text)

    # Make Biagrams
    tokens = make_biagram(data=data, tokens=tokens)

    # Corpora Dictionary
    dictionary = corpora.Dictionary(tokens)

    # Creating Document Term Matrix
    doc_term_matrix = [dictionary.doc2bow(doc) for doc in tokens]

    # Training The LDA Model
    lda_model = gensim.models.LdaModel(doc_term_matrix,  # Document Term Matrix
                                       num_topics=1,  # Number of Topics
                                       id2word=dictionary,  # Word and Frequency Dictionary
                                       # Number of passes throw the corpus during training (similar to epochs in neural networks)
                                       passes=10,
                                       chunksize=10,  # Number of documents to be used in each training chunk
                                       # Number of documents to be iterated through for each update.
                                       update_every=1,
                                       alpha='auto',  # number of expected topics that expresses
                                       per_word_topics=True,
                                       random_state=42)
    # remove numbers, asterisk and quotation marks
    word_list = re.findall(r'"(.+?)"', lda_model.print_topic(0))
    weights = re.findall("\d+\.\d+", lda_model.print_topic(0))
    csv = ""
    for word, weight in zip(word_list, weights):
        temp = str(word) + "," + str(weight)
        csv = csv + temp + "\n"
    return csv


if __name__ == "__main__":
    client: MongoClient = MongoClient('127.0.0.1', 27017)
    db = client['Assignment1']
    tweets = db['Tweets']
    csv = ""
    for t in tweets.find({}, {"body": 1, "link": 1}):
        tweet_text = t['body']
        clean_text = clean_tweet(tweet_text)
        if not only_empty_strings(clean_text):
            csv = topic_modelling(clean_text)
        else:
            csv = ""
        tweets.update_one({"link": t["link"]},
                          {"$set": {"Topic": csv}})
