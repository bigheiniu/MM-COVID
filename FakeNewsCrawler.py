import configparser
import logging
import time
from argparse import ArgumentParser
from random import shuffle
from pymongo import MongoClient, UpdateOne
from TwitterCrawlerHelper import twitter_adv_search_twint, get_tweet_details
from TwitterCrawlerHelper import fetch_user_meta_info, fetch_user_tweets, \
    fetch_user_follower_ids, fetch_user_friends_ids
from util import Constants
from util.CrawlerUtil import get_all_users_by_news_source
from util import TwarcConnector
from util.Util import get_collection_db, chunkify, multiprocess_function, \
    MultiManager, init_path, is_epoch_time_in_interval, get_urls_in_text, \
    TweetSearchEntry
import numpy as np
import dateparser
import traceback
from tqdm import tqdm
from bson.int64 import Int64
from datetime import datetime
from NewsCrawler.NewsCrawler import get_news

# ATTENTION: In previous version the Fake label is stored from true category.

key_words_list = ['sars-cov-2','covid-19',"coronavirus", "covid"]

def get_database_connection(config):
    host = config['MongoDB']['host']
    port = int(config['MongoDB']['port'])
    db_name = config['MongoDB']['database_name']

    client = MongoClient(host, port)
    db = client[db_name]
    return db

def init_logging(config):
    format = '%(asctime)s %(process)d %(module)s %(levelname)s %(message)s'
    # format = '%(message)s'
    logging.basicConfig(
        filename='{}/fake_news_crawler_{}.log'.format(config["Logging"]["log_dir"], str(int(time.time()))),
        level=logging.INFO,
        format=format)
    logging.getLogger('requests').setLevel(logging.CRITICAL)


def load_configuration(config_file):
    """Gets the configuration file and returns the dictionary of configuration"""
    filename = config_file
    config = configparser.ConfigParser()
    config.read(filename)

    return config


# ATTENTION: change
def fetch_save_news_tweets(db, twarc_connector):
    global config
    """
    Fetch fake tweets for the title of the formatted  news and use twitter advanced search to get the fake tweets and save them
    :return:
    """
    logging.info("Fetching tweets using Twitter Advanced API...")
    news_collection = db[Constants.NEWS_COLLECTION]
    news_tweet_relation = db[Constants.NEWS_TWEET_RELATION]
    tweet_search_entries = []
    for use_url in [True, False]:
        # focus on langauge
        langs = ["hi", 'fr', 'it', 'pt', 'es', 'en']
        for news in news_collection.find({"lang":{"$in": langs}, "label":"real"}):
            retrieved_tweets = news_tweet_relation.find_one({"news_id": news['news_id']})
            if "agency" not in news.keys():
                news['agency'] = news['news_id'].split("-")[0]
            if "lang" not in news.keys():
                news['lang'] = None
            try:
                if "time" in news.keys():
                    time = str(dateparser.parse(news['time'])).split(" ")
                elif "date" in news.keys():
                    time = str(dateparser.parse(news['date'])).split(" ")
                else:
                    time = None
            except:
                continue
            if retrieved_tweets is None or len(retrieved_tweets['tweet_list']) == 0:
                if use_url:
                    try:
                        if news['ref_source_url'] == "NONE":
                            continue
                        news['statement'] = news['ref_source_url'].replace("https://", "")
                    except:
                        continue
                else:
                    if "statement" not in news.keys():
                        continue
                if "lang" not in news.keys():
                    news['lang'] = None
                if time is not None and use_url is False:
                    news['statement'] += " since:{}".format(time)
                tweet_search_entries.append(TweetSearchEntry.from_processed_news_article(news))

    print("Length {}".format(len(tweet_search_entries)))
    multiprocess_fetch_save_tweets(tweet_search_entries, twarc_connector, db)


def fetch_save_reliable_tweets_job(idx, tweet_search_entry_chunks, db):
    news_format = get_collection_db(db, 'real_news_format')
    tweet_collection = get_collection_db(db, 'real_tweet_collection_query')
    logging.info("Process %d started for fetch_save_reliable_tweets_job" % idx)
    tweet_serach_queries = tweet_search_entry_chunks[idx]
    try:
        for index, search_entry in enumerate(tweet_serach_queries):

            tweets = twitter_adv_search_twint(kws=search_entry['kw'], sc_name=search_entry['sc_name'], lang=search_entry['lang'], limit=300)
            for tweet in tweets:
                news_format_data = {'news_id': 'twitter-'+str(tweet['id']), 'statement':tweet['text'],'title':tweet['text'], 'url':tweet['id'],
                                 "news_source":"twitter", 'lang':search_entry['lang']}

                print(news_format_data)
                tweet['news_id'] = news_format_data['id']
                news_format.find_and_modify({"news_id":news_format_data['news_id']}, {"$set":news_format_data}, upsert=True)
                # tweet_collection.insert(tweet)

    except Exception as e:
        logging.error(e)
        print(e)
        print("pass")

    logging.info("Process %d completed for fetch_save_reliable_tweets_job" % idx)



import os
def multiprocess_fetch_save_tweets(tweet_search_entries, twarc_connector, db):
    num_process = os.cpu_count() - 3
    tweet_search_entry_chunks = chunkify(tweet_search_entries, num_process)

    multiprocess_function(num_process, fetch_save_tweets_job, (tweet_search_entry_chunks, twarc_connector, db))


def fetch_save_tweets_job(idx, tweet_search_entry_chunks, twarc_connector, db):
    tweet_serach_queries = tweet_search_entry_chunks[idx]
    logging.info("Process %d started for fetch_save_tweets_job %s samples" % (idx, len(tweet_serach_queries)))

    news_twitter_collection = db[Constants.NEWS_TWEET_RELATION]
    tweet_collection = db[Constants.TWEET_COLLECTION]
    try:
        for index, search_entry in tqdm(enumerate(tweet_serach_queries), desc="Process {}".format(idx)):
            tweets = twitter_adv_search_twint(search_entry.search_query, search_entry.lang, search_entry.since_date)
            logging.info("Found {} tweets for news id {} and statement {}".format(len(tweets), search_entry.news_id, search_entry.search_query))

            # TODO: If multiple fake news match to same tweet then fake news ids as array and update the objects accordingly
            # Tweets in collection as saved with candidate keys (Tweet ID + news_id)
            if len(tweets) == 0:
                continue

            if news_twitter_collection.find_one({"news_id": search_entry.news_id}) is None:
                news_twitter_collection.find_and_modify({"news_id": search_entry.news_id},
                                                        {"$set":{"tweet_list": [], "news_id": search_entry.news_id}}, upsert=True)
            tweets_ids = [i['id'] for i in tweets]
            news_twitter_collection.find_one_and_update({"news_id": search_entry.news_id}, {"$addToSet":{"tweet_list":
                                                                                            {"$each":tweets_ids}}})
            for tweet in tweets:
                tweet['lang'] = search_entry.lang
                tweet_collection.find_one_and_update({"id":tweet['id']}, {"$set":tweet}, upsert=True)


    except Exception as e:
        logging.error(e)
        print(e)
        print("pass")

    logging.info("Process %d completed for fetch_save_tweets_job" % idx)


def collect_tweets_from_urls(db, config):
    news_tweets = get_all_news_tweets(db, config)
    tweet_collection = db[Constants.TWEET_COLLECTION]

    urls_info = dict()
    try:
        for news_id, tweet_id in news_tweets:
            text = tweet_collection.find_one({"tweet_id":tweet_id})
            urls = get_urls_in_text(text)
            for url in urls:
                urls_info[url] = TweetSearchEntry(url, news_id, news_id.split("-")[0], None)

        tweet_url_search_entries = list(urls_info.values())
        # tweet_search_entries, db, fake, is_url
        print("tweets {} in Fetch URLs".format(len(tweet_url_search_entries)))
        multiprocess_fetch_save_tweets(tweet_url_search_entries, db)
    except:
        logging.error("ERROR in Fetch URLs")
        print("ERROR in Fetch URLs")
        pass

def get_all_news_tweets(db, config, small_lang=True):
    news_tweet_relation = db[Constants.NEWS_TWEET_RELATION]
    news_collection = db[Constants.NEWS_COLLECTION]
    tweet_tweet_relation = db[Constants.TWEET_TWEET_RELATION]

    news_tweets_filtered = []
    lang_list = ['es','pt','hi','fr','it', 'en']
    for lang in lang_list:
        # ATTENTION: Only for real twitter information
        for news_id in news_collection.find({"lang":lang},{"news_id":1}):
            news_id = news_id['news_id']
            news_tweet = news_tweet_relation.find_one({"news_id":news_id})
            if news_tweet is not None:
                for tweet_id in news_tweet["tweet_list"]:
                    tweet_id = int(tweet_id)
                    th = tweet_tweet_relation.find_one({"tweet_id": int(tweet_id)})
                    if th is not None:
                        if "tweet_replies" in th.keys() and len(th['tweet_replies']) > 0:
                            continue
                    news_tweets_filtered.append((news_tweet["news_id"], tweet_id))



    news_tweets_filtered = list(set(news_tweets_filtered))
    shuffle(news_tweets_filtered)
    print("The filtered news pieces")
    print(news_tweets_filtered)
    return news_tweets_filtered


def should_fetch_fake_tweet_info(db, tweet_id, append):

    # if fake:
    #     tweets_info_collection = get_collection_db(db, Constants.FAKE_TWEET_INFO_COLLECTION)
    # else:
    #     tweets_info_collection = get_collection_db(db, Constants.REAL_TWEET_INFO_COLLECTION)
    tweet_tweet_relation = db[Constants.TWEET_TWEET_RELATION]
    tweet_info = tweet_tweet_relation.find_one({Constants.TWEET_ID: tweet_id})
    if tweet_info is None:
        return True
    else:
        # return False
        return (False or append)
        created_at = tweet["created_at"]
        return is_epoch_time_in_interval(created_at, days=days_to_fetch)


def get_all_users(db, fake: bool):
    """Get the user ids involved with the tweets including social engagements"""
    users = set()

    if fake:
        tweet_collection = db["fake_tweet_collection"]
        tweet_info_collection = db["fake_tweet_info_coll"]
    else:
        tweet_collection = db["real_tweet_collection"]
        tweet_info_collection = db["real_tweet_info_coll"]

    tweet_ids = set()
    for tweet in tweet_collection.find():
        tweet_ids.add(tweet["id"])

    for tweet_info in tweet_info_collection.find():
        if tweet_info["id"] in tweet_ids:
            users.update(set(tweet_info["tweet_likes"]))

            # Get replies in all levels of reply
            users.update(get_users_involved_in_replies(tweet_info["tweet_replies"]))
            # users.update(set([comment["user"] for comment in tweet_info["tweet_replies"]]))
            users.update(set([comment["user"]["id"] for comment in tweet_info["tweet_retweets"]]))

    return list(users)


def get_users_involved_in_replies(replies: list):
    user_set = set()

    for reply in replies:
        if reply:
            user_set.update(get_users_in_reply(reply))

    return user_set


def get_users_in_reply(reply):
    user_set = set()

    if reply is None:
        return user_set

    if "user_id" in reply:
        user_set.add(reply["user_id"])

    if "engagement" in reply:
        user_set.update(get_users_in_reply(reply["engagement"]))

    if Constants.TWEET_REPLIES in reply:
        for reply_of_reply in reply[Constants.TWEET_REPLIES]:
            user_set.update(get_users_in_reply(reply_of_reply))

    return user_set


def fetch_save_news_tweets_details(db, config, twarc_conntector):
    """Fetch the recursive replies and retweets regarding the fake tweets and save them to database"""

    twarc_conntector.change_params(window_limit=2, time_window=8)
    news_tweets = get_all_news_tweets(db, config)
    num_process = int(config["Selenium"]["num_process"])
    news_tweets_chunks = chunkify(news_tweets, num_process)
    multiprocess_function(num_process, function_ref=fetch_save_news_tweets_details_job,
                          args=(news_tweets_chunks, config, twarc_conntector))



def fetch_save_news_tweets_details_job(idx, news_tweets_chunks, config, twarc_connector):
    logging.info("{} process crawling tweets details".format(idx))
    news_tweets = news_tweets_chunks[idx]

    db = get_database_connection(config)


    user_profile = db[Constants.USER_PROFILE_RELATION]
    tweet_tweet_relation = db[Constants.TWEET_TWEET_RELATION]
    tweets_collection = db[Constants.TWEET_COLLECTION]
    count = 0

    parent_id_list = []
    non_exist_tweet = []
    for news_id, tweet_id in tqdm(news_tweets, desc="{} Process".format(idx)):

        logging.info("News ID: {}, Tweets ID: {}".format(news_id, tweet_id))
        print("News ID: {}, Tweets ID: {}".format(news_id, tweet_id))
        try:

            tweet_id = int(tweet_id)
            tweet = tweets_collection.find_one({"id": str(tweet_id)})
            if tweet is None:
                tweet_int = tweets_collection.find_one({"id": int(tweet_id)})
                tweet = tweet_int
            if tweet is None:
                non_exist_tweet.append((news_id, int(tweet_id)))
                continue

            use_twint = True

            try:

                screen_name = tweet['username']
                if "created_at" in tweet.keys():
                    created_at = tweet['created_at']
                else:
                    created_at = tweet['raw']['datestamp']


                if "conversation_id" in tweet.keys():
                    conversation_id = tweet['conversation_id']
                elif "raw" in tweet.keys():
                    if "conversation_id" not in tweet['raw'].keys():
                        conversation_id = tweet['id']
                    else:
                        conversation_id = int(tweet['raw']['conversation_id'])
                else:
                    conversation_id = None
                    use_twint = False
                    if "in_reply_to_status_id" in tweet.keys():
                        parent_id = tweet['in_reply_to_status_id']
                        if parent_id is not None:
                            parent_id_list.append((news_id, parent_id))


                if conversation_id is not None and tweet_id != int(conversation_id):
                    parent_id_list.append((news_id, conversation_id))

                # check whether the code contains the number of replie and retweets
                if "n_retweets" in tweet.keys():
                    retweets_count = tweet['n_retweets']
                    replies_count = tweet['n_replies']
                elif "nretweets" in tweet.keys():
                    retweets_count = tweet['nretweets']

                    replies_count = 1
                elif "retweet_count" in tweet.keys():
                    retweets_count = tweet['retweet_count']
                    replies_count = 1
                else:
                    logging.error("This tweet {} does not have retweet count".format(tweet_id))
                    continue

                if 'lang' in tweet.keys():
                    lang = tweet['lang']
                else:
                    lang = None

                try:
                    created_at = datetime.fromtimestamp(created_at / 1000) if type(created_at) is Int64 else dateparser.parse(created_at)
                    if created_at is None:
                        created_at = dateparser.parse(" ".join(np.array(created_at.split())[[1, 2, -1]]))
                except:
                    logging.error("TIME ERROR At Tweet {}, ERROR INFO".format(tweet_id, created_at))
                    created_at = datetime.strptime('2020-04-09', '%Y-%m-%d')




                updated_info = get_tweet_details(tweet_id, twarc_connector, created_at, screen_name,lang, conversation_id,
                                                   user_collection=user_profile,tweet_collection=tweets_collection, tweet_relation_collection=tweet_tweet_relation,
                                                   get_retweets=retweets_count>0,get_replies=replies_count>0, flag=use_twint)



                if tweet_tweet_relation.find_one({"tweet_id":tweet_id}) is None:
                    tweet_tweet_relation.find_one_and_update({"tweet_id":tweet_id}, {"$set":{"tweet_id": tweet_id,
                                                                                             Constants.TWEET_REPLIES: [1], Constants.TWEET_RETWEETS: [1],
                                                                                             Constants.TWEET_LIKES: [1]}}, upsert=True)
                engagement = {key: {"$each": values} for key, values in updated_info.items()}
                print({"tweet_id":tweet_id,**engagement})
                logging.info("Mother Tweet {}".format({"tweet_id":tweet_id,**engagement}))
                tweet_tweet_relation.find_one_and_update({"tweet_id":tweet_id}, {"$addToSet":engagement})
                count += 1

            except Exception as e:
                logging.error(e)
                print(traceback.format_exc())
                print(str(e))



        except Exception as e:
            logging.error(e)
            print(traceback.format_exc())
            print(str(e))





def fetch_user_followers_friends(db, screen_name_user_id_list, twarc_connector):
    # twarc_connector.change_params(window_limit=15, time_window=900)


    user_profile_collection = db[Constants.USER_PROFILE_RELATION]
    existing_user_screen_name_with_followers = set([(i['screen_name'], i['user_id']) for i in user_profile_collection.find() if Constants.FOLLOWERS in i.keys()])
    existing_user_screen_name_with_followees = set([(i['screen_name'], i['user_id']) for i in user_profile_collection.find() if Constants.FOLLOWEES in i.keys()])



    for screen_name_user_id in screen_name_user_id_list:
        try:
            if screen_name_user_id not in existing_user_screen_name_with_followers or screen_name_user_id not in existing_user_screen_name_with_followees:
                twarc_connection, idx = twarc_connector.get_twarc_connection_new()
                profile_info = user_profile_collection.find_one({"screen_name":screen_name_user_id[0]}, {"profile_info":1})['profile_info']
                if screen_name_user_id not in existing_user_screen_name_with_followers:
                    followers = fetch_user_follower_ids(screen_name_user_id, profile_info['followers_count'], None ,use_twarc=False)
                    if len(followers) == 0 and profile_info['followers_count'] > 0:
                        try:
                            followers = fetch_user_follower_ids(screen_name_user_id, profile_info['followers_count'],
                                                                twarc_connection, use_twarc=True)

                        except NameError as e:
                            try:
                                twarc_connection, idx = twarc_connector.get_twarc_connection_new(idx, e)
                                followers = fetch_user_follower_ids(screen_name_user_id,
                                                                    profile_info['followers_count'],
                                                                    twarc_connection, use_twarc=True)
                            except:
                                twarc_connection, idx = twarc_connector.get_twarc_connection_new(idx, e)
                                followers = fetch_user_follower_ids(screen_name_user_id,
                                                                    profile_info['followers_count'],
                                                                    twarc_connection, use_twarc=True)




                    logging.info("Followers count for {} : {}".format(screen_name_user_id, len(followers)))
                    user_followers_info = {Constants.SCREEN_NAME: screen_name_user_id[0], Constants.FOLLOWERS: followers}
                    user_profile_collection.update({Constants.SCREEN_NAME: user_followers_info[Constants.SCREEN_NAME]},
                                                   {'$set': user_followers_info}, upsert=True)

                if screen_name_user_id not in existing_user_screen_name_with_followees:
                    followees = fetch_user_friends_ids(screen_name_user_id, profile_info['friends_count'], None, use_twarc=False)
                    if len(followees) == 0 and profile_info['friends_count'] > 0:
                        try:
                            followees = fetch_user_friends_ids(screen_name_user_id, profile_info['friends_count'],
                                                                twarc_connection, use_twarc=True)

                        except NameError as e:
                            try:
                                twarc_connection, idx = twarc_connector.get_twarc_connection_new(idx, e)
                                followees = fetch_user_friends_ids(screen_name_user_id,
                                                                   profile_info['friends_count'],
                                                                   twarc_connection, use_twarc=True)
                            except:
                                twarc_connection, idx = twarc_connector.get_twarc_connection_new(idx, e)
                                followees = fetch_user_friends_ids(screen_name_user_id,
                                                                    profile_info['friends_count'],
                                                                    twarc_connection, use_twarc=True)
                    user_followees_info = {Constants.SCREEN_NAME: screen_name_user_id, Constants.FOLLOWEES: followees}
                    logging.info("Friends countfor {} : {}".format(screen_name_user_id, len(followees)))

                    user_profile_collection.update({Constants.SCREEN_NAME: user_followees_info[Constants.SCREEN_NAME]},
                                                   {'$set': user_followees_info}, upsert=True)
        except Exception as e:
            logging.error(e)
            print("ERROR in fetch_user_followers_friends ")
            traceback.print_stack(e)


def fetch_save_user_profile(db, screen_name_user_id_list, twarc_connector):
    twarc_connector.change_params(window_limit=900, time_window=900)
    num_process = int(config["Selenium"]["num_process"])
    print("Length Before {}".format(len(screen_name_user_id_list)))
    screen_name_user_id_list = list(set(screen_name_user_id_list))
    print("Length After {}".format(len(screen_name_user_id_list)))
    user_name_chunks = chunkify(list(screen_name_user_id_list), num_process)

    logging.info("Total no. of users to fetch profile info : {}".format(len(screen_name_user_id_list)))

    multiprocess_function(num_process, function_ref=fetch_save_user_profile_job,
                          args=(user_name_chunks, twarc_connector, db))


def filter_user_ids_for_profile_info_fetch(db, screen_names):

    user_profile_collection = db[Constants.USER_PROFILE_RELATION]
    stored_names = [i['screen_name'] for i in user_profile_collection.find() if Constants.PROFILE_INFO in i.keys()]

    fetched_screen_name = set(stored_names)

    return screen_names.difference(fetched_screen_name)

def filter_user(user_profile_collection, user_list):
    stored_list = []
    for i in user_list:
        th = user_profile_collection.find_one({"screen_name":i})
        if th is None or Constants.PROFILE_INFO not in th.keys():
            stored_list.append(i)
    return stored_list


def fetch_save_user_profile_job(index, users_chunks, twarc_connector, db):
    global config

    try:
        logging.info("Started fetch_save_user_profile_job() : {}".format(index))
        user_profile_collection = db[Constants.USER_PROFILE_RELATION]
        tweet_collection = db[Constants.TWEET_COLLECTION]
        # if fake:
        #     user_profile_collection = db.fake_twitter_user_profile
        # else:
        #     user_profile_collection = db.real_twitter_user_profile

        users = users_chunks[index]


        logging.info("No. of users for process : {}".format(len(users)))
        user_need_meta = [i[0] for i in users if i[2] == "NONE"]

        use_twarc = True
        if use_twarc:
            user_list = []
            n = 100
            for i in range(0, len(user_need_meta), n):
                if i + 100 >= len(user_need_meta):
                    n = len(users) - i - 1
                user_list.append(user_need_meta[i: i + n])
        else:
            user_list = [[i] for i in user_need_meta]

        for idx, screen_name_list in enumerate(user_list):

            try:
                connection, idx = twarc_connector.get_twarc_connection_new()
                try:
                    profile_info_list = list(fetch_user_meta_info(filter_user(user_profile_collection, screen_name_list), connection, use_twarc=use_twarc))
                except NameError as e:
                    connection, _ = twarc_connector.get_twarc_connection_new(idx, e)
                    profile_info_list = list(fetch_user_meta_info(filter_user(user_profile_collection, screen_name_list), connection,
                                                             use_twarc=use_twarc))

                for i in profile_info_list:
                    user_info = {Constants.SCREEN_NAME: i['screen_name'],
                                 Constants.PROFILE_INFO: i}
                    user_profile_collection.find_and_modify({"screen_name": user_info[Constants.SCREEN_NAME]}, user_info, upsert=True)
                logging.info("Getting user info {}".format(screen_name_list))


            except:
                logging.exception("Exception in fetching user profile of user_id : {}".format(screen_name_list))

        for i in users:
            screen_name = i[0]
            # connection = twarc_connector.get_twarc_connection()
            try:
                user_tweets = fetch_user_tweets(screen_name, twarc_connector)
                user_tweet_ids = [i['id'] for i in user_tweets]
                tweet_bulk_write = [
                    UpdateOne({"id": tweet['id']}, {"$set": tweet}, upsert=True)
                    for tweet in user_tweets
                ]

                tweet_collection.bulk_write(tweet_bulk_write)
                user_profile_collection.find_one_and_update({"screen_name": screen_name}, {
                    "$set": {"recent_post": user_tweet_ids, "screen_name": screen_name}}, upsert=True)
            except Exception as e:
                logging.error("Fetch {} timeline problem {}".format(screen_name, str(e)))


    except:
        logging.exception("Exception in fetch_save_user_profile_job {}".format(index))


def fetch_save_users_info(db, twarc_connector):
    screen_name_user_id_list = get_all_users_by_news_source(db, Constants)
    logging.info("Total number of users : {}".format(len(screen_name_user_id_list)))
    print("Total number of users : {}".format(len(screen_name_user_id_list)))
    fetch_save_user_profile(db, screen_name_user_id_list, twarc_connector)

    fetch_user_followers_friends(db, screen_name_user_id_list, twarc_connector)



def main(args):
    global config
    """
        1) Get all the fake news articles from politifact
        2) Parse the articles obtained from step 1 and save it in table
        3) Fetch source of fake news and save it in separate table - if the source is not found then crawl using google and save it
        4) Use TwitterAdvCrawler to find out the tweets matching the title and save in Fake Tweets collection
        5) For each fake news tweet - get the replies, likes, retweet  using the Scrapper and update in db
    """


    try:
        config = load_configuration(args.config_file)
        init_logging(config)
        init_path(config)
        db = get_database_connection(config)

        MultiManager.register('TwarcConnector', TwarcConnector,
                              exposed=['get_twarc_connection', 'change_params',"get_twarc_connection_new"])

        manager = MultiManager()
        manager.start()
        twarc_connector = manager.TwarcConnector(config["Twitter"]["keys_file"])


        if not args.fetch_user_info:

            # directly get the url
            get_news(db)

            # # use source URL to query the tweet
            fetch_save_news_tweets(db, twarc_connector)

            # fetch URLs from the tweet content
            collect_tweets_from_urls(db, config)


            # get the information recursive replies, retweets
            fetch_save_news_tweets_details(db, config, twarc_connector)

        else:
            fetch_save_users_info(db, twarc_connector)


        manager.shutdown()


    except Exception as e:
        logging.info("--------------ERROR IN PROGRAM - TERMINATE--------------")
        logging.error(e)
        print(str(e))
        logging.exception("message")

    finally:
        pass


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--config-file", dest="config_file", help="Configuration file of the crawler", default="./project.config")
    parser.add_argument('--fetch-user-info', dest="fetch_user_info", action='store_true', default=False,
                        help="If specified fetches only user related info and graph")

    args = parser.parse_args()
    main(args)
