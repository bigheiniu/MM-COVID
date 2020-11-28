import logging
import time

from code.util import Constants
from datetime import timedelta
import twint
from Util import time_parser

from itertools import chain
import pandas as pd
###=======================Fetch Tweets=======================####

def get_replies(conversation_id, screen_name, created_at):
    # replies, likes, retweets
    replies = twint.Config()

    logging.info("screen_name {}, Created at {}, Conversation ID {}".format(screen_name, created_at, conversation_id))
    print("screen_name {}, Created at {}, Conversation ID {}".format(screen_name, created_at, conversation_id))
    replies.Retries_count = 2
    replies.Store_object = True
    replies.Store_object_tweets_list = []
    replies.Search = "(to:{})".format(screen_name)
    replies.Limit = 1000
    replies.Hide_output = True

    max_try = 2
    try_times = 0
    time_delta = 1
    df_list = []
    while try_times < max_try:
        time.sleep(1)
        if created_at:
            search_end = created_at + timedelta(time_delta)
            search_end_str = search_end.strftime("%Y-%m-%d")
            created_at_str = created_at.strftime("%Y-%m-%d")
            replies.Until = search_end_str
            replies.Since = created_at_str

        twint.run.Search(replies)
        df = pd.DataFrame([vars(i) for i in replies.Store_object_tweets_list])
        replies.search_tweet_list = []
        df = df.rename(columns={"data-conversation-id":"conversation_id","date":"created_at","data-item-id":"id"})
        df.drop_duplicates(inplace=True, subset=['id_str'])
        if len(df) == 0:
            time_delta = 2 * time_delta
            try_times += 1
            continue
        df['username'] = df['username'].apply(lambda x:x.replace("@",""))
        df['nreplies'] = df['replies_count']
        df['nretweets'] = df['retweets_count']

        return_replies_df = []
        print(len(df))
        if len(df) > 0:
            df['id'] = df['id'].apply(lambda x: int(x))
            return_replies_df = df[df['conversation_id'].apply(lambda x:str(x)==str(conversation_id)) ]
            # return_replies_df = df

            logging.info("There are {} replies for {}, {}".format(len(return_replies_df), conversation_id, screen_name))
        df_list.append(df)
        if len(return_replies_df) < 10:
            time_delta = 2 * time_delta
            try_times += 1
        else:
            break



    if len(df_list) == 0:
        return_replies_list = []
        unrelated_replies = []
    else:
        df = pd.concat(df_list)
        df.drop_duplicates(inplace=True, subset=['id'])
        df = df.astype({"id":"int64"})
        return_replies_df = df[df['conversation_id'].apply(lambda x:str(x)==str(conversation_id))]
        return_replies_list = return_replies_df.to_dict(orient="record")
        unrelated_replies = df.to_dict(orient="record")
        print("There are {} related tweets".format(len(return_replies_list)))


    return return_replies_list, unrelated_replies


def get_recursive_replies(tweet_id, screen_name,
                          created_at, conversation_id,
                          level,
                          user_collection, tweet_collection,
                          tweet_relation_collection):
    try:
        replies, all_replies = get_replies(conversation_id, screen_name, created_at)
    except Exception as e:
        logging.error("ERROR in get replies {}, {}, {}".format(conversation_id, screen_name, created_at))
        print("ERROR in get replies {}, {}, {}".format(conversation_id, screen_name, created_at))
        logging.error(str(e))
        return []

    user_replies = list(chain.from_iterable([[{"tweet_id":i['id'], "reply_screen_name":j['screen_name']} for j in i['reply_to']] for i in all_replies if len(i['reply_to']) > 0]))

    for i in all_replies:
        screen_name_here = i['username']
        if user_collection.find_one({"screen_name": screen_name_here}) is None:
            user_collection.insert({"post_tweet": [], "screen_name": screen_name_here})
        user_collection.find_one_and_update({"screen_name": screen_name_here},
                                            {"$addToSet": {"post_tweet": i['id']}}, upsert=True)
    for i in user_replies:
        if user_collection.find_one({"screen_name":i['reply_screen_name']}) is None:
            user_collection.insert({"reply_from":[],"screen_name":i['reply_screen_name']})
        user_collection.find_one_and_update({"screen_name":i['reply_screen_name']}, {"$addToSet":{"reply_from":i['tweet_id']}})
    for i in all_replies:
        tweet_collection.find_and_modify({"id": i['id']},{"$set": i}, upsert=True)
    conversation_thread_tweets = [i for i in replies if i['nreplies'] > 0]
    print("There are {} tweets looking for replies at level {}".format(len(conversation_thread_tweets), level))
    replies_ids = [i['id'] for i in replies]

    if level < 3:
        for search_one in conversation_thread_tweets:

            search_one['id'] = int(search_one['id'])

            created_at = time_parser(created_at)

            is_get_replies = search_one['nreplies'] > 0
            if is_get_replies:
                engagement = get_recursive_replies(screen_name=search_one['username'],
                                                   created_at=created_at,
                                                   conversation_id=conversation_id,
                                                   tweet_id=search_one['id'],
                                                   level=level + 1,
                                                   user_collection=user_collection,
                                                   tweet_collection=tweet_collection,
                                                   tweet_relation_collection=tweet_relation_collection,
                                                   )

                engagement = {"replies":{"$each":engagement}}
                if len(engagement) == 0:
                    continue
                if tweet_relation_collection.find_one({"tweet_id": search_one['id']}) is None:
                    tweet_relation_collection.find_one_and_update({"tweet_id": search_one['id']}, {"$set": {"tweet_id": search_one['id'],
                                                                                                   "replies": [], }},
                                                                  upsert=True)
                tweet_relation_collection.find_one_and_update({"tweet_id": search_one['id']}, {"$addToSet": engagement})

    return replies_ids

def get_tweet_details(tweet_id, twarc_connector, created_at, screen_name, conversation_id, user_collection,
                      tweet_collection, tweet_relation_collection,
                      get_retweets=True,
                      get_replies=True, level=0):

    logging.info(
        "level {}, tweet_id {}, created_at {}, screen_name {}, conversation_id {}".format(level, tweet_id, created_at,
                                                                                          screen_name, conversation_id))
    tweet_id = int(tweet_id)
    tweet_past = tweet_relation_collection.find_one({"tweet_id": tweet_id})

    retweet_l = len(tweet_past[
                        Constants.TWEET_RETWEETS]) < 2 if tweet_past and Constants.TWEET_RETWEETS in tweet_past.keys() else True
    replies_l = len(tweet_past[
                        Constants.TWEET_REPLIES]) < 2 if tweet_past and Constants.TWEET_REPLIES in tweet_past.keys() else True

    if get_retweets and retweet_l:
        retweets = list(get_retweets_twarc(tweet_id, twarc_connector))
        for retweet in retweets:
            user_collection.find_one_and_update({"screen_name": retweet['user']['screen_name']},
                                                {"$set": {**retweet['user'],
                                                          "screen_name": retweet['user']['screen_name']}}, upsert=True)
        for retweet in retweets:
            retweet["screen_name"] = retweet['user']['screen_name']
            retweet["user_id"] = retweet['user']['id']
            del retweet['user']
            tweet_collection.find_one_and_update({"id": retweet['id']}, {"$set": retweet}, upsert=True)
        retweet_ids = [i['id'] for i in retweets]
    else:
        retweet_ids = []

    if get_replies and replies_l:
        replies = get_recursive_replies(tweet_id, screen_name, created_at, conversation_id,
                                        level, user_collection, tweet_collection,
                                        tweet_relation_collection)
    else:
        replies = []

    result = {}
    result[Constants.TWEET_LIKES] = []
    result[Constants.TWEET_RETWEETS] = list(retweet_ids)
    result[Constants.TWEET_REPLIES] = list(replies)

    logging.info(
        "Tweet ID: {} => Favourites : {} Replies: {}  Retweets :  {} ".format(tweet_id, (len([])),
                                                                              (len(replies)), (len(retweet_ids))))
    print((
        "Tweet ID: {} => Favourites : {} Replies: {}  Retweets :  {} ".format(tweet_id, (len([])),
                                                                              (len(replies)), (len(retweet_ids)))))

    return result


def get_retweets_twarc(tweet_id, twarc_connector):
    retweets = []
    twarc_connector.change_params(window_limit=75, time_window=900)
    try:
        try:
            connection, idx = twarc_connector.get_twarc_connection_new()
        except:
            connection = twarc_connector
        # if type(tweet_id) is not list:
        #     tweet_id = tweet_id
        try:
            retweets = list(connection.retweets([tweet_id]))
        except NameError as e:
            connection, _ = twarc_connector.get_twarc_connection_new(idx, e)
            retweets = list(connection.retweets([tweet_id]))

    except:
        logging.exception(
            "Exception in getting retweets for tweet id %d using connection %s" % (tweet_id, twarc_connector))
    return retweets

def twitter_adv_search_twint(kws,date=None, limit=200):

    c = twint.Config()
    c.Search = kws
    c.Limit = limit
    c.Retries_count = 1
    if date:
        c.Since = "{} 00:00:00".format(date)
    # if lang:
    c.Retweets = True
    c.Favorites = True
    c.Hide_output = True
    c.Store_object = True
    c.Store_object_tweets_list = []
    twint.run.Search(c)

    tweets = c.Store_object_tweets_list
    result_json = []
    for tweet in tweets:
        tweet = vars(tweet)
        try:
            aTweet = {}
            aTweet['id'] = int(tweet["id"])
            aTweet['user'] = int(tweet['user_id'])
            aTweet['username'] = tweet['username']
            aTweet['text'] = tweet['tweet']
            aTweet['nretweets'] = int(tweet['retweets_count'])
            aTweet['nreplies'] = int(tweet['replies_count'])
            aTweet['raw'] = tweet
            result_json.append(aTweet)
        except Exception as e:
            logging.error('Error parsing tweet (' + str(e) + ')')
    logging.info('Found %d tweets : Query : %s' % (len(result_json), kws.encode('utf-8')))
    print('Found %d tweets : Query : %s' % (len(result_json), kws.encode('utf-8')))
    return result_json

###=======================User Meta Information=======================####
def fetch_user_meta_info(screen_name, twarc_connection, use_twarc):
    # logging.info("Getting profile info of Twitter user : {}".format(screen_name))

    if use_twarc:
        try:
            profile_info = list(twarc_connection.user_lookup(screen_name, id_type='screen_name'))
            return profile_info
        except KeyError as k:
            logging.error(k)
    else:
        if type(screen_name) is list:
            screen_name = screen_name[0]
        user_meta_info = []
        c = twint.Config()
        c.Username = screen_name
        c.Pandas = True
        c.Pandas_clean = True
        c.Retries_count = 1
        all_try = 1
        repeat_try = 0
        while repeat_try < all_try:
            try:
                twint.run.Lookup(c)
                user_meta_info = [twint.storage.panda.User_df.to_dict(orient='record')[0]]
            except:
                logging.exception("Exception in follower_ids for user : {} for {} times".format(screen_name, repeat_try))

            if len(user_meta_info) == 0:
                repeat_try += 1
            else:
                return user_meta_info

    return None


def fetch_user_follower_ids(screen_name_user_id, followers_count, twarc_connection, use_twarc=False):
    screen_name, user_id = screen_name_user_id
    if use_twarc:
        followers_ids = []
        try:
            followers_ids = twarc_connection.follower_ids(screen_name)
            followers_ids = followers_ids["ids"]
        except:
            logging.exception("Exception in follower_ids for user : {} when using twarc".format(screen_name))
        return followers_ids
    else:
        followers_screen_name = []
        c = twint.Config()
        c.Username = screen_name
        c.User_id = user_id
        c.Limit = followers_count
        c.Pandas = True
        c.Pandas_clean = True
        all_try = 2
        repeat_try = 0
        while repeat_try < all_try:
            try:
                twint.run.Followers(c)
                followers_screen_name = twint.storage.panda.Follow_df.to_dict(orient='record')[0]['followers']
            except:
                logging.exception("Exception in follower_ids for user : {} for {} times".format(screen_name, repeat_try))

            if len(followers_screen_name) == 0:
                repeat_try += 1
            else:
                return followers_screen_name

        return followers_screen_name

def fetch_user_tweets(screen_name, twarc_connector, use_twarc=False, kws=None, limit=None):
    user_tweets = []
    repeat_try = 0
    try:
        if use_twarc:
            connection, idx = twarc_connector.get_twarc_connection_new()
            try:
                user_tweets = list(connection.timeline(screen_name=screen_name))
            except NameError as e:
                try:
                    connection, idx = twarc_connector.get_twarc_connection_new(idx, e)
                    user_tweets = list(connection.timeline(screen_name=screen_name))
                except NameError as e:
                    connection, idx = twarc_connector.get_twarc_connection_new(idx, e)
                    user_tweets = list(connection.timeline(screen_name=screen_name))
            return user_tweets
        else:
            c = twint.Config()
            c.Search = "(from:{})".format(screen_name)
            if kws:
                c.Search = kws + " " + c.Search
            c.Pandas = True
            c.Pandas_clean = True
            c.Retries_count = 2
            all_try = 2
            c.Limit = limit if limit else 300
            while repeat_try < all_try:
                try:
                    twint.run.Search(c)
                    user_tweets = twint.storage.panda.Tweets_df.to_dict(orient='record')

                except:
                    logging.exception(
                        "Exception in follower_ids for user : {} for {} times".format(screen_name, repeat_try))

                if len(user_tweets) == 0:
                    repeat_try += 1
                else:
                    return user_tweets
    except:
        logging.exception("Exception in follower_ids for user : {} for {} times".format(screen_name, repeat_try))

    return user_tweets

def fetch_user_friends_ids(screen_name_user_id, following_count, twarc_connection, use_twarc):
    user_friends = []
    screen_name, user_id = screen_name_user_id
    if use_twarc:
        try:
            user_friends = twarc_connection.friend_ids(screen_name, 1)
            user_friends = user_friends["ids"]
        except:
            logging.exception("Exception in follower_ids for user : {}".format(screen_name))
    else:
        c = twint.Config()
        c.Username = screen_name
        c.User_id = user_id
        c.Limit = following_count
        c.Store_object=True
        c.Hide_output=True
        twint.run.Following(c)
        user_friends = twint.output.follows_list



    return user_friends

def tweet_dyhrate(tweet_id_list, twarc_connector):
    hydrate_list = []
    for idx in range(0, len(tweet_id_list), 100):
        connection, idx = twarc_connector.get_twarc_connection_new()
        tweet_ids = tweet_id_list[idx:idx + 100]
        try:
            tweets = list(connection.hydrate(tweet_ids))
        except NameError as e:
            connection, _ = twarc_connector.get_twarc_connection_new(idx, e)
            tweets = list(connection.hydrate(tweet_ids))

        hydrate_list.extend(tweets)
    hydrate_list = [{**i, "screen_name": i['user']['screen_name']} for i in hydrate_list]
    users = [i['user'] for i in hydrate_list]

    return hydrate_list, users

