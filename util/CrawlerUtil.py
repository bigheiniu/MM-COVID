
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

    if 'tweet_replies' in reply:
        for reply_of_reply in reply['tweet_replies']:
            user_set.update(get_users_in_reply(reply_of_reply))

    return user_set


def get_all_users_by_news_source(db, Constants):
    """Get the user ids involved with the tweets including social engagements"""
    users_screen_name = []
    users_user_id = []
    tweets_collection = db[Constants.TWEET_COLLECTION]
    user_profile = db[Constants.USER_PROFILE_RELATION]
    for i in tweets_collection.find({},{"username":1, "screen_name":1,"_id":0,"user_id":1}):
        if "username" in i.keys():
            username = i['username']
        elif "screen_name" in i.keys():
            username = i['screen_name']
        else:
            continue
        users_screen_name.append(username)
        users_user_id.append(i.get("user_id",None))
    screen_name_user_id = [(i, j) for i,j in zip(users_screen_name, users_user_id)]
    screen_name_user_id = list(set(screen_name_user_id))
    return_screen_name_user_id = []
    for idx, (screen_name, user_id) in enumerate(screen_name_user_id):
        th = user_profile.find_one({"screen_name":screen_name})
        if th is None:
            flag = "NONE"
        else:
            flag = "NoMeta"
        return_screen_name_user_id.append((screen_name, user_id, flag))


    return return_screen_name_user_id

    # if fake:
    #     tweet_collection = db["fake_tweet_collection_{}".format(type)]
    #     tweet_info_collection = db["fake_tweet_info_coll"]
    # else:
    #     tweet_collection = db["real_tweet_collection_{}".format(type)]
    #     tweet_info_collection = db["fake_tweet_info_coll"]
    #
    # if len(news_source) > 0:
    #     tweeted_users = tweet_collection.distinct("user",{"news_source": news_source})
    #     tweeted_user_screen_name = tweet_collection.distinct("username", {"news_source": news_source})
    #     tweet_ids = tweet_collection.distinct("id", {"news_source": news_source})
    # else:
    #     tweeted_users = tweet_collection.distinct("user")
    #     tweeted_user_screen_name = tweet_collection.distinct("username")
    #     tweeted_users = tweet_collection.distinct("user")
    #     tweet_ids = tweet_collection.distinct("id")
    #
    # users_id = set(tweeted_users)
    # users_screen_name = set(tweeted_user_screen_name)
    #
    # for tweet_info in tweet_info_collection.find({"id": {"$in" : tweet_ids}}):
    #         users_id.update(set(tweet_info["tweet_likes"]))
    #         # Get replies in all levels of reply
    #         users_id.update(get_users_involved_in_replies(tweet_info["tweet_replies"]))
    #         users_id.update(set([comment["user"]["id"] for comment in tweet_info["tweet_retweets"]]))
    #         users_screen_name.update(set([comment["user"]["id"] for comment in tweet_info["tweet_retweets"]]))
    # return list(users_screen_name), list(users_id)
