'''
Contains the entire pipeline for running the twitter scraper.

Here are the high level steps:
1. Create the database, and all tables.

Tables include:
- source_tweets (from json file)
- user
- tweet (populated from user's timelines)
- relationship (between 2 users)
- queue (of users)

2. Ingest the source dataset of tweets. This requires reading a JSON file (one record per line) and saving
 them to the source_tweets table.

3. Get users from source_tweets table, and add them to queue (check for duplicates)

4. Get user A from queue (where status = "pending")

5. IF NOT PRIMARY: get user A's profile (see User class in models.py for details) and save user A to database

6. Get user A's timeline of tweets and save each to the tweet database

7. IF PRIMARY: get user A's followers and friends. save each follower and friend relationship to the "relationship"
database, then save each follower and friend user id to the queue (with primary = 0)

8. update user A's record in the queue: status field should be changed to "complete"

9. go back to step 4 until all users in the queue are "complete"

'''

import wrapper
import database
import models
import json
import pprint
import tweepy
import sys
import progressbar
import time
import api_keys as api

## Parse Args:
try:
    app_number = int(sys.argv[1])
except:
    app_number = 1
try:
    file_ind = True if sys.argv[2] == "file" else False
except:
    file_ind = False

## Database Connection ##
# db = database.Database('data/tweets_ethans_test.db')
db = database.Database('data/tweets.db', file_output=True)
db.create_tables()

## Load Wrapper ##
w = wrapper.Wrapper(api.keys[app_number])

# ## 2. Ingest Source JSON file ##
# # filename = "data/tweets_short_test.json"
# # filename = "data/split/jul5_aa"  # segment of the large megafile
# filename = "data/filtered.json"
# with open(filename) as f:
#     content = f.readlines()
# content = [x for i, x in enumerate(content) if i in range(1000)]
# content = [json.loads(x) for x in content if type(x) != None]
#
#
# ## Process the data from source JSON ##
# print(len(content))
# # users = []
# for c in content:
#     db.save_source_tweet(c)
#     u = models.User().load_from_source_tweet(c)
#     if u is not None:
#         db.save_user(u)
#         db.add_user_to_queue(u)
#         # users.append(u)
#
# import sys
# sys.exit()


# ETHAN: widgets = ['Processed: ', progressbar.Counter('%d'),
#           ' lines (', progressbar.Timer(), ' ', progressbar.AdaptiveETA(), ')']
# ETHAN: bar = progressbar.ProgressBar(maxval=len(users), widgets=widgets).start()

widgets = [progressbar.FormatLabel('Processed: %(value)d lines (in: %(elapsed)s)')]
bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength, widgets=widgets).start()
count = 0
starttime = time.time()

queued_user = db.get_user_from_queue()
if queued_user is not None:
    done = False
else:
    done = True

while not done:
    bar.update(count)
    print("\nRate: " + str(round(count/((time.time() - starttime)/60),2)) + " / min\t")
    count += 1

    user_id = queued_user[0]
    print('\nUser: ' + str(user_id), end='\t')

    try:
        if not queued_user[1]:  # if user is not primary
            if len(db.get_user_by_id(user_id)) == 0:
                twitter_user = models.User().load_from_twitter_user_object(w.get_user_by_id(user_id))
                db.save_user(twitter_user)

        # Get user's timeline
        print('Timeline', end=' ', flush=True)
        if len(db.get_tweets_by_user_id(user_id)) == 0:
            statuses = w.get_timeline(user_id)
            # for status in statuses:
            #     s = models.Tweet().load_from_twitter_status_object(status)
            #     db.save_tweet(s)
            statuses = [models.Tweet().load_from_twitter_status_object(status) for status in statuses]
            rows_count = db.save_tweets(statuses)
            print('(' + str(rows_count) + ')', end='\t', flush=True)

        if queued_user[1]:  # if user is primary
            if len(db.get_followers_by_user_id(user_id)) == 0:
                # get followers and add each to queue
                print('Followers', end=' ', flush=True)
                followers = w.get_followers(user_id)
                for follower in followers:
                    db.save_follower(user_id, follower)
                    db.add_user_to_queue_by_id(follower, 0)
                print('(' + str(len(followers)) + ')', end='\t', flush=True)

            if len(db.get_friends_by_user_id(user_id)) == 0:
                # get friends and add each to queue
                print('Friends', end=' ', flush=True)
                friends = w.get_friends(user_id)
                for friend in friends:
                    db.save_friend(user_id, friend)
                    db.add_user_to_queue_by_id(friend, 0)
                print('(' + str(len(friends)) + ')', end='\n', flush=True)

        # Mark them as done
        db.update_user_status_in_queue(user_id, "complete")

    except tweepy.error.TweepError as e:
        pprint.pprint(e)
    finally:
        queued_user = db.get_user_from_queue()
        if queued_user is None:
            done = True

db.close()