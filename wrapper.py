import tweepy
import pprint


class Wrapper:

    def __init__(self, creds):
        self.api = None
        self.cursor = None

        auth = tweepy.OAuthHandler(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])
        auth.set_access_token(creds['ACCESS_TOKEN'], creds['ACCESS_TOKEN_SECRET'])

        self.api = api = tweepy.API(auth, retry_count=3, retry_delay=10, wait_on_rate_limit=True,
                                    wait_on_rate_limit_notify=True)

    def get_user(self, username):
        user = self.api.get_user(username)
        return user

    def get_user_by_id(self, user_id):
        user = self.api.get_user(user_id=user_id)
        return user

    def lookup_users(self, user_id_list):
        users = self.api.lookup_users(user_ids=user_id_list)
        return users

    def get_followers(self, username):
        results = []
        for r in tweepy.Cursor(self.api.followers_ids, id=username).items():
            results.append(r)
        return results

    def get_friends(self, username):
        results = []
        for r in tweepy.Cursor(self.api.friends_ids, id=username).items():
            results.append(r)
        return results

    def get_timeline(self, username):
        # try:
        statuses = []
        for status in tweepy.Cursor(self.api.user_timeline, id=username).items():
            if status:
                statuses.append(status)
            # print('.', end='', flush=True)
        return statuses
        # except tweepy.error.TweepError as e:
        #     pprint.pprint(e)
        #     return []
            # if e.message[0]['code'] == 401:
            #     print("Private account. Moving on...")
            #     return []
            # else:
            #     print(type(e))
            #     print(e)
        # finally:
        #     continue
