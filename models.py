import database
import tweepy


class User:
    def __init__(self):
        self.is_primary = None
        self.id = None
        self.created_at = None
        self.screen_name = None
        self.followers_count = None
        self.friends_count = None
        self.name = None
        self.description = None
        self.statuses_count = None
        self.location = None
        self.lang = None

    def load_from_source_tweet(self, json):
        self.is_primary = 1
        self.id = json['user']['id']
        self.created_at = json['user']['created_at']
        self.screen_name = json['user']['screen_name']
        self.followers_count = json['user']['followers_count']
        self.friends_count = json['user']['friends_count']
        self.name = json['user']['name']
        self.description = json['user']['description']
        self.statuses_count = json['user']['statuses_count']
        self.location = json['user']['location']
        self.lang = json['user']['lang']
        return self

    def load_from_twitter_user_object(self, user):
        """
        Loads data into this User object from a User object returned from Twitter's API
        :param user: a twitter User object returned from Twitter's API
        :return: this User instance
        """
        self.is_primary = 0
        self.id = user.id
        self.created_at = user.created_at
        self.screen_name = user.screen_name
        self.followers_count = user.followers_count
        self.friends_count = user.friends_count
        self.name = user.name
        self.description = user.description
        self.statuses_count = user.statuses_count
        self.location = user.location
        self.lang = user.lang
        return self

    def get_id(self):
        return self.id

    def get_screen_name(self):
        return self.screen_name

    def is_primary(self):
        return self.is_primary


class Tweet:
    def __init__(self):
        self.status_id = None
        self.text = None
        self.created_at = None
        self.in_reply_to_status_id = None
        self.user_id = None

    def load_from_twitter_status_object(self, status):
        """
        Method to load the values we are interested in from the Status object returned
        from Twitter's API into this Tweet object
        :param status: a Status object returned from Wrapper's get_timeline function
        :return: this Tweet object instance
        """
        self.status_id = status.id_str
        self.text = status.text
        self.created_at = str(status.created_at)
        self.in_reply_to_status_id = status.in_reply_to_status_id_str
        self.user_id = status.author.id
        return self
