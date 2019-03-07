import sqlite3
import models
import os
import csv


class Database:

    def __init__(self, database_name, file_output=False):
        """
        Initialization function
        :param database_name: the name of the database to read or create, usually in the form "data/filename.db"
        """
        self.db = sqlite3.connect(database_name, timeout=30000)
        self.db_readonly = sqlite3.connect("file:" + database_name + "?mode=ro", timeout=30000, uri=True)

        # Params for File Output
        self.file_output = file_output
        self.output_folder = 'data/files/'
        self.tweet_folder = self.output_folder + 'tweet/'

    def get_cursor(self):
        return self.db.cursor()

    def create_tables(self):

        cursor = self.db.cursor()

        # Source Tweets (from original JSON file)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "source_tweets" (
                `tweet_id` CHAR(18) PRIMARY KEY,
                `tweet_text` TEXT,
                `tweet_created_at` TEXT,
                `tweet_in_reply_to_status_id` CHAR(18),
                `tweet_in_reply_to_user_id` INTEGER,
                `user_id` INTEGER,
                `user_created_at` TEXT,
                `user_description` TEXT,
                `user_screen_name` TEXT,
                `user_name` TEXT,
                `user_location` TEXT,
                `user_friends_count` INTEGER,
                `user_followers_count` INTEGER,
                `user_lang` TEXT,
                `source` TEXT,
                `added` TEXT DEFAULT CURRENT_TIMESTAMP );
        ''')

        # User table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "user" (
                `id` INTEGER PRIMARY KEY,
                `created_at` TEXT,
                `screen_name` TEXT,
                `followers_count` INTEGER,
                `friends_count` INTEGER,
                `name` TEXT,
                `description` TEXT,
                `statuses_count` INTEGER,
                `location` TEXT,
                `lang` TEXT );
        ''')

        # Tweets collected from User's timelines
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "tweet" (
                `status_id` CHAR(18) PRIMARY KEY,
                `text` TEXT,
                `created_at` TEXT,
                `in_reply_to_status_id` CHAR(18),
                `user_id` INTEGER,
                `added` TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(`user_id`) REFERENCES user(`id`) );
        ''')

        # Relationship database
        # HOW TO READ:
        # someone you follow is a friend, i.e. "twitter_id1 follows twitter_id2"
        # someone who follows you is a follower, i.e. "twitter_id1 is followed_by twitter_id2"
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "relationship" (
                `twitter_id1` INTEGER,
                `twitter_id2` INTEGER,
                `rel_type` TEXT,
                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                FOREIGN KEY(`twitter_id1`) REFERENCES user(`id`),
                FOREIGN KEY(`twitter_id2`) REFERENCES user(`id`) );
        ''')

        # queue database
        # the queue is a list of users.
        # NOTES: a primary user is a user who was found in the original source json file
        # accordingly, the 'is_primary' field value should be 0 or 1, where 0 means "false" and 1 means "true"
        # the 'status' field value should be either 'pending' or 'complete', depending on whether the user
        # has been processed.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "queue" (
                `user_id` INTEGER PRIMARY KEY,
                `is_primary` INTEGER,
                `status` TEXT,
                `status_date` TEXT DEFAULT CURRENT_TIMESTAMP,
                `added` TEXT DEFAULT CURRENT_TIMESTAMP );
        ''')

        self.db.commit()

    def alter_queue_092417(self):
        """
        This function should be used AFTER the create_tables function is called. Its
        purpose is to modify the queue table and its existing records to include
        one entry per task for each user (so the queue becomes task-based instead
        of user-based).
        :return: void
        """
        cursor = self.db.cursor()

        # -------------------------------------------------------------------
        # Step 1: Recreate queue table, adding a "task" column and changing
        #   the primary key from user_id to (user_id, task). Then, copy
        #   existing records over with task set to "no_task"
        # -------------------------------------------------------------------
        cursor.execute('''
            PRAGMA foreign_keys=off;
        ''')
        cursor.execute('''
            ALTER TABLE queue
            ADD `task` TEXT NOT NULL
            CONSTRAINT TaskDefault DEFAULT "no_task";
        ''')
        cursor.execute('''
            ALTER TABLE queue RENAME TO old_queue;
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS queue (
                `user_id` INTEGER,
                `is_primary` INTEGER,
                `task` TEXT DEFAULT "no_task",
                `status` TEXT,
                `status_date` TEXT DEFAULT CURRENT_TIMESTAMP,
                `added` TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, task));
        ''')
        cursor.execute('''
            INSERT INTO queue
            SELECT
                user_id, is_primary, task, status, status_date, added
            FROM
                old_queue;
        ''')
        cursor.execute('''
            DROP TABLE old_queue;
        ''')
        cursor.execute('''
            PRAGMA foreign_keys=on;
        ''')
        self.db.commit()

        # -------------------------------------------------------------------
        # Step 2: Pull all of the "no_task" records from the table (i.e., all
        #   existing users in the queue) and make new records for each task
        #   that should be done for that user.
        #
        # For primary users, insert 4 tasks: profile, timeline, followers, friends
        # For non-primary users, insert 2 tasks: profile, timeline
        # -------------------------------------------------------------------
        cursor.execute('''
            SELECT * FROM queue WHERE task='no_task';
        ''')
        rows = cursor.fetchall()
        for row in rows:
            """
            REFERENCE:
            row[0] = user_id
            row[1] = is_primary
            row[2] = task
            row[3] = status
            row[4] = status_date
            row[5] = added
            """
            if row[1] == 1:
                # user is primary
                complete_value = 'complete' if row[3] == 'complete' else 'incomplete'
                cursor.execute('''
                    INSERT INTO queue(user_id, is_primary, task, status)
                    VALUES(?, ?, ?, ?);
                ''', (row[0], 1, 'profile', 'complete'))
                cursor.execute('''
                    INSERT INTO queue(user_id, is_primary, task, status)
                    VALUES(?, ?, ?, ?);
                ''', (row[0], 1, 'timeline', complete_value))
                cursor.execute('''
                    INSERT INTO queue(user_id, is_primary, task, status)
                    VALUES(?, ?, ?, ?);
                ''', (row[0], 1, 'followers', complete_value))
                cursor.execute('''
                    INSERT INTO queue(user_id, is_primary, task, status)
                    VALUES(?, ?, ?, ?);
                ''', (row[0], 1, 'friends', complete_value))
                self.db.commit()
            elif row[1] == 0:
                # user is not primary
                complete_value = 'complete' if row[3] == 'complete' else 'incomplete'
                cursor.execute('''
                    INSERT INTO queue(user_id, is_primary, task, status)
                    VALUES(?, ?, ?, ?);
                ''', (row[0], 0, 'profile', complete_value))
                cursor.execute('''
                    INSERT INTO queue(user_id, is_primary, task, status)
                    VALUES(?, ?, ?, ?);
                ''', (row[0], 0, 'timeline', complete_value))
                self.db.commit()

        # -------------------------------------------------------------------
        # Step 3: Delete all of the rows from the original database
        # -------------------------------------------------------------------
        cursor.execute('''
            DELETE FROM queue WHERE task = 'no_task';
        ''')
        self.db.commit()

        # -------------------------------------------------------------------
        # Step 4: Drop the is_primary column from the table, and reinsert
        #   the records
        # -------------------------------------------------------------------
        cursor.execute('''
            ALTER TABLE queue RENAME TO old_queue;
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS queue (
                `user_id` INTEGER,
                `task` TEXT DEFAULT "no_task",
                `status` TEXT,
                `status_date` TEXT DEFAULT CURRENT_TIMESTAMP,
                `added` TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, task));
        ''')
        cursor.execute('''
            INSERT INTO queue
            SELECT 
                user_id, task, status, status_date, added
            FROM
                old_queue;
        ''')
        cursor.execute('''
            DROP TABLE old_queue;
        ''')
        self.db.commit()

    def close(self):
        self.db.close()

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#                       SOURCE_TWEET TABLE FUNCTIONS
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def save_source_tweet(self, tweet):
        cursor = self.db.cursor()

        tweet_id = tweet['id_str']
        tweet_text = tweet['text']
        tweet_created_at = tweet['created_at']  # TODO: Strip away the +0000
        tweet_in_reply_to_status_id = tweet['in_reply_to_status_id_str']
        tweet_in_reply_to_user_id = tweet['in_reply_to_user_id']

        user_id = tweet['user']['id']
        user_created_at = tweet['user']['created_at']
        user_description = tweet['user']['description']
        user_screen_name = tweet['user']['screen_name']
        user_name = tweet['user']['name']
        user_location = tweet['user']['location']
        user_friends_count = tweet['user']['friends_count']
        user_followers_count = tweet['user']['followers_count']
        user_lang = tweet['user']['lang']

        source = "jennings_json"

        try:
            cursor.execute('''
                INSERT INTO source_tweets(
                    tweet_id, tweet_text, tweet_created_at,
                    tweet_in_reply_to_status_id, tweet_in_reply_to_user_id,
                    user_id, user_created_at, user_description,
                    user_screen_name, user_name, user_location,
                    user_friends_count, user_followers_count, user_lang, source)
                VALUES( ?,?,?,?,?,
                        ?,?,?,?,?,
                        ?,?,?,?,?)''',
                (tweet_id, tweet_text, tweet_created_at,
                tweet_in_reply_to_status_id, tweet_in_reply_to_user_id,
                user_id, user_created_at, user_description,
                user_screen_name, user_name, user_location,
                user_friends_count, user_followers_count, user_lang, source))
            self.db.commit()

        except UnicodeEncodeError:
            # TODO: remove the offending character from the variable in question
            # specific case: \ud83c is an unknown character and cannot be inserted into database
            # error given is "surrogates not allowed"
            # temp solution: swallow the error, and proceed with the next record without inserting the bad one
            # db.rollback()
            print("UNICODE HELL! Skipping.\n")
            pass
        except sqlite3.IntegrityError:
            print("Duplicate record, id = " + str(tweet_id) + " already exists in source_tweets. Skipping insert")


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#                       USER TABLE FUNCTIONS
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def save_user(self, user):
        """
        save's a user to the user database
        :param user: a User object (see models.py)
        :return: void
        """
        try:
            cursor = self.db.cursor()
            cursor.execute('''
                INSERT INTO user(
                    id, created_at, screen_name, followers_count, friends_count,
                    name, description, statuses_count, location, lang
                )
                VALUES( ?,?,?,?,?,
                        ?,?,?,?,?)''',
                           (user.id, user.created_at, user.screen_name, user.followers_count, user.friends_count,
                            user.name, user.description, user.statuses_count, user.location, user.lang))

            self.db.commit()

        except sqlite3.IntegrityError:
            print("Duplicate record, id = " + str(user.id) + ".\t Skipping insert")

    def get_user_by_id(self, user_id):
        """
        Retrieve a user from the user table by their twitter ID number
        :param user_id: the twitter ID for the user you're interested in
        :return: if a match was found, the corresponding row from the database is returned. else,
            empty array returned
        """
        cursor = self.db_readonly.cursor()
        cursor.execute('''
            SELECT *
            FROM user
            WHERE id = ?
        ''', (user_id,))
        rows = cursor.fetchall()

        if len(rows) == 1:
            return rows[0]
        else:
            return []

    def get_all_user_ids(self):
        cursor = self.db_readonly.cursor()
        cursor.execute('SELECT id FROM user')
        rows = cursor.fetchall()
        return rows


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#                       TWEET TABLE FUNCTIONS
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def save_tweet(self, status):
        """
        Saves a Tweet object to the tweet database (tweet database is made up of statuses collected from
        user's timelines)
        :param status: a Tweet object (see models.py)
        :return: void
        """
        try:
            cursor = self.db.cursor()
            cursor.execute('''
                INSERT INTO tweet(
                    status_id, text, created_at, in_reply_to_status_id, user_id
                )
                VALUES(?,?,?,?,?)''',
                           (status.status_id, status.text, status.created_at,
                            status.in_reply_to_status_id,
                            status.user_id))

            self.db.commit()
            # print('+', end='', flush=True)

        except sqlite3.IntegrityError:
            return
            # print('.', end='', flush=True)
            # print("Duplicate record, id = " + str(status.id) + ".\t Skipping insert")

    def save_tweets(self, statuses):
        if len(statuses) == 0:
            return 0
        if self.file_output:
            return self.save_tweets_tsv(statuses)

        sql = '''
            INSERT INTO tweet(
                status_id, text, created_at, in_reply_to_status_id, user_id
            ) VALUES(?,?,?,?,?)'''

        data = []
        for status in statuses:
            data.append(
                [status.status_id, status.text, status.created_at,
                 status.in_reply_to_status_id,
                 status.user_id])

        try:
            cursor = self.db.cursor()
            cursor.executemany(sql,data)
            count = cursor.rowcount
            self.db.commit()
            return count
        except sqlite3.IntegrityError:
            return -1

    def save_tweets_tsv(self, statuses):

        user_id = statuses[0].user_id
        with open(self.tweet_folder + str(user_id) + '.tsv', 'w') as output:
            csv_out = csv.writer(output, delimiter='\t')

            # write header
            csv_out.writerow(['status_id', 'text', 'created_at', 'in_reply_to_status_id', 'user_id'])

            # write data
            for status in statuses:
                csv_out.writerow([status.status_id, status.text, status.created_at,
                                  status.in_reply_to_status_id,
                                  status.user_id])

        return len(statuses)

    def get_tweets_by_user_id_tsv(self,user_id):
        if os.path.exists(self.tweet_folder + str(user_id) + '.tsv'):
            return ['true']
        else:
            return []

    def get_tweets_by_user_id(self, user_id):
        """
        Retrieves all the tweets in the tweet database made by a particular user
        :param user_id: the twitter user ID of the user you want tweets from
        :return: an array of rows from the db, where each row is a single tweet by the corresponding user
            if no tweets are found by that user, None is returned
        """

        # If we're running in file mode, let's check the filesystem
        if self.file_output:
            return self.get_tweets_by_user_id_tsv(user_id)

        cursor = self.db_readonly.cursor()
        cursor.execute('''
            SELECT *
            FROM tweet
            WHERE user_id=?
        ''', (user_id,))
        rows = cursor.fetchall()
        return rows


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#                       RELATIONSHIP TABLE FUNCTIONS
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    def save_follower(self, user_id, follower):

        try:
            cursor = self.db.cursor()
            cursor.execute('''
                INSERT INTO relationship (
                    twitter_id1, twitter_id2, rel_type)
                VALUES( ?, ?, ? );''',
                           (user_id, follower, "followed_by"))

            self.db.commit()

        except sqlite3.IntegrityError:
            print("Duplicate record in relationship table. Skipping insert.")

    def save_friend(self, user_id, friend):

        try:
            cursor = self.db.cursor()
            cursor.execute('''
                INSERT INTO relationship (
                    twitter_id1, twitter_id2, rel_type)
                VALUES(?, ?, ?)''',
                           (user_id, friend, "follows"))

            self.db.commit()

        except sqlite3.IntegrityError:
            print("Duplicate record in relationship table. Skipping insert.")

    def get_followers_by_user_id(self, user_id):
        """
        Retrieves all the records in the relationship database of users who follow the specified Twitter user
        :param user_id: the twitter user ID of the user whose followers you want to retrieve
        :return: an array of rows from the database, where each row represents a follower of the specified user
                or, if no followers are found for that user, returns None
            the returned row values look like:
                rows[x][0] = twitter_id1 (matches user_id parameter)
                rows[x][1] = twitter_id2 (the follower's id)
                rows[x][2] = rel_type ("followed_by")
                rows[x][3] = id (autoincrementing, assigned by SQLite)
        """
        cursor = self.db_readonly.cursor()
        cursor.execute('''
            SELECT *
            FROM relationship
            WHERE twitter_id1 = ?
            AND rel_type = 'followed_by'
        ''', (user_id,))
        rows = cursor.fetchall()
        return rows

    def get_friends_by_user_id(self, user_id):
        """
        Retrieves all the records in the relationship database for users who the specified Twitter user follows
        (these are the user's "friends" in Twitter terminology)
        :param user_id: the Twitter user ID of the user whose friends you want to retrieve
        :return: an array of rows from the database, where each row represents a friend of the specified user (i.e.,
                someone that user is following). Or, if no friends are found for that user, returned None.
            the returned row values look like:
                rows[x][0] = twitter_id1 (matches user_id parameter)
                rows[x][1] = twitter_id2 (the friend)
                rows[x][2] = rel_type ("follows")
                rows[x][3] = id (autoincrementing, assigned by SQLite)
        """
        cursor = self.db_readonly.cursor()
        cursor.execute('''
            SELECT *
            FROM relationship
            WHERE twitter_id1 = ?
            AND rel_type = 'follows'
        ''', (user_id,))
        rows = cursor.fetchall()
        return rows


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#                       QUEUE TABLE FUNCTIONS
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# IMPORTANT: these functions depend on the latest version of the queue table after migration
#       (see alter queue table function above)

    def add_user_to_queue(self, user_id, include_rel):
        """
        Adds entries to the queue for each task that must be completed for the user. This
        function always inserts at least 2 tasks for the user: profile & timeline. If the
        parameter include_rel is true, then two additional tasks are inserted: followers & friends.
        :param user_id:
        :param include_rel: boolean value indicating whether to include getting followers and friends
            as tasks for this user in the queue.
        :return: void
        """
        assert(isinstance(include_rel, bool)), "second argument to add_user_to_queue must be a boolean"
        try:
            cursor = self.db.cursor()
            cursor.execute('''
                INSERT INTO queue (
                    user_id, task, status)
                VALUES(?,?,?);''',
                (user_id, "profile", "incomplete"))
        except sqlite3.IntegrityError:
            print("Duplicate value; task already exists in queue. Skipping insert.")

        try:
            cursor.execute('''
                INSERT INTO queue (
                    user_id, task, status)
                VALUES(?,?,?);''',
                (user_id, "timeline", "incomplete"))
        except sqlite3.IntegrityError:
            print("Duplicate value; task already exists in queue. Skipping insert.")

        if include_rel:
            try:
                cursor.execute('''
                    INSERT INTO queue (
                        user_id, task, status)
                    VALUES(?,?,?);''',
                    (user_id, "followers", "incomplete"))
            except sqlite3.IntegrityError:
                print("Duplicate value; task already exists in queue. Skipping insert.")

            try:
                cursor.execute('''
                    INSERT INTO queue (
                        user_id, task, status)
                    VALUES(?,?,?);''',
                    (user_id, "friends", "incomplete"))
            except sqlite3.IntegrityError:
                print("Duplicate value; task already exists in queue. Skipping insert.")

        self.db.commit()

    def get_task_from_queue(self):
        """
        Gets a single task from the queue where status is "incomplete". Marks that task status
        as "pending". This function does not return "profile" tasks; those are handled in bulk.
        :return: a row from the queue database where:
            row[0] = user_id
            row[1] = task ["profile, timeline", "followers", "friends"]
            row[2] = status ["complete", "pending", "incomplete"]
            row[3] = status_date
            row[4] = added
        or, if no tasks are incomplete, return value is None
        """
        # fetch the record
        cursor_ro = self.db_readonly.cursor()
        cursor_ro.execute('''
            SELECT * FROM queue 
            WHERE status = 'incomplete'
                AND task IN ("timeline", "followers", "friends")
            ORDER BY RANDOM() LIMIT 1;
        ''')
        row = cursor_ro.fetchone()

        # update status to pending
        self.update_task_status_in_queue(row[0], row[1], "pending")

        return row

    def get_profile_tasks_from_queue(self):
        """
        Finds all the "profile" tasks in the queue that are marked "incomplete" and returns
        a list containing all the user id's associated with those tasks
        :return: list of user id's for users whose profile task is "incomplete"
        """
        # fetch the records
        cursor_ro = self.db_readonly.cursor()
        cursor_ro.execute('''
            SELECT user_id FROM queue
            WHERE task = "profile"
                AND status = "incomplete"
            ORDER BY RANDOM() LIMIT 100;
        ''')
        rows = cursor_ro.fetchall()

        # update the tasks' statuses to 'pending'
        for row in rows:
            user_id = row[0]
            self.update_task_status_in_queue(user_id, "profile", "pending")

        return rows

    def update_task_status_in_queue(self, user_id, task_name, status):
        """
        Updates the user's status in the queue
        :param user_id: the twitter user id of the user associated with this task
        :param task_name: the name of the task to update (one of "profile", "timeline", "followers", or "friends")
        :param status: what to change the user's status to (one of 'incomplete', 'pending', or 'complete')
        :return: void
        """
        status = status.lower()
        task_name = task_name.lower()
        if status not in ["incomplete", "pending", "complete"]:
            return
        if task_name not in ["profile", "timeline", "followers", "friends"]:
            return
        cursor = self.db.cursor()

        cursor.execute('''
            UPDATE queue
            SET status = ?, status_date = CURRENT_TIMESTAMP
            WHERE user_id = ? AND task = ?
        ''', (status, user_id, task_name))
        self.db.commit()
