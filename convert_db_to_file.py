import database
import csv
import os
import progressbar
import time

db = database.Database('data/bigdata_tweets.db')

# Create folders as needed
output_folder = 'data/files/'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
if not os.path.exists(output_folder + 'tweet/'):
    os.makedirs(output_folder + 'tweet/')
# if not os.path.exists(output_folder):
#     os.makedirs(output_folder)
# if not os.path.exists(output_folder):
#     os.makedirs(output_folder)


# Select all users from tweet tables
cursor = db.get_cursor()
cursor.execute('''SELECT DISTINCT user_id FROM tweet''')
users = cursor.fetchall()

# For each user, select all tweets for that user, and then output them
users = [user[0] for user in users]

print("Users to create: "+str(len(users)))

# widgets = ['Processed: ', progressbar.Counter('%d'),
        #   ' lines (', progressbar.Timer(), ' ', progressbar.AdaptiveETA(), ')']
# widgets = [FileTransferSpeed()]

widgets = [progressbar.FormatLabel('Processed: %(value)d lines (in: %(elapsed)s)')]
bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength, widgets=widgets).start()

count = 0
starttime = time.time()
for user in users:
    bar.update(count)
    print ("\nRate: " + str(round(count/((time.time() - starttime)/60),2)) + " / min")
    count += 1


    if os.path.exists(output_folder + 'tweet/' + str(user)+'.tsv'):
        continue

    cursor = db.get_cursor()
    cursor.execute(
        '''SELECT * FROM tweet WHERE user_id = ? ''',
        (user,)
    )
    with open(output_folder + 'tweet/' + str(user)+'.tsv', 'w') as output:
        csv_out = csv.writer(output, delimiter ='\t')

        # write header
        csv_out.writerow([d[0] for d in cursor.description])

        # write data
        for result in cursor:
            csv_out.writerow(result)

bar.finish()