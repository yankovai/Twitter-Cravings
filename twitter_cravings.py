import json
import pickle
import string
import collections
import csv

def tweet_hour(utc_time, longitude):
	"""
	Returns the hour of day at which a tweet was made relative to the tweet's
	location.
	"""

	utc_time = utc_time.split()[3].split(':')
	utc_time = map(float, utc_time)
	utc_time = utc_time[0] + utc_time[1]/60. + utc_time[2]/3600. 
	hour = round(utc_time + longitude/15.)

	if hour < 0:
		return 24.0 + hour
	elif hour >= 24.:
		return hour - 24.
	else:
		return hour

def generate_sql_db(db_name = 'output.db', output_file = 'output.csv'):
	"""
	Stores the data in output_file into a sqlite3 database.
	"""

	import sqlite3 as lite

	with lite.connect(db_name) as con:
		cur = con.cursor()
		cur.execute('CREATE TABLE Cravings(Hour INT, Longitude REAL, Latitude REAL, Craving TEXT)')

		with open(output_file,'rb') as output_data:
			cravings = csv.reader(output_data)
			cravings.next()

			cur.executemany("INSERT INTO Cravings VALUES(?, ?, ?, ?)", cravings)

class Twitter_Cravings(object):
	"""
	Determines what Twitter users crave the most.
	"""

	def __init__(self, tweets_filename):
		"""
		Accepts a text file filled with tweets containing the word 'craving'.
		The tweets file originates from the Twitter live stream API.
		"""

		self.tweets_filename = tweets_filename

		# Load stop words
		with open('stopwords.pickle','rb') as pickle_file:
			self.stop_words = pickle.load(pickle_file)
			self.stop_words.add('amp')

	def __preprocess_filter(self, tweet_text):
		"""
		Removes puncutation marks and non ascii characters from a tweet text.
		From the original tweet, returns all the words after 'craving'.
		"""

		tweet_text = filter(lambda x: x in string.printable, tweet_text)
		tweet_text = filter(lambda x: not(x in string.punctuation), tweet_text)
		tweet_text = tweet_text.lower().split()
		index = tweet_text.index('craving') + 1
		tweet_text = tweet_text[index::]

		return tweet_text

	def __postprocess_filter(self, craving_candidates):
		"""
		For each candidate craving in 'craving_candidates' attempts to remove
		poorly formatted cravings.
		"""

		cravings = []
		for candidate in craving_candidates:
			if len(candidate) < 12:
				if not(candidate in self.stop_words):
					cravings.append(candidate)

		if len(cravings) == 1:
			return cravings[0]
		elif len(cravings) > 1:
			return '-'.join(cravings[0:2])
		else:
			return None

	def __extract_craving(self, tweet_text):
		"""
		Extracts the craving from 'tweet_text'.
		"""

		tweet_text = self.__preprocess_filter(tweet_text)
		
		craving_candidates = tweet_text[0:2]
		for word in tweet_text[2::]:
			if not(word in self.stop_words):
				craving_candidates.append(word)
			else:
				break

		cravings = self.__postprocess_filter(craving_candidates)

		return cravings

	def __call__(self):
		"""
		Go through a tweet file and return a list with each tweet's craving, 
		data and location (if avaialable).
		"""

		all_cravings = []
		with open(self.tweets_filename) as content_file:
			tweets = content_file.readlines()

			for tweet in tweets:
				try:
					tweet = json.loads(tweet)
					cravings = self.__extract_craving(tweet['text'])

					if cravings:
						tweet_sum = {}
						tweet_sum.setdefault('cravings', cravings)
						tweet_sum.setdefault('date', tweet['created_at'])

						if tweet.get('coordinates'):
							tweet_sum.setdefault('coords', tweet['coordinates'])
						else:
							tweet_sum.setdefault('coords', None)

						all_cravings.append(tweet_sum)

				except ValueError:
					pass
		
		self.all_cravings = all_cravings

	def most_common_cravings(self, n=10):
		"""
		Returns the top 'n' cravings from the tweets in 'tweets_filename'.
		"""

		cravings = [tweet_sum['cravings'] for tweet_sum in self.all_cravings]
		c = collections.Counter(cravings)

		return c.most_common(n)

	def write_output(self):
		"""
		Write results in all_cravings to csv file. Only results with geo
		coordinates are output.
		"""

		with open('output.csv','wb') as output_file:
			write2me = csv.writer(output_file)
			write2me.writerow(['Hour', 'Longitude', 'Latitude', 'Craving'])

			for tweet_sum in self.all_cravings:
				if tweet_sum.get('coords'):
					date, coords, craving = tweet_sum.values()
					longitude, latitude = coords['coordinates']
					hour = tweet_hour(date, longitude)

					write2me.writerow([hour, longitude, latitude, craving])


# tc = Twitter_Cravings('cravings.txt')
# tc()
# print tc.most_common_cravings(25)
# tc.write_output()

generate_sql_db()



