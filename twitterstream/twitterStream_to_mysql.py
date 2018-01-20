#!/usr/bin/python3
import sys
sys.path.append('/usr/lib/python35.zip')
sys.path.append('/usr/lib/python3.5')
sys.path.append('/usr/lib/python3.5/plat-arm-linux-gnueabihf')
sys.path.append('/usr/lib/python3.5/lib-dynload')
sys.path.append('/home/pi/.local/lib/python3.5/site-packages')
sys.path.append('/usr/local/lib/python3.5/dist-packages')
sys.path.append('/usr/lib/python3/dist-packages')

# supplied by http://pythonprogramming.net/twitter-api-streaming-tweets-python-tutorial/
# further info: http://miningthedetails.com/blog/python/TwitterStreamsPythonMySQL/


from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import mysql.connector
from mysql.connector import errorcode
import time
import json



# set up connection to db
# make sure to set charset to 'utf8mb4' to support emoji

cnx = mysql.connector.connect(user='root', password='makemoney',
                              host='localhost',
                              database='stockfind',
                              charset = 'utf8mb4')
cursor=cnx.cursor()


#Twitter consumer key, consumer secret, access token, access secret


# set up stream listener
class listener(StreamListener):

    def on_data(self, data):
        all_data = json.loads(data)
	# collect all desired data fields 
        if 'text' in all_data:
          tweet         = all_data["text"]
          created_at    = all_data["created_at"]
          retweeted     = all_data["retweeted"]
          username      = all_data["user"]["screen_name"]
          user_tz       = all_data["user"]["time_zone"]
          user_location = all_data["user"]["location"]
          user_coordinates   = all_data["coordinates"]
		  
	  # if coordinates are not present store blank value
	  # otherwise get the coordinates.coordinates value
          if user_coordinates is None:
            final_coordinates = user_coordinates
          else:
            final_coordinates = str(all_data["coordinates"]["coordinates"])
		  
	  # insert values into the db
          cursor.execute("INSERT INTO tweetTable (created_at, username, tweet, coordinates, userTimeZone, userLocation, retweeted) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (created_at, username, tweet, final_coordinates, user_tz, user_location, retweeted))
          cnx.commit()
          
          #          print((username,tweet))
          #          print((user_coordinates, user_tz))
          #          print((user_location, retweeted))
          #          print("")
          
          return True
        else:
          return True

    def on_error(self, status):
        print(status)

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

# create stream and filter on a searchterm
twitterStream = Stream(auth, listener())
twitterStream.filter(track=["penny stock", "penny share", "pennystock", "pennyshare"],
  languages = ["en"], stall_warnings = True)

