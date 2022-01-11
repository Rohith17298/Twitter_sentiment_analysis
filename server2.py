
import json
import tweepy
import re
import socket

CONSUMER_SECRET ="TWNELcC6BsrEnbcdPeG2Ta6hFvoSFZeds8RrOmGoRpQpglbsL0"
CONSUMER_KEY = "LI6Ik8YWro4Wcuib3ssHj53z1"
ACCESS_TOKEN_SECRET = "5SxCztI5B6bPJUAPmINXci5WjNk5DWJwo8mlliYF7Yv5b"
ACCESS_TOKEN = "1464348805390872580-9lkdUvsLDWcCuIwMB0ymY1GMFLLsFz"


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

class MyStreamListener(tweepy.StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, raw_data):
        try:
            data = json.loads(raw_data)
	    noEmojiTwt = data['text'].encode('ascii', 'ignore').decode('ascii') if data['text'] else None
	    preprocessedTwt = ' '.join(re.sub("(@\w+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", noEmojiTwt).split())
            print(preprocessedTwt)
            self.client_socket.send((preprocessedTwt + "\n").encode("utf-8"))
            return True
        except BaseException as e:
            print("Error: " + str(e))
        return True

    def on_error(self, status_code):
        print(status_code)
        return True 


soc = socket.socket()
host = "localhost"
port = 12346
soc.bind((host, port))
soc.listen(4)
client, address = soc.accept()

myStreamListener = MyStreamListener(csocket=client)
myStream = tweepy.Stream(auth=auth, listener=myStreamListener, )
myStream.filter(track=["covid"], languages=["en"])
