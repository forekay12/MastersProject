import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import helper_methods as shm
import mysql.connector
from mysql.connector import Error


consumer_key = 'X4d68DK8xv1iT2sEZnILFW4mm'
consumer_secret = 'LFKNV0USuF0Bbm8NxzTaCVuqmzFA7zO8rO5dmahL4EbkermhR8'
access_token = '1169155924080173056-qT6gaMBBTd8TKtfwwqiS7InpPQ1BRB'
access_secret = 'eq4Br4LsVbXJopCLJumm8KtldOV09dcxyez7BjkqkMims'

def get_values(tweet):
    tweet_coordinates = shm.get_coordinates(tweet)
    language = shm.get_language(tweet)
    created_at = shm.get_created_at(tweet)
    tweet_id = shm.get_tweet_id(tweet)
    user_id = shm.get_user_id(tweet)
    text = shm.get_tweet_text(tweet)
    location = shm.get_location(tweet)
    name_of_place = shm.get_name_of_place(tweet)
    hashtags = shm.get_hashtags(tweet)
    return [created_at, tweet_id, user_id, text, location, name_of_place, language, hashtags, tweet_coordinates, 3]

def connection():
    try:
        conn = mysql.connector.connect(host='127.0.0.1',
                                       database='THESIS',
                                       user='root',
                                       password='jenmenna',
                                       auth_plugin='mysql_native_password')

        if conn.is_connected():
            print("Successful connection")
            return conn
    except Error as e:
        print("Failed to connect")
        print(e)
        conn.close()

def upload_to_db(tweet):
    query = "INSERT INTO THESIS.TwitterData(THESIS.TwitterData.created_at, THESIS.TwitterData.tweet_id, THESIS.TwitterData.user_id, THESIS.TwitterData.text, THESIS.TwitterData.location, THESIS.TwitterData.name_of_place, THESIS.TwitterData.language, THESIS.TwitterData.hashtags, THESIS.TwitterData.tweet_coordinates, THESIS.TwitterData.flu)" \
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    args = (tweet[0], tweet[1], tweet[2], tweet[3], tweet[4], tweet[5], tweet[6], tweet[7], tweet[8], tweet[9])

    try:
        conn = connection()
        cursor = conn.cursor()
        cursor.execute(query, args)
        if cursor.lastrowid:
            print('last insert id', cursor.loastrowid)
        else:
            print('last insert id not found')

        conn.commit()
    except Error as error:
        print(error)

    finally:
        cursor.close()
        conn.close()


class TweetsListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            tweet = json.loads(data)
            values = get_values(tweet)
            language = shm.get_tweet_language(tweet)

            if (values != None) and (language == 'en') and ("flu" in tweet['text'] or "Flu" in tweet['text'] or
                                     "influenza" in tweet['text'] or "Influenza" in tweet['text'] or
                                     "fever" in tweet['text'] or "Fever" in tweet['text'] or
                                     "Runny nose" in tweet['text'] or "stuffy nose" in tweet['text'] or
                                     "runny nose" in tweet['text'] or "Stuffy nose" in tweet['text'] or
                                     "Headaches" in tweet['text'] or "headaches" in tweet['text'] or
                                    "vomiting" in tweet['text'] or "Vomiting" in tweet['text'] or
                                     "Diarrhea" in tweet['text'] or "diarrhea" in tweet['text'] or
                                     "cough" in tweet['text'] or "Cough" in tweet['text'] or
                                     "Sore throat" in tweet['text'] or "sore throat" in tweet['text']):
                upload_to_db(values)

                text = shm.get_tweet_text(tweet)
                user_id = shm.get_user_id(tweet)
                created_at = shm.get_created_at(tweet)
                location = shm.get_location(tweet)
                location_name = shm.get_name_of_place(tweet)
                coordinates = shm.get_coordinates(tweet)

                print("----------TWEET----------")
                print("Text: ", end=" ")
                print(text)
                print("User ID: ", end=" ")
                print(user_id)
                print("Created At: ", end=" ")
                print(created_at)
                print("User Location: ", end=" ")
                print(location)
                print("Location: ", end=" ")
                print(location_name)
                print("Coordinates: ", end=" ")
                print(coordinates)


                if tweet['user']['location']:
                    loc = str(location)
                    self.client_socket.send((loc + "\n").encode("utf-8"))
                #else:
                    #loc = "None"
                #self.client_socket.send(bytes(loc, "utf-8"))
                t = tweet['text']

                #self.client_socket.send(tweet['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['flu', 'Flu', 'influenza', 'Influenza', 'fever', 'Fever',
                                 'Runny nose', 'stuffy nose', 'runny nose', 'Stuffy nose', 'Headaches', 'headaches',
                                 'vomiting', 'Vomiting', 'Diarrhea', 'diarrhea', 'cough', 'Cough',
                                 'Sore throat', 'sore throat'])

if __name__ == "__main__":

    s = socket.socket()
    host = "127.0.0.1"
    port = 9009
    s.bind((host, port))
    print("Listening on port: %s" % str(port))

    s.listen(5)
    c, addr = s.accept()
    print("Recieved request from: " + str(addr))
    sendData(c)