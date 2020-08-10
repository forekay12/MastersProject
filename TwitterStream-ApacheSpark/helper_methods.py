def get_tweet_text(tweet):
    if 'text' in tweet:
        if tweet['text']:
            try:
                tweetText = tweet['text']
                return tweetText
            except:
                print("Could not get the text field from tweet: ", tweet)


def get_created_at(tweet):
    if 'created_at' in tweet:
        if tweet['created_at']:
            try:
                created_at = tweet['created_at']
                return created_at
            except:
                print("Could not get the created_at field from tweet: ", tweet)


def get_user_id(tweet):
    if 'user' in tweet:
        if tweet['user']:
            user = tweet['user']
            if 'id_str' in user:
                if user['id_str']:
                    try:
                        user_id = user['id_str']
                        return user_id
                    except:
                        print("Could not get the user_id field from tweet: ", tweet)


def get_tweet_language(tweet):
    try:
        if 'lang' in tweet:
            if tweet['lang']:
                language = tweet['lang']
                return language
    except:
        print("Had trouble getting language from get_language_tweet method.")


def get_user_language(tweet):
    try:
        if 'user' in tweet:
            if tweet['user']:
                user = tweet['user']
                if 'lang' in user:
                    if user['lang']:
                        language = user['lang']
                        return language
    except:
        print("Had trouble getting language from get_language_tweet method.")


def get_language(tweet):
    language = get_user_language(tweet)
    if (language == None):
        language = get_tweet_language(tweet)
    return language


def get_tweet_id(tweet):
    if 'id_str' in tweet:
        if tweet['id_str']:
            try:
                tweet_id = tweet['id_str']
                return tweet_id
            except:
                print("Could not get the tweet_id field from tweet: ", tweet)


def get_location(tweet):
    if 'user' in tweet:
        if tweet['user']:
            user = tweet['user']
            if 'location' in user:
                if user['location']:
                    try:
                        location = user['location']
                        return location
                    except:
                        print("Could not get the location from tweet: ", tweet)


def get_user_id(tweet):
    if tweet['user']['id_str']:
        try:
            user_id = tweet['user']['id_str']
            return user_id
        except:
            print("Could not get the user_id field from tweet: ", tweet)


def get_name_of_place(tweet):
    if 'place' in tweet:
        if tweet['place']:
            try:
                place = tweet['place']
                if 'full_name' in place:
                    location = place['full_name']
                    return location
            except:
                print("Count not get the name_of_place from tweet: ", tweet)


def get_hashtags(tweet):
    if 'entities' in tweet:
        if tweet['entities']:
            entities = tweet['entities']
            if 'hashtags' in entities:
                if entities['hashtags']:
                    hashtags = entities['hashtags']
                    if 'text' in hashtags[0]:
                        if hashtags[0]['text']:
                            try:
                                hashtags_text = hashtags[0]['text']
                                return hashtags_text
                            except:
                                print("Could not get the hashtags from tweet: ", tweet)


def get_coordinates_from_tweet(tweet):
    if 'coordinates' in tweet:
        if tweet['coordinates']:
            tweet_coordinates = tweet['coordinates']
            if 'coordinates' in tweet_coordinates:
                if tweet_coordinates['coordinates']:
                    try:
                        coordinates = tweet_coordinates['coordinates']
                        return coordinates
                    except:
                        print("Problem with getting the coordinates from tweet: ", tweet)


def get_coordinates_from_user(tweet):
    if 'user' in tweet:
        if tweet['user']:
            user = tweet['user']
            if 'derived' in user:
                if user['derived']:
                    derived = user['derived']
                    if 'locations' in derived:
                        if derived['locations']:
                            locations = derived['locations']
                            if 'geo' in locations:
                                if locations['geo']:
                                    geo = locations['geo']
                                    if 'coordinates' in geo:
                                        if geo['coordinates']:
                                            try:
                                                coordinates = geo['coordinates']
                                                return coordinates
                                            except:
                                                print("Problem with getting the coordinates from tweet: ", tweet)


def centroid(coordinates):
    to_return = [0] * 2
    size = len(coordinates[0])
    for lat in coordinates[0]:
        to_return[0] += lat[0]
        to_return[1] += lat[1]

    to_return[0] = to_return[0] / size
    to_return[1] = to_return[1] / size
    return to_return


def get_coordinates_from_place(tweet):
    if 'place' in tweet:
        if tweet['place']:
            place = tweet['place']
            if 'bounding_box' in place:
                if place['bounding_box']:
                    bounding_box = place['bounding_box']
                    if 'coordinates' in bounding_box:
                        if bounding_box['coordinates']:
                            try:
                                coordinates = bounding_box['coordinates']
                                computed_centroid = centroid(coordinates)
                                return computed_centroid
                            except:
                                print("Problem with getting the coordinates from tweet ", tweet)


def get_coordinates_from_geo(tweet):
    if 'geo' in tweet:
        if tweet['geo']:
            geo = tweet['geo']
            if 'coordinates' in geo:
                if geo['coordinates']:
                    try:
                        to_return = geo['coordinates']
                        return to_return
                    except:
                        print("Problem with getting the coordinates from tweet ", tweet)


def get_coordinates(tweet):
    # check the tweet itself for coordinates
    coordinates = get_coordinates_from_place(tweet)

    if coordinates == None:
        coordinates = get_coordinates_from_user(tweet)
        if coordinates == None:
            coordinates = get_coordinates_from_geo(tweet)
            if coordinates == None:
                coordinates = get_coordinates_from_tweet(tweet)

    return coordinates