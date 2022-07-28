auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token_new, access_token_secret_new)
api = tweepy.API(auth)

########## Collect user's timeline
def user_tl(n, user, attr, term=''):
    """
    collects all tweets made by a user (tl = timeline)
    Parameters
    ----------
    n : number of tweets to fetch
    user : user whose tweets to fetch
    attr : attributes to record
    term : TYPE, optional
        if searching for a keyword. The default is ''

    Returns
    -------
    df : data frame containing attribute values
    """

    # initialise output
    output = {}
    for a in attr:
        output[a] = []
    if 'retweeted' in attr:
        output['rt_from'] = []


    # get data
    user_list = tweepy.Cursor(api.user_timeline, screen_name=user,
            tweet_mode='extended').items(n)

    sleep(60)

    # process data & add to dict
    for tweet in user_list:
        if term in tweet.full_text:
            for a in attr:
                b = getattr(tweet,a)
                if a == 'created_at':
                    b = pd.to_datetime(b)
                output[a].append(b)

        sleep(10)


    #  finally, make dataframe
    df = pd.DataFrame.from_dict(output)
    return df


#######  Get conversations

def get_bearer_header():
    """
    returns bearer header of elevated account
    """
    uri_token_endpoint = 'https://api.twitter.com/oauth2/token'
    key_secret = f"{api_key}:{api_key_secret}".encode('ascii')
    b64_encoded_key = base64.b64encode(key_secret)
    b64_encoded_key = b64_encoded_key.decode('ascii')

    auth_headers = {
       'Authorization': 'Basic {}'.format(b64_encoded_key),
       'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
       }

    auth_data = {
       'grant_type': 'client_credentials'
       }

    auth_resp = requests.post(uri_token_endpoint, headers=auth_headers,
                data=auth_data)

    bearer_header = {
       'Accept-Encoding': 'gzip',
       'Authorization': 'Bearer {}'.format(bearer_token),
       'oauth_consumer_key': api_key
    }
    return bearer_header

def get_CID(ID):
    # gets the conversation ID of a tweet
    uri = 'https://api.twitter.com/2/tweets?'

    params = {
       'ids':ID,
       'tweet.fields':'conversation_id'
    }

    bearer_header = get_bearer_header()
    resp = requests.get(uri, headers=bearer_header, params=params)
    return resp.json()['data'][0]['conversation_id']


def get_Conv(conversation_id):
    """
    given a conversation ID (the ID of the orig tweet), returns a list
    each element corresponds to a reply to the orig tweet
    each element is a dictionary containing the conversation_id, id, and text
    first element is original tweet
    """

    uri = 'https://api.twitter.com/2/tweets/search/all'

    params = {'query': f'conversation_id:{conversation_id}',
       'tweet.fields': 'in_reply_to_user_id',
       'tweet.fields':'conversation_id',
              'max_results':500
    }

    bearer_header = get_bearer_header()
    resp = requests.get(uri, headers=bearer_header, params=params)

    assert resp.json() != {'meta': {'result_count': 0}}, "error encountered"

    if resp.json() == {'meta': {'result_count': 0}}:
        print('no tweets found in lookup for', conversation_id)
    orig = {'conversation_id':conversation_id, 'id':conversation_id,
            'text':api.get_status(conversation_id)._json['text'],
                'in_reply_to_status_id': 'Root'}
    whole = [orig] + resp.json()['data']
    return whole

##### Add parameters and original IDs

def add_params(lis):
    """
    given a list 'lis' of dictionaries of tweets, adds user ID and screen name,
    user and status being replied to
    if applicable, and time of creation
    """
    for dic in lis:
        twid = int(dic['id'])
        stat = api.get_status(twid)
        dic['in_reply_to_user_id'] = stat.in_reply_to_user_id_str
        dic['in_reply_to_status_id'] = stat.in_reply_to_status_id_str
        if stat.in_reply_to_status_id_str is None:
            dic['in_reply_to_user_id'] = 'Root'
            dic['in_reply_to_status_id'] = 'Root'
        dic['user_name'] = stat._json['user']['screen_name']
        dic['user_id'] = stat._json['user']['id_str']
        dic['created_at'] = pd.to_datetime(stat._json['created_at'])
    return lis

def orig_id_list(df):
    """
    if tweet is a retweet, return ID of original tweet and not
    that of retweeted instance
    """
    idlist = list(df['id'])
    # go through tweet IDs
    for i,ID in enumerate(idlist):
        # lookup tweet
        tweet = api.get_status(ID)
        # is it an RT?
        if 'retweeted_status' in tweet._json.keys():
            ID = tweet._json['retweeted_status']['id']

        # root tweet ID
        idlist[i] = get_CID(ID)
        sleep(15) # to avoid rate limit
    return idlist

#### Retrieve conversations en masse


def scrape(ids):
    """
    given list of original tweet ids, retrieves the full conversations
    """
    from time import sleep
    bigdf = pd.DataFrame()
    issues = 0
    for ID in ids:
        try:
            # fetch conversation
            cascade = get_Conv(ID)
            # add attributes
            cascade = add_params(cascade)

            # add onto output
            tempdf = pd.DataFrame(cascade)
            bigdf = bigdf.append(tempdf)
        except:
            # track errors
            print('something went wrong with ' + str(ID))
            issues += 1

        sleep(30) # avoid rate limits

    print('number of tweets that raised issues: ', issues)
    return bigdf

#### Add user info

def adduserparams(casc):
    """
    adds a user's follower, friend count and verified status to dataframe
    """
    users = casc['user_id']
    # avoid unnecessarily looking people up twice
    checked = []
    issues = 0

    # initialise output
    userinfo = {'verified':[None]*len(users), 'followers_count':[None]*len(users),
            'friends_count':[None]*len(users)}
    for i, u in enumerate(users):
        if u in checked:
            # if we've already looked up the user just use previous info
            j = checked.index(u)
            userinfo['verified'][i] = userinfo['verified'][j]
            userinfo['followers_count'][i] = userinfo['followers_count'][j]
            userinfo['friends_count'][i] =  userinfo['friends_count'][j]
        else:
            try:
                # user lookup, value adding
                user = api.get_user(user_id=u)
                userinfo['verified'][i] = user._json['verified']
                userinfo['followers_count'][i] = user._json['followers_count']
                userinfo['friends_count'][i] = user._json['friends_count']
            except:
                # track errors
                print('something went wrong with ' +str(u))
                userinfo['verified'][i] = 'error'
                userinfo['followers_count'][i] = 'error'
                userinfo['friends_count'][i] = 'error'
                issues += 1
        sleep(10)

        checked.append(u)

    userdf = pd.DataFrame(userinfo) # make df

    # add to input df
    casc['verified'] = userdf['verified']
    casc['followers_count'] = userdf['followers_count']
    casc['friends_count'] = userdf['friends_count']
    print('number of users that raised issues: ', issues)

    return casc

#### All an account's conversations

def collect(attr, name, earliest, latest, csv1, csv2, n=100000):
    """
    collects all conversations from an account
    attr: attributes to collect information on in initial tweet collection
    name: name of account we collect from
    earliest: earliest date to collect tweets from
    csv1: name of the csv to save the inital tweet collection to
    csv2: name of the csv to save the cascade to
    n: max no of tweets to collect info on in initial collection
    """
    # collect tweets
    tweetdf = user_tl(n, name, attr, GetRT=False, term='')

    # don't go back too far in time
    tweetdf = tweetdf[~(tweetdf['created_at'] < earliest)]
    tweetdf = tweetdf[~(tweetdf['created_at'] > latest)]

    for i,n in enumerate(tweetdf['in_reply_to_status_id']):
        if n is int:
            if n == None or np.isnan(n):
                tweetdf['in_reply_to_status_id'].iloc[i] = 'Root'
                tweetdf['in_reply_to_user_id'].iloc[i] = 'Root'
    # store
    tweetdf.to_csv(csv1)
    print(name, ': save 1 done') #track progress

    # only original tweets
    tweetdf = tweetdf[pd.isna(tweetdf['in_reply_to_status_id'])]

    sleep(180)

    # original, not retweet, id
    idlist = orig_id_list(tweetdf)

    sleep(180)

    # get all conversations
    casc = scrape(idlist)

    # mark root tweets
    for i,n in enumerate(casc['in_reply_to_status_id']):
        if n is int:
            if n == None or np.isnan(n):
                casc['in_reply_to_status_id'].iloc[i] = 'Root'
                casc['in_reply_to_user_id'].iloc[i] = 'Root'

    # track progress
    print(name, ': scrape done')

    casc = adduserparams(casc)
    print(name, ': added user params')

    # final save
    casc.to_csv(csv2)

    return casc


#### All conversations from a series of accounts

def collect_all(attr, namelist, earliest, latest, csvlist1, csvlist2, bigcsv, n=10^5):
    """
    runs collect() on a series of accounts
    attr: attributes to collect information on in initial tweet collection
    namelist: names of account we collect from
    earliest: earliest date to collect tweets from
    csvlist1: names of the csv to save the inital tweet collection to
    csvlist2: names of the csv to save the cascade to
    n: max no of tweets to collect info on in initial collection
    """
    # initialise
    bigdf = pd.DataFrame()

    # loop through all accounts and collect all conversations
    for i in range(len(namelist)):
        casc = collect(attr, namelist[i], earliest, csvlist1[i], csvlist2[i])
        bigdf = bigdf.append(casc)
        sleep(360)
    bigdf.to_csv(bigcsv)
    return bigdf


#### Reconstruction


def recons_df(df, attr, savename):
    """
    trawls through the dataframe looking for missing tweets and
    attempting to recover them
    this is lengthy -- every 50 attempted recoveries, reports progress and saves
    """
    err = 0
    look = 0

    ids =  list(df['id'])
    reply = list(df['in_reply_to_status_id'])
    conv = list(df['conversation_id'])
    reply[0] = ids[0]
    origid = ids.copy()

    output = {}
    for a in attr:
        output[a] = []

    for n,i in enumerate(reply):
        # check if we know about this tweet
        if i == 'Root':
            pass
        elif i in ids or int(i) in ids:
            pass
        else:
            if i == '0' or i == 0 or i == "Root":
                break
            try:
                look += 1
                # look up the tweet
                stat = api.get_status(i)
                # record it in list of id's
                ids.append(i)
                # make sure we also know what conversation it's part of
                # (same one as the reply)
                conv.append(conv[n])

                # if this is an original tweet, mark it as such
                if stat.in_reply_to_status_id == None:
                    reply.append('Root')

                # if not, get the ID of the tweet it's replying to
                else:
                    reply.append(stat.in_reply_to_status_id)
                    # reply.append(conv[n])
                sleep(10)

                # finally, add everything to the dictionary
                for a in attr:
                    if a == 'conversation_id':
                        b = conv[n]
                    else:
                        b = getattr(stat,a)
                        if a == 'created_at':
                            b = pd.to_datetime(b)
                    output[a].append(b)
                    print('lookup no errors')


            except:
                # if the above didn't work, usually the tweet was deleted or something
                # this is ONLY called if the tweet lookup didn't work
                # print('status ' + str(i) + ' raised an error')
                # record it anyway
                ids.append(i)
                # make this link back to the root node
                reply.append(conv[n])
                conv.append(conv[n])

                for a in attr:
                    if a == 'conversation_id':
                        b = conv[n]
                    else:
                        b = 'error'
                    output[a].append(b)

                err += 1
                sleep(10)
                print(n,i)

            if look%50==0 and look != 0:
                print('performed '+str(look)+' lookups,
                and gone through '+str(n)+' tweets
                with '+str(err)+' errors')
                print(output)
                print([len(output[a]) for a in output.keys()])
                newdf = pd.DataFrame.from_dict(output)
                newdf.to_csv(savename)

                sleep(60)

    print('had to lookup ' + str(look) + ' times')
    print('issues with ' +str(err) + ' tweets')

    newdf = pd.DataFrame.from_dict(output)

    return newdf
