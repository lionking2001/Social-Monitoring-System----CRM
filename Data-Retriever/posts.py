import urllib2
import json
import datetime
import csv
import time

page_id = "5550296508"
access_token = "EAACEdEose0cBADMooZBPDBx3j1N4L4vdZCMyMQfjegOctvKnxC1UNZAjQCeXumIDEw2UfcfJyVbfa99dKFZCzf0TQONX5rPL9eoS9afBzGMKv0IklgJSXZATWGZCE3jyURmbPN6M4JqKbJW2s1cijZC9P2isCZCeCXOOCZC9A5ToP2A1YjsZC08JsNOySWp88K6ZCUZD"


def request_until_succeed(url):
    req = urllib2.Request(url)
    success = False
    while success is False:
        try:
            response = urllib2.urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception, e:
            print e
            time.sleep(5)

            print "Error for URL %s: %s" % (url, datetime.datetime.now())
            print "Retrying."

    return response.read()


# ------------------------------------Unicode----------------------------------
def unicode_normalize(text):
    return text.translate({0x2018: 0x27, 0x2019: 0x27, 0x201C: 0x22, 0x201D: 0x22,
                           0xa0: 0x20}).encode('utf-8')


def getFacebookPageFeedData(page_id, access_token, num_statuses):
    base = "https://graph.facebook.com/v2.12"
    node = "/%s/posts" % page_id
    fields = "/?fields=message,link,permalink_url,created_time,type,name,id," + \
             "comments.limit(0).summary(true),shares,reactions" + \
             ".limit(0).summary(true)"
    parameters = "&limit=%s&access_token=%s" % (num_statuses, access_token)
    url = base + node + fields + parameters

    data = json.loads(request_until_succeed(url))

    return data


def getFacebookCommentFeedData(status_id, access_token, num_comments):
    # Construct the URL string
    base = "https://graph.facebook.com/v2.6"
    node = "/%s/comments" % status_id
    fields = "?fields=id,message,like_count,created_time,comments,from,attachment"
    parameters = "&order=chronological&limit=%s&access_token=%s" % \
                 (num_comments, access_token)
    url = base + node + fields + parameters

    # retrieve data
    data = request_until_succeed(url)
    if data is None:
        return None
    else:
        return json.loads(data)
    return data



# ----------------------------------Facebook Comment-----------------------


def getReactionsForStatus(status_id, access_token):
    base = "https://graph.facebook.com/v2.6"
    node = "/%s" % status_id
    reactions = "/?fields=" \
                "reactions.type(LIKE).limit(0).summary(total_count).as(like)" \
                ",reactions.type(LOVE).limit(0).summary(total_count).as(love)" \
                ",reactions.type(WOW).limit(0).summary(total_count).as(wow)" \
                ",reactions.type(HAHA).limit(0).summary(total_count).as(haha)" \
                ",reactions.type(SAD).limit(0).summary(total_count).as(sad)" \
                ",reactions.type(ANGRY).limit(0).summary(total_count).as(angry)"
    parameters = "&access_token=%s" % access_token
    url = base + node + reactions + parameters
    data = json.loads(request_until_succeed(url))
    return data


def processFacebookPageFeedStatus(status, access_token):
    status_id = status['id']
    status_message = '' if 'message' not in status.keys() else \
        unicode_normalize(status['message'])
    link_name = '' if 'name' not in status.keys() else \
        unicode_normalize(status['name'])
    status_type = status['type']
    status_link = '' if 'link' not in status.keys() else \
        unicode_normalize(status['link'])
    status_permalink_url = '' if 'permalink_url' not in status.keys() else \
        unicode_normalize(status['permalink_url'])
    status_published = datetime.datetime.strptime(
        status['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
    status_published = status_published + \
                       datetime.timedelta(hours=-5)
    status_published = status_published.strftime(
        '%Y-%m-%d %H:%M:%S')
    num_reactions = 0 if 'reactions' not in status else \
        status['reactions']['summary']['total_count']
    num_comments = 0 if 'comments' not in status else \
        status['comments']['summary']['total_count']
    num_shares = 0 if 'shares' not in status else status['shares']['count']

    reactions = getReactionsForStatus(status_id, access_token) if \
        status_published > '2016-02-24 00:00:00' else {}

    num_likes = 0 if 'like' not in reactions else \
        reactions['like']['summary']['total_count']

    num_likes = num_reactions if status_published < '2016-02-24 00:00:00' \
        else num_likes

    def get_num_total_reactions(reaction_type, reactions):
        if reaction_type not in reactions:
            return 0
        else:
            return reactions[reaction_type]['summary']['total_count']

    num_loves = get_num_total_reactions('love', reactions)
    num_wows = get_num_total_reactions('wow', reactions)
    num_hahas = get_num_total_reactions('haha', reactions)
    num_sads = get_num_total_reactions('sad', reactions)
    num_angrys = get_num_total_reactions('angry', reactions)

    return (status_id, status_message, link_name, status_type, status_link, status_permalink_url,
            status_published, num_reactions, num_comments, num_shares,
            num_likes, num_loves, num_wows, num_hahas, num_sads, num_angrys)


def scrapeFacebookPageFeedStatus(page_id, access_token):
    with open('%s_facebook_statuses.csv' % page_id, 'wb') as file:
        w = csv.writer(file)
        w.writerow(["status_id", "status_message", "link_name", "status_type",
                    "status_link", "permalink_url", "status_published", "num_reactions",
                    "num_comments", "num_shares", "num_likes", "num_loves",
                    "num_wows", "num_hahas", "num_sads", "num_angrys"])

        has_next_page = True
        num_processed = 0  # keep a count on how many we've processed
        scrape_starttime = datetime.datetime.now()

        print "Scraping %s Facebook Page: %s\n" % (page_id, scrape_starttime)

        statuses = getFacebookPageFeedData(page_id, access_token, 100)

        while has_next_page:
            for status in statuses['data']:
                if 'reactions' in status:
                    w.writerow(processFacebookPageFeedStatus(status,
                                                             access_token))
                num_processed += 1
                if num_processed % 100 == 0:
                    print "%s Statuses Processed: %s" % \
                          (num_processed, datetime.datetime.now())

            if 'paging' in statuses.keys():
                statuses = json.loads(request_until_succeed(
                    statuses['paging']['next']))
            else:
                has_next_page = False

        print "\nDone!\n%s Statuses Processed in %s" % \
              (num_processed, datetime.datetime.now() - scrape_starttime)


# -----------------------------------Comment retriever----------------------


if __name__ == '__main__':
    scrapeFacebookPageFeedStatus(page_id, access_token)
