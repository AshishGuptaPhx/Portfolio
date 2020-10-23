#!/Users/agupt323/AppData/Local/Programs/Python/Python36 python
import codecs
import requests
import math

from datetime               import *
#from email.mime.application import MIMEApplication
#from email.mime.multipart   import MIMEMultipart
#from email.mime.text        import MIMEText
#from email.utils            import COMMASPACE, formatdate

global __HOME_DIR__
global __FOLDER_LIST_FILE__
global __NAC_OUTPUT_FILE__
global __MERCHANT_ERS_FILE__

#Setting up the Home Folder. It can be passed as an configuration argument
__HOME_DIR__ = 'C:\\Code\\Crawlee'

#Setting up all file locations.
__NAC_OUTPUT_FILE__        = __HOME_DIR__ + '\\Data\\NAC_Results.txt'
__NAC_LOG__                = __HOME_DIR__ + '\\Data\\NAC_Log.txt'
__ERS_OUTPUT_FILE__        = __HOME_DIR__ + '\\Data\\ERS_Results.txt'
__ERS_LOG__                = __HOME_DIR__ + '\\Data\\ERS_Log.txt'
__MERCHANT_ERS_FILE__      = __HOME_DIR__ + '\\Data\\Merchant_ERS_Summary.txt'

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def Cr_get_artifacts (arg_crawler, arg_entities, arg_token, arg_from_date, arg_to_date, arg_var_list, arg_output_mode, arg_payload):
    """Searches for published articles through various web-crawlers and returns a json containing the list of articles along with its several attributes

    Args:
        arg_crawler   (Crawler)             - Crawler API to be used (NEWSAPI/NEWSRIVER/TWITTER)
        arg_entities
        arg_token     (Search Word)         - Word to be searched.
        arg_from_date (From Date)           - From Date (YYYY-MM-DD)
        arg_to_date   (To Date)             - To Date (YYYY-MM-DD)
        arg_var_list  (Output Variables)    - Variables to return
        arg_output_mode                     -
        arg_payload                         -

    Returns:
        A JSON Object containing List of variables from Crawler Run Results such as:
            > publishDate
            > discoverDate
            > title
            > language
            > text
            > structured Text
            > url
            > elements
            > website
            > metadata
            > highlight
            > score
    """

    #-----------------------------------------------------------------------------------------------------------------
    ## Variables Section
    #-----------------------------------------------------------------------------------------------------------------
    # Local Variables
    date_format = '%Y-%m-%d'
    start_date  = datetime.strptime(arg_from_date, date_format).date()
    end_date    = datetime.strptime(arg_to_date, date_format).date()
    complete_set = []
    article_count = 0

    # http_proxy  = "http://proxy.skynet.com:8080"
    # https_proxy = "https://proxy.skynet.com:8080"
    # ftp_proxy   = "ftp://100.100.1.10:3128"
    # proxy_dict = { "http": http_proxy, "https": https_proxy }

    #Conforming the Arguments Values
    arg_crawler = arg_crawler.upper()

    w_NAC_file = codecs.open(__NAC_OUTPUT_FILE__, 'w', "utf-8")
    w_NAC_log  = codecs.open(__NAC_LOG__, 'w', "utf-8")

    #Configuring Crawler Specific Variables
    if arg_crawler == 'NEWSRIVER':

        NR_url = 'https://api.newsriver.io/v2/search?query=text%3A@@ARG_TOKEN%20AND%20language%3AEN%20AND%20publishDate%3A%5B@@ARG_FROM_DATE%20TO%20@@ARG_TO_DATE%5D*&sortBy=_score&sortOrder=DESC&limit=100'
        NR_url = NR_url.replace('@@ARG_TOKEN', arg_token)
        auth_key = 'sBBqsGXiYgF0Db5OV5tAwxvg5LqbRRNQF2vDd2yC24SZM38NGEqRrY7ZsS9H4HA1n2pHZrSf1gT2PUujH1YaQA'

        for single_date in daterange(start_date, end_date):
            x_date = single_date.strftime("%Y-%m-%d")

            # Setting up the Crawler URL along with arguments
            crawler_url = NR_url
            crawler_url = crawler_url.replace('@@ARG_FROM_DATE', x_date)
            crawler_url = crawler_url.replace('@@ARG_TO_DATE', x_date)

            response = requests.get(crawler_url, headers={"Authorization": auth_key})

            if (response.ok):
                response_value = response.json()
                article_count = 0
                for line in response_value:
                    article_count += 1
                    complete_set.append(line)

            line = ''
            i = 0

            line = 'Articles searched for :' + x_date + '. Articles found :' + str(article_count) + '\n'
            w_NAC_log.write(line)
            print(line)

        for item in complete_set:
            i += 1
            try:
                line = item['url'] + ',\'' + item['title'] + '\',' + '\'' + item['publishDate'] + '\''
            except KeyError:
                line = item['url'] + ',\'' + item['title'] + '\','

            print(i, line)
            w_NAC_file.write(line + '\n')

    elif arg_crawler == 'NEWSAPI':
        arg_parms = {}
        ERS_Flag = arg_payload['ers_flag']
        arg_key = '445d8b54f0e2493ba0bb71d9c7692307'
        arg_url = 'https://newsapi.org/v2/everything?apiKey=@@ARG_KEY'

        token = arg_token
        arg_parms['from'] = arg_from_date
        arg_parms['to'] = arg_from_date
        arg_parms['language'] = 'en'
        arg_parms['pageSize'] = 100
        arg_parms['page'] = 1
        arg_parms['sort_by'] = 'publishedAt'
        # arg_parms['sources']            = arg_sources
        # arg_parms['domains']            = arg_domains
        # arg_parms['excludeDomains']     = arg_exclude_domains

        arg_url = arg_url.replace('@@ARG_KEY', arg_key)

        for entity in arg_entities:
            token = arg_token
            token = token.replace ('@@ENTITY', entity)
            arg_parms['q'] = token

            response = requests.get(arg_url, timeout=30, params=arg_parms)
            response_value = response.json()

            if response_value['status'] == 'ok':
                page_count = math.ceil(response_value['totalResults'] / arg_parms['pageSize'])
            else:
                print('Does not work')
                print(response_value)
                exit()

            print('Total Pages: ', page_count)

            for i in range(page_count):
                arg_parms['page'] = i

                response = requests.get(arg_url, timeout=30, params=arg_parms)
                response_value = response.json()

                if response_value['status'] == 'ok':
                    for article in response_value['articles']:
                        print(article['url'], article['title'], article['publishedAt'])
                else:
                    print('Does not work')

    elif arg_crawler == 'TWITTER':
        pass
    else:
        pass

    w_NAC_log.close()
    w_NAC_file.close()

    #sorted_list = list(set(complete_set))
    #final_result_set = sorted_list.sort()

    return complete_set

# elif arg_crawler == 'GOOGLE':
#     crawler_url = 'http://news.google.com.br/news?cf=all&ned=us&hl=en&output=rss'
#     # just some GNews feed - I'll use a specific search later
#
#     feed = parse(crawler_url)
#     for post in feed.entries:
#         # dict_keys(['title', 'title_detail', 'links', 'link', 'id', 'guidislink', 'tags', 'published', 'published_parsed', 'summary', 'summary_detail'])
#         print(post.link)
#         # print (post.keys())

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
def Cr_news_articles (arg_crawler, arg_entities, arg_token, arg_from_date, arg_to_date, arg_var_list, arg_output_mode):
    """Searches for published articles through various web-crawlers and returns a json containing the list of articles along with its several attributes

    Args:
        arg_crawler   (Crawler)             - Crawler API to be used (GOOGLE/NEWSRIVER/TWITTER)
        arg_token     (Search Word)         - Word to be searched.
        arg_from_date (From Date)           - From Date (YYYY-MM-DD)
        arg_to_date   (To Date)             - To Date (YYYY-MM-DD)
        arg_var_list  (Output Variables)    - Variables to return

    Returns:
        A JSON Object containing List of variables from Crawler Run Results such as:
            > publishDate
            > discoverDate
            > title
            > language
            > text
            > structured Text
            > url
            > elements
            > website
            > metadata
            > highlight
            > score
    """
    # Local Variables
    date_format = '%Y-%m-%d'
    start_date  = datetime.strptime(arg_from_date, date_format).date()
    end_date    = datetime.strptime(arg_to_date, date_format).date()
    result_set  = []
    complete_set = []
    final_result_set = []
    http_proxy  = "http://proxy.skynet.com:8080"
    https_proxy = "https://proxy.skynet.com:8080"
    ftp_proxy   = "ftp://10.10.1.10:3128"

    proxy_dict = { "http": http_proxy, "https": https_proxy }

    #Conforming the Arguments Values
    arg_crawler = arg_crawler.upper()

    w_NAC_file = codecs.open(__NAC_OUTPUT_FILE__, 'w', "utf-8")
    w_NAC_log  = codecs.open(__NAC_LOG__, 'w', "utf-8")

    #Configuring Crawler Specific Variables
    if arg_crawler == 'NEWSRIVER':
        NR_url = 'https://api.newsriver.io/v2/search?query=text%3A@@ARG_TOKEN%20AND%20language%3AEN%20AND%20publishDate%3A%5B@@ARG_FROM_DATE%20TO%20@@ARG_TO_DATE%5D*&sortBy=_score&sortOrder=DESC&limit=100'
        NR_url = NR_url.replace('@@ARG_TOKEN', arg_token)

        auth_key = 'sBBqsGXiYgF0Db5OV5tAwxvg5LqbRRNQF2vDd2yC24SZM38NGEqRrY7ZsS9H4HA1n2pHZrSf1gT2PUujH1YaQA'

    elif arg_crawler == 'NEWSAPI':
        NR_url = ('https://newsapi.org/v2/everything?q=@@ARG_TOKEN&from=@@ARG_FROM_DATE&to=@@ARG_TO_DATE&sortBy=popularity&apiKey=@@ARG_KEY')

        auth_key = 'fa152e5f177f4b24ad06b20bd9ba9c29'
        NR_url = NR_url.replace('@@ARG_TOKEN', arg_token)
        NR_url = NR_url.replace('@@ARG_KEY', arg_token)
        NR_url = NR_url.replace('@@ARG_KEY', auth_key)

    else:
        pass

    #Calling Crawler API based on Argument Crawler
    #print(start_date, type(start_date))
    #print(end_date, type(end_date))

    for single_date in daterange(start_date, end_date):
        x_date = single_date.strftime("%Y-%m-%d")

        # Using NewsRiver API
        if arg_crawler == 'NEWSRIVER':
            # Setting up the Crawler URL along with arguments
            crawler_url = NR_url
            crawler_url = crawler_url.replace('@@ARG_FROM_DATE', x_date)
            crawler_url = crawler_url.replace('@@ARG_TO_DATE', x_date)

            #response = requests.get(crawler_url,headers={"Authorization": auth_key}, proxies=proxy_dict)
            if arg_crawler == 'NEWSRIVER':
                response = requests.get(crawler_url, headers={"Authorization": auth_key})
            elif arg_crawler == 'NEWSAPI':
                response = requests.get(crawler_url)
            else:
                pass

            if (response.ok):
                response_value = response.json()
                article_count = 0
                for line in response_value:
                    article_count += 1
                    complete_set.append(line)

            line = ''
            i = 0
        elif arg_crawler == 'NEWSAPI':
            # Setting up the Crawler URL along with arguments
            crawler_url = NR_url
            crawler_url = crawler_url.replace('@@ARG_FROM_DATE', x_date)
            crawler_url = crawler_url.replace('@@ARG_TO_DATE', x_date)

            #response = requests.get(crawler_url,headers={"Authorization": auth_key}, proxies=proxy_dict)
            if arg_crawler == 'NEWSRIVER':
                response = requests.get(crawler_url, headers={"Authorization": auth_key})
            elif arg_crawler == 'NEWSAPI':
                response = requests.get(crawler_url)
            else:
                pass

            if (response.ok):
                response_value = response.json()
                article_count = 0
                for line in response_value:
                    article_count += 1
                    complete_set.append(line)

            line = ''
            i = 0

        elif arg_crawler == 'GOOGLE':
            crawler_url = 'http://news.google.com.br/news?cf=all&ned=us&hl=en&output=rss'
            # just some GNews feed - I'll use a specific search later

            feed = parse(crawler_url)
            for post in feed.entries:
                # dict_keys(['title', 'title_detail', 'links', 'link', 'id', 'guidislink', 'tags', 'published', 'published_parsed', 'summary', 'summary_detail'])
                print(post.link)
                # print (post.keys())
        #Default
        else:
            pass

        line = 'Articles searched for :' + x_date + '. Articles found :' + str(article_count) + '\n'
        w_NAC_log.write(line)
        print(line)

    w_NAC_log.close()

    #sorted_list = list(set(complete_set))
    #final_result_set = sorted_list.sort()

    final_result_set = complete_set

    for item in final_result_set:
        i += 1
        try:
            line = item['url'] + ',\'' + item['title'] + '\',' + '\'' + item['publishDate'] + '\''
        except KeyError:
            line = item['url'] + ',\'' + item['title'] + '\','

        print(i, line)
        w_NAC_file.write(line + '\n')

    w_NAC_file.close()
    return final_result_set


# -----------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------
def Cr_merchant_ers  (arg_crawler, arg_entities, arg_token, arg_from_date, arg_to_date, arg_var_list):
    """Searches for published articles through various web-crawlers and returns a json containing the list of articles along with its several attributes

    Args:
        arg_crawler   (Crawler)             - Crawler API to be used (GOOGLE/NEWSRIVER/TWITTER)
        arg_entities                        - List of entities such as Merchants to search for
        arg_token     (Search Word)         - Word to be searched.
        arg_from_date (From Date)           - From Date (YYYY-MM-DD)
        arg_to_date   (To Date)             - To Date (YYYY-MM-DD)
        arg_var_list  (Output Variables)    - Variables to return

    Returns:
        A JSON Object containing List of variables from Crawler Run Results such as:
            > publishDate
            > discoverDate
            > title
            > language
            > text
            > structured Text
            > url
            > elements
            > website
            > metadata
            > highlight
            > score
    """
    # Local Variables
    date_format = '%Y-%m-%d'
    start_date = datetime.strptime(arg_from_date, date_format).date()
    end_date = datetime.strptime(arg_to_date, date_format).date()
    result_set = []
    complete_set = []
    final_result_set = []
    http_proxy = "http://proxy.skynet.com:8080"
    https_proxy = "https://proxy.skynet.com:8080"
    ftp_proxy = "ftp://10.10.1.10:3128"

    proxy_dict = {"http": http_proxy, "https": https_proxy}

    # Conforming the Arguments Values
    arg_crawler = arg_crawler.upper()

    w_ERS_file = codecs.open(__ERS_OUTPUT_FILE__, 'w', "utf-8")
    w_ERS_log = codecs.open(__ERS_LOG__, 'w', "utf-8")
    w_MER_file = codecs.open(__MERCHANT_ERS_FILE__, 'w', "utf-8")
    search_token = arg_token

    for entity in arg_entities:

        # Configuring Crawler Specific Variables
        if arg_crawler == 'NEWSRIVER':
            NR_url = 'https://api.newsriver.io/v2/search?query=text%3A@@ARG_TOKEN%20AND%20language%3AEN%20AND%20publishDate%3A%5B@@ARG_FROM_DATE%20TO%20@@ARG_TO_DATE%5D*&sortBy=_score&sortOrder=DESC&limit=100'
            search_token = arg_token
            search_token = search_token.replace('@@ENTITY', entity)
            NR_url = NR_url.replace('@@ARG_TOKEN', search_token)

            auth_key = 'sBBqsGXiYgF0Db5OV5tAwxvg5LqbRRNQF2vDd2yC24SZM38NGEqRrY7ZsS9H4HA1n2pHZrSf1gT2PUujH1YaQA'
        elif arg_crawler == 'NEWSAPI':
            NR_url = ('https://newsapi.org/v2/everything?q=@@ARG_TOKEN&from=@@ARG_FROM_DATE&sortBy=popularity&apiKey=@@ARG_KEY')
            auth_key = '445d8b54f0e2493ba0bb71d9c7692307'
            NR_url = NR_url.replace('@@ARG_TOKEN', arg_token)
            NR_url = NR_url.replace('@@ARG_KEY', arg_token)
        else:
            pass

        # Calling Crawler API based on Argument Crawler
        # print(start_date, type(start_date))
        # print(end_date, type(end_date))

        for single_date in daterange(start_date, end_date):
            x_date = single_date.strftime("%Y-%m-%d")
            article_count = 0
            # Using NewsRiver API
            if arg_crawler == 'NEWSRIVER':
                # Setting up the Crawler URL along with arguments
                crawler_url = NR_url
                crawler_url = crawler_url.replace('@@ARG_FROM_DATE', x_date)
                crawler_url = crawler_url.replace('@@ARG_TO_DATE', x_date)

                # response = requests.get(crawler_url,headers={"Authorization": auth_key}, proxies=proxy_dict)
                if arg_crawler == 'NEWSRIVER':
                    response = requests.get(crawler_url, headers={"Authorization": auth_key})
                elif arg_crawler == 'NEWSAPI':
                    response = requests.get(crawler_url)
                else:
                    pass

                if (response.ok):
                    response_value = response.json()
                    article_count = 0
                    for line in response_value:
                        article_count += 1
                        line['entity'] = entity.replace('%20', ' ').replace('%22', '"')
                        complete_set.append(line)

                line = ''
                i = 0

            elif arg_crawler == 'GOOGLE':
                crawler_url = 'http://news.google.com.br/news?cf=all&ned=us&hl=en&output=rss'
                # just some GNews feed - I'll use a specific search later

                feed = parse(crawler_url)
                for post in feed.entries:
                    # dict_keys(['title', 'title_detail', 'links', 'link', 'id', 'guidislink', 'tags', 'published', 'published_parsed', 'summary', 'summary_detail'])
                    print(post.link)
                    # print (post.keys())
            # Default
            else:
                pass

            line = 'Articles searched for token: ' + entity.replace('%20', ' ').replace('%22', '"') + ' and Publish date: ' + x_date + '. Articles found : ' + str(article_count) + '\n'
            w_ERS_log.write(line)
            print(line)

        # sorted_list = list(set(complete_set))
        # final_result_set = sorted_list.sort()

    final_set = set()
    new_complete_set = []

    # for d in complete_set:
    #     t = tuple(d.items())
    #     if t not in final_set:
    #         final_set.add(t)
    #         new_complete_set.append(d)

    search_ratings = { 'bankruptcy' : 6, 'closure': 5, 'lawsuit': 4, 'merger': 3, 'new location':2, 'year end financials': 1}
    prev_entity = ''
    merchant_score = 0
    article_count = 0
    for item in complete_set:
        i += 1
        article_count += 1

        article_text   = item['text']
        article_title  = item['title']
        article_score  = item['score']
        article_url    = item['url']
        article_entity = item['entity']

        article_text_search_score = 0
        article_search_score = 0

        if article_entity == prev_entity:
            pass
        else:
            if i > 1:
                line = prev_entity + ', ' + str(round((merchant_score/article_count),2))
                w_MER_file.write(line + '\n')

                merchant_score = 0
            article_count = 0
        for word in search_ratings.keys():
            if article_text.upper().find(word.upper()) > 0: article_text_search_score += search_ratings[word]

        if article_text_search_score == 0:
            multiplier = 1
        else:
            multiplier = article_text_search_score

        for word in search_ratings.keys():
            if article_title.upper().find(word.upper()) > 0: article_search_score += ((search_ratings[word] + 1) * multiplier)

        if article_search_score == 0: article_search_score = article_text_search_score

        merchant_score += (article_score * round(article_search_score,2) )

        line = article_entity + ', ' + str(article_score) + ', ' + str(article_search_score) + ', "' + article_title.replace(',', ';') + '", "' + article_url + '"'

        print(i, line)
        w_ERS_file.write(line + '\n')

        prev_entity = article_entity

    w_MER_file.close()
    w_ERS_log.close()
    w_ERS_file.close()
    return final_result_set

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def daterange(arg_from_date, arg_to_date):
    for n in range(int((arg_to_date - arg_from_date).days) + 1):
        yield arg_from_date + timedelta(n)
