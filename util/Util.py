import logging
import os
import random
import re
import string
import time
from multiprocessing import Process
from multiprocessing.managers import BaseManager

import nltk
import requests

from newspaper import Article
from nltk import word_tokenize
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from urlextract import URLExtract
import hashlib
import datefinder
import hashlib
from urllib.parse import quote_plus
from pymongo import MongoClient
def get_image_document(image_url):
    from bson import Binary
    if len(image_url) == 0:
        return None
    try:

        response = requests.get(image_url, timeout=1)
        if response.status_code == 200:
            result = {}
            image_data = Binary(response.content)
            img_name = image_url.split('/')[-1]
            result["image_name"] = img_name
            result["image_data"] = image_data
            result["image_url"] = image_url
            return result

    except Exception:
        logging.error("Exception in downloading image from url : {}".format(image_url))

    return None


def get_collection_db(db, collection_name):
    return db[collection_name]


def epoch_time_to_date(epoch_time):
    return time.strftime('%Y-%m-%d', time.localtime(int(epoch_time)))


def is_epoch_time_in_interval(epoch_time, days):
    epoch_time = int(epoch_time)

    curr_time = time.time()

    #  Check if the time is within the specified interval of days . 86400 sec in a day
    return (days * 86400) + epoch_time >= curr_time


def chunkify(lst, n):
    return [lst[i::n] for i in range(n)]


def equal_chunks(list, chunk_size):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(list), chunk_size):
        yield list[i:i + chunk_size]


def multiprocess_function(num_process, function_ref, args):
    jobs = []
    logging.info("Multiprocessing function %s started..." % function_ref.__name__)

    for idx in range(num_process):
        process = Process(target=function_ref, args=(idx,) + args)
        process.daemon = True
        jobs.append(process)
        process.start()

    for i in range(num_process):
        jobs[i].join()

    logging.info("Multiprocessing function %s completed..." % function_ref.__name__)


class TweetObject:
    def __init__(self, dict):
        vars(self).update(dict)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TweetObject) and self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


class MultiManager(BaseManager):
    pass


def close_all_drivers(drivers):
    for driver in drivers:
        if driver:
            driver.quit()


def init_path(config, driver_path=None):
    if driver_path is None:
        selenium_driver_path = config['Selenium']['driver_path']
    else:
        selenium_driver_path = driver_path
    os.environ["PATH"] += os.pathsep + selenium_driver_path


def get_selenium_drivers(config):
    num_process = int(config["Selenium"]["num_process"])

    drivers = []

    for i in range(num_process):
        drivers.append(get_selenium_driver())

    return drivers



def get_selenium_driver():
    options = Options()
    options.add_argument("--headless")

    # chromeOptions = webdriver.ChromeOptions()
    # firefoxOptions = webdriver.FirefoxOptions()

    # chromeOptions.add_argument("--headless")
    # prefs = {"profile.managed_default_content_settings.images": 2, 'disk-cache-size': 4096}

    # chromeOptions.add_experimental_option('prefs', prefs)
    # firefoxOptions.add_experimental_option('prefs', prefs)
    profile = webdriver.FirefoxProfile()

    profile.set_preference("permissions.default.image", 2)
    profile.set_preference('disk-cache-size', 4096)

    driver = webdriver.Firefox(firefox_options=options, firefox_profile=profile)
    # driver = webdriver.Chrome(chrome_options=chromeOptions)
    driver.implicitly_wait(1)

    return driver


def remove_escape_chars(str):
    return ' '.join(str.split())


def get_urls_in_text(text):
    # urls = re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', text)
    # return urls
    urls = []
    try:
        extractor = URLExtract()
        urls = extractor.find_urls(text)
    except:
        pass

    return urls


class TweetSearchEntry:
    def __init__(self, search_query, news_id, news_source, lang):
        # since date have been already added into search query
        self.search_query = search_query
        self.news_id = news_id
        self.news_source = news_source
        self.lang = lang

    @classmethod
    def from_processed_news_article(cls, news_article):
        return cls(news_article["statement"], news_article["news_id"], news_article["agency"], news_article["lang"])


class FactCheckingCrawler:

    def get_news_from_fact_checking_website(self, is_fake, category):
        pass

    def get_formatted_news_article_details(self, is_fake, fact_checking_source_data):
        pass

    def get_news_source_article(self, is_fake, formatted_news_article_details):
        pass

    def close(self, config):
        pass


def generate_random_id():
    """
    Creates a random number for requesting the full contents of message from the dark scout
    :return: Random request id
    """
    return int(random.random() * 1000000)


def get_text_hash(text):
    return int(hashlib.sha256(text.encode('utf-8')).hexdigest(), 16) % 10 ** 10


def get_text_within_quotes(text):
    quotes_string = re.findall('["“](.*?)["”]', text)
    return quotes_string


def get_stanford_ner_tagger(config):
    corenlp_jars_path = config["StanfordCoreNLP"]["core_nlp_jars_path"]
    port = int(config["StanfordCoreNLP"]["port"])
    start_corenlp_servers(corenlp_jars_path, port)

    ner_tagger = nltk.tag.stanford.CoreNLPNERTagger("http://localhost:{}".format(port))
    return ner_tagger


def get_named_entities(text, ner_tagger):
    tokenized_text = word_tokenize(text)
    classified_text = ner_tagger.tag(tokenized_text)

    named_entities = []

    for word_tuple in classified_text:
        tag = word_tuple[1]
        if "ORGANIZATION" in tag or "LOCATION" in tag or "PERSON" in tag or "COUNTRY" in tag:
            named_entities.append(word_tuple[0])

    return named_entities


def get_negative_sentiment_words(config):
    word_polarity = {}
    pos_cnt = 0
    neg_cnt = 0
    pos_avg = 0
    neg_avg = 0
    lenn = 0

    with open(config["Sentiment"]["sentiwordnet_path"]) as data_file:
        for line in data_file:
            if line[0] == '#':
                continue
            sp = line.split('\t')
            sp[4] = sp[4].split('#')[0]
            if sp[4] not in word_polarity:
                lenn += 1
                if not sp[2] and not sp[3]:
                    print
                    sp[2], sp[3]
                    continue
                if float(sp[2]) != 0:
                    pos_cnt = pos_cnt + 1
                    pos_avg = pos_avg + float(sp[2])
                if float(sp[3]) != 0:
                    neg_cnt = neg_cnt + 1
                    neg_avg = neg_avg + float(sp[3])
                word_polarity[sp[4].lower()] = {'pos': float(sp[2]), 'neg': float(sp[3])}

    pos_avg = pos_avg / pos_cnt
    neg_avg = neg_avg / neg_cnt

    negative_words_set = set()

    for key, value in word_polarity.items():
        if value["neg"] >= neg_avg:
            negative_words_set.add(key)

    return negative_words_set


def start_corenlp_servers(nlp_jars_path, port_no):
    os.chdir(nlp_jars_path)
    start_server_cmd = 'java -mx4g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -annotators "tokenize,' \
                       'ssplit,pos,lemma,parse,sentiment,ner" -port {} -timeout 3000 &'.format(port_no)
    logging.info("Starting the CoreNLP server...")
    os.system(start_server_cmd)
    time.sleep(5)
    logging.info("CoreNLP server started...")


def stop_core_nlp_servers(port_no):
    kill_command = "lsof -n -i:%d | grep LISTEN | awk '{ print $2 }' | xargs kill"
    logging.info(kill_command % port_no)
    os.system(kill_command % port_no)
    logging.info("Stopped the CoreNLP server process...")


def remove_punctuations(text):
    return str(text).translate(str.maketrans('', '', string.punctuation))

def parse_artilce(url, article):
    visible_text = article.text
    top_image = article.top_image
    images = article.images
    keywords = article.keywords
    authors = article.authors
    canonical_link = article.canonical_link
    title = article.title
    meta_data = article.meta_data
    movies = article.movies
    publish_date = article.publish_date
    source = article.source_url
    summary = article.summary
    html = article.html
    lang = article.meta_lang

    result_json = {'url': url, 'text': visible_text, 'images': list(images), 'top_img': top_image,
                   'keywords': keywords,'html':html,
                   'authors': authors, 'canonical_link': canonical_link, 'title': title, 'meta_data': meta_data,
                   'movies': movies, 'publish_date': publish_date, 'source': source, 'summary': summary, "lang":lang
                   }
    return result_json

def crawl_link_article(url, inner_html=None):
    flag = True
    result_json = None
    if inner_html is None:
        try:
            if 'http' not in url:
                if url[0] == '/':
                    url = url[1:]
                try:
                    article = Article('http://' + url)
                    article.download()
                    time.sleep(2)
                    article.parse()
                    flag = True
                except:
                    logging.exception("Exception in getting figure from url {}".format(url))
                    flag = False
                    pass
                if flag == False:
                    try:
                        article = Article('https://' + url)
                        article.download()
                        time.sleep(2)
                        article.parse()
                        flag = True
                    except:
                        logging.exception("Exception in getting figure from url {}".format(url))
                        flag = False
                        pass
                if flag == False:
                    return None
            else:
                try:
                    article = Article(url)
                    article.download()
                    time.sleep(2)
                    article.parse()
                except:
                    logging.exception("Exception in getting figure from url {}".format(url))
                    return None

            if not article.is_parsed:
                return None

            result_json = parse_artilce(article=article, url=url)
            
        except:
            logging.exception("Exception in fetching article form URL : {}".format(url))
    else:
        article = Article(url)
        article.download(input_html=inner_html)
        article.parse()
        result_json = parse_artilce(article=article, url=url)



    if result_json is None:
        try:
            pesuldo_url = "http://newspaper-demo.herokuapp.com/articles/show?url_to_clean={}"
            url_payload = quote_plus(url)
            pesuldo_url = pesuldo_url.format(url_payload)
            response = requests.get(pesuldo_url)
            bs4 = BeautifulSoup(response.text, "html.parser")
            elements = bs4.findAll("tr")
            title = elements[0].findAll("td")[1].get_text()
            authors = elements[1].findAll("td")[1].get_text()
            text = elements[2].findAll("td")[1].get_text()
            top_image = elements[3].findAll("td")[1].get_text()
            movies = elements[4].findAll("td")[1].get_text()
            keywords = elements[5].findAll("td")[1].get_text()
            summary = elements[6].findAll("td")[1].get_text()
            try:
                html = requests.get(url, timeout=10).text
            except:
                html = None
                logging.exception("Exception in Get the Source Content {}".format(url))

            result_json = {'url': url, 'text': text, 'images': None, 'top_img': top_image,
                           'keywords': keywords, 'html': html,
                           'authors': authors, 'canonical_link': None, 'title': title,
                           'meta_data': None,
                           'movies': movies,
                           'publish_date': None,
                           'source': None,
                           'summary': summary}
        except:
            logging.exception("Exception in fetching Article from: {} by crawling demo".format(url))



    return result_json


def crawl_link_politifact(url):
    flag = True
    result_json = None

    try:
        if 'http' not in url:
            if url[0] == '/':
                url = url[1:]
            try:
                article = Article('http://' + url)
                article.download()
                time.sleep(2)
                article.parse()
                flag = True
            except:
                logging.exception("Exception in getting figure from url {}".format(url))
                flag = False
                pass
            if flag == False:
                try:
                    article = Article('https://' + url)
                    article.download()
                    time.sleep(2)
                    article.parse()
                    flag = True
                except:
                    logging.exception("Exception in getting figure from url {}".format(url))
                    flag = False
                    pass
            if flag == False:
                return None
        else:
            try:
                article = Article(url)
                article.download()
                time.sleep(2)
                article.parse()
            except:
                logging.exception("Exception in getting figure from url {}".format(url))
                return None

        if not article.is_parsed:
            return None


        statement = article.title
        bs = BeautifulSoup(article.html, "lxml")
        time_stamp = next(datefinder.find_dates(str(bs.find("section",{"class":"o-stage"}).find("div", {"class" : "m-statement__desc"}).contents[0])))


        source_section = bs.find("section",{'id':'sources'})
        a_list = source_section.find_all('a',href=True)
        resource_urls_list = []
        resource_statement_list = []
        for a in a_list:
            try:
                resource_urls_list.append(a.attrs['href'])
                resource_artile = Article(resource_urls_list[-1])
                resource_artile.download()
                resource_artile.parse()
                resource_statement_list.append(resource_artile.title)
            except:
                logging.exception("Exception in getting the resource figure url {}".format(a.attrs['href']))

        # ATTENTION: Yichuan Cannot find the ID for the crawled news, so he used the hash256 for the statement as the id
        # ATTENTION: Question about the statement
        result_json = {
            'id': hashlib.sha256(statement.encode('utf-8')).hexdigest(),
            'statement': statement,
            'created_at': time_stamp,
            'resource_url': resource_urls_list,
            'resource_statement': resource_statement_list
        }
    except:
        logging.exception("Exception in fetching article form URL : {}".format(url))

    return result_json


def get_resource_state(fake_news, fake_source):
    try:
        id = fake_news['id']
        # id, sources,
        statement = ''
        url = ''

        ## Using the first source that contains href as the fake news source if source is not removed
        ## This is not always true
        for url, statement in zip(fake_news['resource_url'], fake_news['resource_statement']):
            if url is not None and len(statement.split()) > 3:
                # TODO: Check if it is proper
                statement = str(statement).translate(str.maketrans('', '', string.punctuation))

                # statement_new = statement.translate(str.maketrans('', '', string.punctuation))  # move punctuations
                break

        # TODO: Check if the condition is proper
        if statement == '' or len(statement.split(' ')) <= 3:
            logging.warning('statement is null..')
            logging.warning("\n Null for the html source : %s \n\n" % url)
            return None

        statement = remove_escape_chars(statement)
        logging.info('politifact-' + str(id) + '\t' + statement + '\t' + url)
        # get the url of the reference element
        politi_fact_fake_news = {"id": fake_source + str(id), "statement": statement, "url": url,
                                 "news_source": fake_source}

        return politi_fact_fake_news

    except Exception as e:
        print(e)
        logging.exception("Exception in get_fromatted_news_details() for id  {}".format(fake_news['id']))
        return None


# def get_database_connection(config):
#     host = config['MongoDB']['host']
#     port = int(config['MongoDB']['port'])
#     db_name = config['MongoDB']['database_name']
#     user = config['MongoDB']['user']
#     pwd = config['MongoDB']['pwd']
#
#     # mongodb://yichuan1:lyc960915@10.218.105.226/covid_19_new
#     client = MongoClient(f"mongodb://{user}:{pwd}@{host}/{db_name}")
#     # if debug == 1:
#     #     client.drop_database(db_name)
#     db = client[db_name]
#     return db



def get_database_connection(config):
    host = config['MongoDB']['host']
    port = int(config['MongoDB']['port'])
    db_name = config['MongoDB']['database_name']

    client = MongoClient(host, port)
    db = client[db_name]
    return db
