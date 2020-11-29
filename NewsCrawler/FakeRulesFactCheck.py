import json
import logging
import time

from bs4 import BeautifulSoup as bs
import re
from util.Util import crawl_link_article, init_path
from util.Util import get_selenium_driver, get_text_within_quotes, get_named_entities, remove_punctuations, \
    chunkify, multiprocess_function, get_text_hash, get_stanford_ner_tagger, get_negative_sentiment_words
from util import Constants

import configparser
import os
from nltk import word_tokenize
import logging
import pandas as pd
from itertools import chain

import archiveis
from NewsCrawler.ArchiveCrawler import page_in_archive, page_in_perma
from util.Util import get_selenium_driver, chunkify
from util import Constants
import time


import re
import pickle
from tqdm import tqdm



def load_configuration(config_file):
    """Gets the configuration file and returns the dictionary of configuration"""
    filename = config_file
    config = configparser.ConfigParser()
    config.read(filename)

    return config


def get_search_query_from_title(title, ner_tagger, remove_word_list):
    search_query = None
    if title.count('“') > 0 or title.count('"') > 0:
        # String contains quotes - use named entities + text within quotes for search query
        quotes_text = get_text_within_quotes(title)
        # ATTENTION: Name entity
        named_entities = get_named_entities(title, ner_tagger)

        search_terms = []

        for term in quotes_text:
            search_terms.append((term, title.find(term)))

        for term in named_entities:
            search_terms.append((term, title.find(term)))

        sorted_search_terms = sorted(search_terms, key=lambda tup: tup[1])

        search_terms = []

        for term in sorted_search_terms:
            search_terms.append(term[0])

        search_query = " ".join(search_terms)
    else:
        # remove negative sentiment words to from the search query from title
        search_query = text_remove_words_form_search_query(title, remove_word_list)

    return search_query




def text_remove_words_form_search_query(text, remove_word_list):
    words = word_tokenize(text)
    for word in words:
        if word.lower() in remove_word_list:
            words.remove(word)

    return " ".join(words)


def unique(sequence):
    seen = set()
    return [x for x in sequence if not (x in seen or seen.add(x))]


from newspaper import Article
from lxml import html

web_page = None
archive_website = set(["perma", 'archive'])


def rule_based_crawl(fact_site_url, driver, domain, claim=None, url_require=None):
    """
    return url_list, domain, content, lang, claim
    """
    url_list = []
    content = "NONE"

    try:

        article = Article(fact_site_url)
        article.download()
        article.parse()
        lang = article.meta_lang
        claim = article.title
        # content = article.title
    except:
        lang = "Unknown"

    try:
        driver.get(fact_site_url)
        # extract twitter url to expand the dataset

        if ".afp." in fact_site_url:
            domain = "afp"
            # take all url
            paraphs = driver.find_elements_by_tag_name("p")
            for paraph in paraphs:
                try:
                    urls = paraph.find_elements_by_tag_name("a")
                    for url in urls:
                        url_list.append((url.text.lower(), url.get_attribute("href")))
                except:
                    continue
            # reorder the link
            archive_urls = [i for i in url_list if any([kw in i[1] for kw in archive_website])]
            non_archive_urls = [i for i in url_list if all([kw not in i[1] for kw in archive_website])]
            url_list = archive_urls + non_archive_urls
            #

        elif "pesacheck" in fact_site_url:
            urls = driver.find_elements_by_xpath("//p/a")
            urls = [url.get_attribute("href") for url in urls]
            url_list = [("archive", i) for i in urls if "archive" in i] + [('first', urls[0])]
        elif 'checkyourfact' in fact_site_url:
            domain = "checkyourfact"
            # the first paragraph contains the post
            first_paraph = driver.find_element_by_tag_name("p")
            urls = first_paraph.find_elements_by_tag_name("a")
            for url in urls:
                url_list.append((url.text, url.get_attribute("href")))
        elif "factcheck.org" in fact_site_url:
            domain = "factcheck"
            # TODO: check this
            # lang='en'
            paraphs = driver.find_elements_by_xpath("//h5[text()='Sources']/following::p")
            for p in paraphs:
                try:
                    url = p.find_element_by_tag_name("a").get_attribute("href")
                    text = p.text
                    url_list.append((text, url))
                except:
                    continue
        elif "healthfeedback" in fact_site_url:
            domain = "healthfeedback"
            # lang='en'
            url = driver.find_element_by_xpath("//a[@title='See the claim in context']") \
                .get_attribute("href")

            url_list.append(("source", url))
        elif "reuters.com" in fact_site_url:
            domain = "reuters"

            # take the here url
            here_urls = driver.find_elements_by_link_text("here")
            for url in here_urls:
                text = url.text.lower()
                url = url.get_attribute("href")
                url_list.append((text, url))
        elif "usatoday.com" in fact_site_url:
            domain = "usatoday"
            # lang='en'
            # take the top urls
            paraphs = driver.find_elements_by_xpath("//p[@class='gnt_ar_b_p']")
            for p in paraphs:
                try:
                    urls = p.find_elements_by_tag_name("a")
                    for url in urls:
                        url_list.append((url.text, url.get_attribute('href')))
                except:
                    continue

            try:
                source = driver.find_element_by_xpath("//ul[@class='gnt_ar_b_ul']")
                urls = source.find_elements_by_xpath("//a[@class='gnt_ar_b_a']]")

                for url in urls:
                    try:
                        text = "ExternalSource"
                        url = url.get_attribute("href")
                        url_list.append((text, url))
                    except:
                        continue
            except:
                pass
        elif "leadstories" in fact_site_url:
            domain = "leadstories"
            # lang='en'
            try:
                url = driver.find_element_by_xpath("//p[text()[contains(., 'archived')]]")
                url = url.find_element_by_partial_link_text("here")
                url = url.get_attribute("href")
                text = "source"

                url_list.append((text, url))
            except:
                pass

        elif "politifact" in fact_site_url:
            domain = "politifact"
            # lang='en'
            sources = driver.find_element_by_id("sources")
            logging.info("PolitFactURL {}".format(fact_site_url))
            urls = sources.find_elements_by_tag_name("p")
            for url in urls:
                text = url.text
                try:
                    url = url.find_element_by_tag_name('a').get_attribute("href")
                except:
                    continue
                url_list.append((text, url))
        elif "snopes" in fact_site_url:
            domain = "snopes"
            pass
        elif "factcrescendo" in fact_site_url:
            domain = "factcrescendo"
            try:
                raw_sources = driver.find_element_by_xpath("//a[text()[contains(., 'आर्काइव लिंक')]]/ancestor::p/a")
                raw_sources = raw_sources.get_attribute("href")
                url_list.append(("source", raw_sources))
            except:
                th = 1

            try:
                urls = driver.find_element_by_xpath("//div[@class='entry-content']/p/a")
                urls = [i.get_attribute("href") for i in urls]
                archive_urls = [i for i in urls if "archive" in i]
                first_urls = [i for i in urls if "archive" not in i][:5]
                url_list.extend([("archive", i) for i in archive_urls])
                url_list.extend([("first_url", i) for i in first_urls])
            except:
                archive_source = "NONE"

            if len(url_list) == 0 or url_list[0][0] == "NONE":
                try:
                    urlsp = driver.find_elements_by_xpath("//p")
                    urls = [i.find_elements_by_xpath(".//a") for i in urlsp]
                    urls = chain.from_iterable([i for i in urls if len(i) == 2])
                    url_list.extend([("url", i.get_attribute('href')) for i in urls])
                except:
                    archive_source = "NONE"


        elif "factly" in fact_site_url:
            domain = "factly"
            sources = driver.find_elements_by_xpath(
                "//strong[text()[contains(., 'సోర్సెస్:')]]/ancestor::p/a[@rel='noreferrer noopener']")
            if len(sources) == 0:
                sources = driver.find_elements_by_xpath(
                    "//strong[text()[contains(., 'Sources:')]]/ancestor::p/a[@rel='noreferrer noopener']")
            urls = [i.get_attribute("href") for i in sources]
            url_list.extend([("archive", i) if "archive" in i else ("source", i) for i in urls])

        elif "maldita" in fact_site_url:
            domain = "maldita"
            urls = driver.find_elements_by_xpath("//a")
            urls = [url.get_attribute("href") for url in urls]
            archive_urls = [url for url in urls if "archive" in url]
            # check the facebook urls
            if url_require:
                url_require_list = [i for i in urls if url_require in i]
                url_list.extend([(url_require, i) for i in url_require_list])
            url_list.extend([("archive", i) for i in archive_urls])


        elif "voxukraine" in fact_site_url:
            # take the first url
            url = driver.find_element_by_xpath("//div[@class='single-post-content col-xs-9']//a")
            url = url.get_attribute("href")
            url_list.append(("first url", url))

        elif "piaui" in fact_site_url:
            # the screenshot of the text
            try:
                screenshot_url = driver.find_element_by_xpath("//i/span[@style='font-weight: 400;']/a")
                screenshot_url = screenshot_url.get_attribute("href")
            except:
                screenshot_url = "NONE"

            # contents = driver.find_elements_by_xpath("//div[@class='etiqueta etiqueta-7']/preceding::p//b")
            contents = driver.find_elements_by_xpath("//p//strong")
            # contents = driver.find_elements_by_xpath("//em/ancestor::p/b")
            if len(contents) < 1:
                # contents = driver.find_elements_by_xpath("//i/span[@style='font-weight: 400']/ancestor::div/b")
                contents = driver.find_elements_by_xpath("//p//b")
                if len(contents) < 1:
                    contents = driver.find_elements_by_xpath("//i/span[@style='font-weight: 400']/ancestor::p/b")
            if len(contents) > 1:
                contents = contents[1:]
            content = " ".join([i.text for i in contents]).replace("Lupa", "")

            url_list.append(("screen_shot", screenshot_url))

        elif "poligrafo" in fact_site_url:
            element = driver.find_elements_by_xpath("//div[@class='content']/figure/preceding::p")
            content = " ".join([i.text for i in element])

        elif "aosfatos" in fact_site_url:
            content = driver.find_elements_by_xpath("//blockquote/p[@dir='ltr']")
            content = " ".join([i.text for i in content])
        elif "newtral.es" in fact_site_url:
            domain = "newtral.es"
            # key word, URL
            # only keep the content
            content = driver.find_element_by_xpath(
                "//div[@class='c-card__verification__main c-card__fake__main']//mark[@class='c-card__verification__quote u-highlight']").text
            url_list.append(("NONE", "NONE"))

        elif "boomlive" in fact_site_url:
            time.sleep(3)
            domain = "boomlive"
            urls = driver.find_elements_by_xpath("//p//a")
            urls = [i.get_attribute("href") for i in urls]
            urls = [i for i in urls if "archive" in i]
            if len(urls) > 0:
                url_list.extend([("acrhive", i) for i in urls])
            else:
                url_list.append(("NONE", "NONE"))


        elif "misbar" in fact_site_url:
            domain = "misbar"
            url_list.append(("NONE", "NONE"))
        elif "estadao" in fact_site_url:
            domain = "estadao"
            url_list.append(("NONE", "NONE"))

        elif "vishvasnews" in fact_site_url:
            domain = "vishvasnews"
            urls = driver.find_elements_by_xpath("//p//a")
            urls = [i.get_attribute("href") for i in urls]
            urls = [i for i in urls if "archive" in i]
            if len(urls) > 0:
                url_list.extend([("acrhive", i) for i in urls])
            else:
                url_list.append(("NONE", "NONE"))

        elif "indiatoday" in fact_site_url:
            domain = "indiatody"
            urls = driver.find_elements_by_xpath("//div[@class='description ']/p//a")
            urls = [i.get_attribute("href") for i in urls]
            urls = [i for i in urls if "archive" in i]
            if len(urls) > 0:
                url_list.extend([("acrhive", i) for i in urls])
            else:
                url_list.append(("NONE", "NONE"))

        elif "animalpolitico" in fact_site_url:
            domain = "animalpolitico"
            url_list.append(("NONE", "NONE"))

        elif "colombiacheck" in fact_site_url:
            domain = "colombiacheck"
            urls = driver.find_elements_by_xpath("//p//a")
            urls = [i.get_attribute("href") for i in urls]
            urls = [i for i in urls if "archive" in i]
            if len(urls) > 0:
                url_list.extend([("acrhive", i) for i in urls])
            else:
                url_list.append(("NONE", "NONE"))
        elif "facta" in fact_site_url:
            domain = "facta"
            urls = driver.find_elements_by_xpath("//div[@class='edgtf-post-text-main']/p//a")
            urls = [i.get_attribute("href") for i in urls]
            archive_urls = [("archive", i) for i in urls if "archive" in i]
            other_urls = [("all_url", i) for i in urls if "archive" not in i][:4]
            url_list.extend(archive_urls)
            url_list.extend(other_urls)
        elif "tfc-taiwan" in fact_site_url:
            domain = "tfc-taiwan"
            url_list.append(("NONE", "NONE"))
        elif "correctiv" in fact_site_url:
            domain = "correctiv"
            urls = driver.find_elements_by_xpath("//div[@class='detail__content']/p/a")
            urls = [i.get_attribute("href") for i in urls]
            urls = [i for i in urls if "archive" in i]
            url_list.extend([("acrhive", i) for i in urls])
        elif "lemonde" in fact_site_url:
            domain = "lemonde"
            try:
                urls = driver.find_elements_by_xpath("//strong[text()[contains(.,'Ce que')]]/parent::p/following::p/a")
            except:
                urls = driver.find_elements_by_xpath("//article/p/a")
            urls = [i.get_attribute("href") for i in urls]
            urls = [i for i in urls if "lemonde" not in i]
            url_list.extend([("ref_url", i) for i in urls])

        elif "publico" in fact_site_url:
            domain = "publico"
            url_list.append(("NONE", "NONE"))

        elif "teyit" in fact_site_url:
            domain = "teyit"
            try:
                th = driver.find_element_by_xpath("//button[@id='read-more']")
                th.click()
                urls = driver.find_elements_by_xpath("//p/a")
            except:
                urls = driver.find_elements_by_xpath("//span[@class='cb-itemprop']/p/a")
            urls = [i.get_attribute("href") for i in urls]
            urls = [i for i in urls if "archive" in i]
            if len(urls) > 0:
                url_list.extend([("acrhive", i) for i in urls])
            else:
                url_list.append(("NONE", "NONE"))
        elif "aosfatos" in fact_site_url:
            domain = "aosfatos"
            url_list.append(("NONE", "NONE"))
        elif "factcheck.kz" in fact_site_url:
            domain = "factcheck.kz"
            url_list.append(("NONE", "NONE"))
        elif "efectococuyo" in fact_site_url:
            domain = "efectococuyo"
            url_list.append(("NONE", "NONE"))
        elif "spondeomedia" in fact_site_url:
            domain = "spondeomedia"
            url_list.append(("NONE", "NONE"))
        elif "observador" in fact_site_url:
            domain = "observador"
            content = driver.find_element_by_xpath("//p[@class='factcheck-text']").text
            url_list.append(("NONE", "NONE"))

        elif "fullfact" in fact_site_url:
            domain = "fullfact"
            url_list.append(("NONE", "NONE"))

        elif "chequeado" in fact_site_url:
            domain = "chequeado"
            urls = driver.find_elements_by_xpath("//span[@class='cb-itemprop']/p/a")
            urls = [i.get_attribute("href") for i in urls]
            urls = [i for i in urls if "archive" in i]
            if len(urls) > 0:
                url_list.extend([("acrhive", i) for i in urls])
            else:
                url_list.append(("NONE", "NONE"))
        elif "verificado" in fact_site_url:
            domain = "fullfact"
            url_list.append(("NONE", "NONE"))
        elif "newsmeter.in" in fact_site_url:
            domain = "newsmeter.in"
            urls = driver.find_elements_by_xpath("//article//p/a")
            urls = [i.get_attribute("href") for i in urls][:5]
            url_list.extend([("all_url", i) for i in urls])
            # get the embed URL:
            try:
                twitter_id = driver.find_element_by_xpath(
                    "//div[@class='twitter-tweet twitter-tweet-rendered']").get_attribute("figure-tweet-id")
                if len(str(twitter_id)) > 0:
                    url_list = [("twitter", "https://twitter.com/i/web/status/{}".format(twitter_id))]
            except:
                th = 1

        elif "france24" in fact_site_url:
            domain = "france24"
            url_list.append(("NONE", "NONE"))

        elif "boliviaverifica" in fact_site_url:
            domain = "boliviaverifica"
            urls = driver.find_elements_by_xpath("//div[@class='entry-content clearfix']//p/a")
            urls = [i.get_attribute("href") for i in urls][:5]
            url_list.extend([("first_url", i) for i in urls])

        elif "digiteye.in" in fact_site_url:
            domain = "digiteye.in"
            url_list.append(("NONE", "NONE"))

        elif "aap" in fact_site_url:
            domain = "aap"
            urls = driver.find_elements_by_xpath(
                "//div[@class='c-article__content e-content c-article__content--factcheck']/p/a")
            urls = [i.get_attribute("href") for i in urls][:5]
            url_list.extend([("first_url", i) for i in urls])

        elif "agenciaocote" in fact_site_url:
            domain = "agenciaocote"
            content = driver.find_element_by_xpath("//article/div[@class='entradilla']/p").text
            url_list.append(("NONE", "NONE"))

        elif "open.online" in fact_site_url:
            domain = "open.online"
            urls = driver.find_elements_by_xpath("//div[@class='news__content article-body adv-single-target']/p/a")
            urls = [i.get_attribute("href") for i in urls][:5]
            url_list.extend([("first_url", i) for i in urls])

        elif "francetvinfo" in fact_site_url:
            domain = "francetvinfo"
            url_list.append(("NONE", "NONE"))

        elif "demagog" in fact_site_url:
            domain = "demagog"
            urls = driver.find_elements_by_xpath("//div[@class='mb-5 pb-3 count-text']//p//a")
            urls = [i.get_attribute("href") for i in urls if domain not in i.get_attribute("href")][:5]
            url_list.extend([("first_url", i) for i in urls])

        elif "ellinikahoaxes" in fact_site_url:
            domain = "ellinikahoaxes"
            urls = driver.find_elements_by_xpath("//div[contains(@id, 'post-')]/p/a")
            urls = [i.get_attribute("href") for i in urls]
            archive_urls = [i for i in urls if "archive" in i]
            other_urls = [i for i in urls if "archive" not in i][:5]
            url_list.extend([("archive", i) for i in archive_urls])
            url_list.extend([("first_url", i) for i in other_urls])

        elif "lasillavacia" in fact_site_url:
            domain = "lasillavacia"
            urls = driver.find_elements_by_xpath("//div[@class='field-items']//p/a")
            urls = [i.get_attribute("href") for i in urls][:5]
            url_list.extend([("first_url", i) for i in urls])

        elif "elsurti" in fact_site_url:
            domain = "elsurti"
            url_list.append(("NONE", "NONE"))

        elif "delfi.lt" in fact_site_url:
            domain = "delfi.lt"
            content = driver.find_element_by_xpath("//div[@class='article-featured-text']").text
            url_list.append(("NONE", "NONE"))

        elif "mcot.net" in fact_site_url:
            domain = "mcot.net"
            url_list.append(("NONE", "NONE"))

        elif "sciencepresse" in fact_site_url:
            domain = "sciencepresse"
            url_list.append(("NONE", "NONE"))

        elif "buzzfeed.com/jp" in fact_site_url:
            domain = "buzzfeed_jp"
            try:
                content = driver.find_element_by_xpath("//blockquote/p").text
            except:
                content = "NONE"

            url_list.append(("NONE", "NONE"))

        elif "factnameh" in fact_site_url:
            domain = "factnameh"
            try:
                content = driver.find_element_by_xpath("//blockquote/p").text
            except:
                content = "NONE"

            url_list.append(("NONE", "NONE"))

        elif "raskrinkavanje" in fact_site_url:
            domain = "raskrinkavanje"
            content = " ".join([i.text for i in driver.find_elements_by_xpath("//blockquote")])
            url_list.append(("NONE", "NONE"))

        elif "annie-lab" in fact_site_url:
            domain = "annie-lab"
            content = driver.find_element_by_xpath("//blockquote").text
            urls = driver.find_elements_by_xpath("//section/div/div/p/a")
            urls = [i.get_attribute("href") for i in urls][:5]
            url_list.extend([("first_url", i) for i in urls])

        elif "15min.lt" in fact_site_url:
            domain = "15min.lt"
            url_list.append(("NONE", "NONE"))

        elif "nieuwscheckers" in fact_site_url:
            domain = "nieuwscheckers"
            url_list.append(("NONE", "NONE"))

        elif "tjekdet" in fact_site_url:
            domain = "tjekdet"
            urls = driver.find_elements_by_xpath("//div[@class='article-copy article-copy-long-form']/div/p/a")
            urls = [i.get_attribute("href") for i in urls][:5]
            url_list.extend([("first_url", i) for i in urls])

        elif "kallkritikbyran" in fact_site_url:
            domain = "kallkritikbyran"
            urls = driver.find_elements_by_xpath("//div[@class='entry-single']/p//a")
            urls = [i.get_attribute("href") for i in urls][:5]
            url_list.extend([("first_url", i) for i in urls])

        elif "PesaCheck" in fact_site_url:
            domain = "PesaCheck"
            urls = driver.find_elements_by_xpath("//article//p/a")
            urls = [i.get_attribute("href") for i in urls]
            archive_urls = [url for url in urls if "archive" in url]
            other_urls = [i for i in urls if "archive" not in i]
            url_list.extend([("archive", i) for i in archive_urls])
            url_list.extend([("first_url", i) for i in other_urls][:5])

        elif "liberation" in fact_site_url:
            domain = "liberation"
            try:
                content = driver.find_element_by_xpath("//blockquote").text
            except:
                content = "NONE"
            urls = driver.find_elements_by_xpath("//div[@class='article-body']/p/a")
            urls = [i.get_attribute("href") for i in urls]
            archive_urls = [url for url in urls if "archive" in url]
            other_urls = [i for i in urls if "archive" not in i and domain not in i]
            url_list.extend([("archive", i) for i in archive_urls])
            url_list.extend([("first_url", i) for i in other_urls][:5])

        elif "africacheck" in fact_site_url:
            domain = "africacheck"
            urls = driver.find_elements_by_xpath("//p/a")
            urls = [i.get_attribute("href") for i in urls]
            archive_urls = [url for url in urls if "archive" in url]
            other_urls = [i for i in urls if "archive" not in i and domain not in i]
            url_list.extend([("archive", i) for i in archive_urls])
            url_list.extend([("first_url", i) for i in other_urls][:5])

        elif "rappler" in fact_site_url:
            domain = "rappler"
            url_list.append(("NONE", "NONE"))

        elif "thejournal.ie" in fact_site_url:
            domain = "thejournal.ie"
            urls = driver.find_elements_by_xpath("//p/a")
            urls = [i.get_attribute("href") for i in urls]
            archive_urls = [url for url in urls if "archive" in url]
            other_urls = [i for i in urls if "archive" not in i and domain not in i]
            url_list.extend([("archive", i) for i in archive_urls])
            url_list.extend([("first_url", i) for i in other_urls][:5])

        elif "dubawa" in fact_site_url:
            domain = "dubawa"
            try:
                url = driver.find_element_by_xpath(
                    "//p[@class='has-background has-very-light-gray-background-color']/a") \
                    .get_attribute("href")
                url_list.append(("source", url))
            except:
                urls = driver.find_elements_by_xpath("//p/a")
                urls = [i.get_attribute("href") for i in urls]
                url_list.extend([("first_url", i) for i in urls][:5])

        elif "cekfakta" in fact_site_url:
            domain = "/cekfakta"
            try:
                content = driver.find_element_by_xpath("//p/em").text
            except:
                content = "NONE"

            urls = driver.find_elements_by_xpath("//p/a")
            urls = [i.get_attribute("href") for i in urls]
            archive_urls = [i for i in urls if "archive" in i]

            if len(archive_urls) > 0:
                url_list.extend([("archive", i) for i in archive_urls])
            else:
                url_list.append(("NONE", "NONE"))
            # url_list.extend([("first_url", i) for i in other_urls][:5])

        elif "pagellapolitica" in fact_site_url:
            domain = "pagellapolitica"
            urls = driver.find_elements_by_xpath("//div[@class='mt-content']"
                                                 "//div[@class='col-lg-9 mb-9 mb-lg-0']"
                                                 "//div[@class='font-size-15 px-2']/p/a")
            urls = [i.get_attribute("href") for i in urls]
            url_list.extend([("first_url", i) for i in urls][:5])

        elif "radio-canada" in fact_site_url:
            domain = "radio-canada"
            content = driver.find_element_by_xpath("//span[@title='Français']").text
            url_list.append(("NONE", "NONE"))

        elif "verafiles" in fact_site_url:
            domain = "verafiles"
            url_list.append(("NONE", "NONE"))

        elif "newschecker" in fact_site_url:
            domain = "newschecker"
            try:
                content = driver.find_element_by_xpath("//blockquote/p").text
            except:
                content = "NONE"
            url_list.append(("NONE", "NONE"))

        elif "ecuadorchequea" in fact_site_url:
            domain = "ecuadorchequea"
            # TODO this should check all
            # http://www.ecuadorchequea.com/las-mentiras-sobre-el-covid-19-en-ecuador/
            url_list.append(("NONE", "NONE"))

        elif "faktograf" in fact_site_url:
            domain = "faktograf"
            urls = driver.find_elements_by_xpath("//div[@class='entry-content']/p/a")
            urls = [i.get_attribute("href") for i in urls]
            archive_urls = [url for url in urls if "archive" in url]
            other_urls = [i for i in urls if "archive" not in i and domain not in i]
            url_list.extend([("archive", i) for i in archive_urls])
            url_list.extend([("first_url", i) for i in other_urls][:5])

        elif "thequint" in fact_site_url:
            domain = "thequint"
            url_list.append(("NONE", "NONE"))


        elif "efe.com" in fact_site_url:
            domain = "efe.com"
            # div_texto
            urls = driver.find_elements_by_xpath("//div[@class='div_texto']/p/a")
            urls = [i.get_attribute("href") for i in urls]
            archive_urls = [url for url in urls if "archive" in url]
            other_urls = [i for i in urls if "archive" not in i and domain not in i]
            url_list.extend([("archive", i) for i in archive_urls])
            url_list.extend([("first_url", i) for i in other_urls][:5])

        elif "crithink" in fact_site_url:
            domain = "crithink"
            source_url = driver.find_element_by_xpath("//p[text()[contains(.,'Линк до оригиналниот напис:')]]/strong/a") \
                .get_attribute("href")
            url_list.append(("source", source_url))

        elif "nacion" in fact_site_url:
            domain = "nacion"
            url_list.append(("NONE", "NONE"))
    except:
        pass

    if len(url_list) == 0:
        url_list = [('NONE', 'NONE')]
    url_list = [i[1] for i in url_list]
    return url_list, domain, content, lang, claim


def extract_twitter_url(fact_site_url, driver):
    driver.get(fact_site_url)
    twitter_urls = driver.find_elements_by_xpath("//a[contains(@href, 'twitter.com')]")
    twitter_urls_list = []
    for i in twitter_urls:
        try:
            twitter_urls_list.append(i.get_attribute("href"))
        except:
            continue
    # twitter_urls = [i.get_attribute("href") for i in twitter_urls]
    twitter_urls = [i for i in twitter_urls_list if 'status' in i]
    return twitter_urls



def twitter_in_fact_check(news_collection, news_tweet_correlation, driver):
    tweet_ids = []
    list_kw = ['twitter', 'media']
    refresh_t = 1
    for i in tqdm(news_collection.find({"ref_source_url": "NONE"})):
        try:
            originated = str(i['orginated'])
        except:
            continue
        if any([kw for kw in list_kw if kw in originated.lower()]):

            try:
                twitter_urls = extract_twitter_url(fact_site_url=i['fact_url'], driver=driver)
            except:
                logging.info("Exception in URL: {}".format(i['fact_url']))
                print("Exception in URL: {}".format(i['fact_url']))
                continue

            tweet_list = []
            for turl in twitter_urls:
                try:
                    tweet_list.append(re.findall('\d+', turl.split("/")[-1])[0])
                except:
                    continue
            if len(tweet_list) == 0:
                continue
            # only take the first element

            news_collection.find_and_modify({"news_id": i['news_id']}, {
                'ref_source_url': "https://twitter.com/i/web/status/{}".format(tweet_list[0])})
            news_tweet = {"news_id": i['news_id'], "tweet_list": [int(tweet) for tweet in tweet_list]}
            news_tweet_correlation.find_and_modify({'news_id': news_tweet['news_id']}, {"$set": news_tweet},
                                                   upsert=True)
            tweet_ids += tweet_list
        if refresh_t % 100 == 0:
            driver.close()
            driver = get_selenium_driver()
    return tweet_ids


def twitter_in_source(news_collection):
    id_tweets = {}
    tweet_ids = []
    for i in news_collection.find({'ref_source_url': {'$regex': 'twitter'}}, {"ref_source_url": 1, 'news_id': 1}):
        try:
            tweet_id = re.findall('status/\d+', i['url'])[0]
        except:
            print(i['url'])
            continue
        tweet_id = tweet_id.replace("status/", "")
        tweet_ids.append(tweet_id)
        id_tweets[i['id']] = [tweet_id]
    return id_tweets, tweet_ids



def get_news_source_article(url, driver):
    news_source_article = {}
    if url != "NONE":

        if ("archive" in url or "perma.cc" in url) \
                and "web.archive.org" not in url:
            if "archive" in url:
                source_article = page_in_archive(driver=driver, url=url)
            else:
                source_article = page_in_perma(driver=None, url=url)
            source_article['ref_archive_url'] = url
        else:
            source_article = crawl_link_article(url)
            if "web.archive.org" not in url:
                source_article['url'] = source_article['url'].split("://")[-1]
                source_article['ref_archive_url'] = url
        if source_article:
            news_source_article = source_article

    if len(news_source_article) == 0:
        news_source_article = None
    return news_source_article


def job_from_facebook(idx, store_urls, db):
    driver = get_selenium_driver()
    news_collection = db[Constants.NEWS_COLLECTION]
    urls = store_urls[idx]
    t = open("./fb_result.txt", 'a')
    for id_url in urls:
        id = id_url[0]
        url = id_url[1]
        try:
            # url = url[0]
            if "archive" not in url:
                archive_url = archiveis.capture(url)
                time.sleep(30)
            else:
                archive_url = url

            print(archive_url)
            # if "wip/" in archive_url:


        except Exception as e:
            print(str(e))
            t.write(url + "\n")
            t.flush()
            print("Factcheck ERROR in {}".format(url))
            continue
        if "wip/" in archive_url:
            archive_url = archive_url.replace("wip/", "")
        if "wip/" not in archive_url:
            try:
                return_dic = {}
                return_dic['ref_source'] = get_news_source_article(archive_url, driver)
                return_dic['news_id'] = id
                return_dic['ref_source_url'] = return_dic['ref_source']['url']
                print(id)
                news_collection.find_one_and_update({"news_id": return_dic['news_id']}, {"$set": return_dic},
                                                    upsert=True)
            except:
                print("Problem in {}".format(archive_url))
                continue

        else:
            news_collection.find_one_and_update({"news_id": id}, {
                "$set": {'archive_wip': archive_url.replace("/wip", ""), 'news_id': id}}, upsert=True)
    t.close()


def job_from_factcheck_facebook(idx, fact_urls, db):
    driver = get_selenium_driver()
    source_db = db['fake_news_source_article']
    format_db = db['fake_news_format']
    id_url_list = fact_urls[idx]
    for id_url in id_url_list:
        tokens = ['posts', 'videos', 'photo', 'fbid', 'permalink']
        id = id_url[0]
        url = id_url[1]
        if source_db.find_one({'id': id}) is None:
            try:
                driver.get(url)
                fb_urls = driver.find_elements_by_xpath("//a[contains(@href, 'facebook.com')]")
                fb_urls = [i.get_attribute("href") for i in fb_urls]
                fb_urls = [i for i in fb_urls if any([t in i for t in tokens])]
                if len(fb_urls) == 0:
                    continue
                fb_url = fb_urls[0]
                print("FB URL is {}".format(fb_url))
                format_db.find_and_modify({'id': id}, {
                    '$set': {'url': fb_url, "type": "facebook"},
                })
                source_db.find_and_modify({'id': id}, {'url': fb_url, 'id': id}, upsert=True)
            except:
                print("ERROR in Factchecking URL {}".format(url))


from bson.objectid import ObjectId


def get_from_facebook(db):
    news_collection = db[Constants.NEWS_COLLECTION]
    url_list = []
    num_process = 1
    for query in [{"ref_source_url": {"$regex": "archive"}}, {"ref_source_url": {"$regex": "facebook"}}]:
        for fb_url in news_collection.find(query,
                                           {"news_id": 1, "ref_source_url": 1, "ref_source.ref_archive_url": 1,
                                            "ref_source.text": 1}):
            # "ref_source.text":{"$in":['NONE']}
            text = ""
            if fb_url:
                if "ref_source" in fb_url.keys() and "ref_archive_url" in fb_url['ref_source'].keys():
                    ref_source_url = fb_url['ref_source']['ref_archive_url']
                    text = fb_url['ref_source'].get("text", "")
                else:
                    ref_source_url = fb_url['ref_source_url']
                filter_list = ["Sorry", "WATCH LIVE", "you may know",
                               "See more", "Anyone can see",
                               "This video may", "more reply", "Skip all",
                               "Sign up", "in the group and what they post",
                               "log in to continue", "having problems playing", "# GiselleMaxwellslist became",
                               ]
                if len(text.split()) < 10 or any([i in text for i in filter_list]):
                    url_list.append((fb_url['news_id'], ref_source_url))

    url_chunk_list = chunkify(url_list, num_process)
    multiprocess_function(num_process, job_from_facebook, (url_chunk_list, db))



def fix_piaui(db):
    news_collection = db['news_collection']

    # domain = "poligrafo"
    domain = "aosfatos"
    for domain in ["poligrafo", "aosfatos"]:
        search_items = []
        for i in news_collection.find({'fact_url': {"$regex": domain}},
                                      {"news_id": 1, "fact_url": 1, "ref_source.text": 1}):
            text = i.get("ref_source", {}).get("text", "")
            news_id = i.get("news_id")
            fact_url = i.get("fact_url")
            # news_collection.find_one_and_update({"news_id": news_id}, {"$set": {"ref_source.text": text.replace("Lupa", "")}})
            if len(text.split()) < 5:
                search_items.append((news_id, fact_url))
        driver = get_selenium_driver()
        idx = 0
        for i in tqdm(search_items):
            # url_list, domain, content, lang, claim
            content = rule_based_crawl(i[1], driver, i[0], None, None)[2]
            print(i[1], content)
            news_collection.find_one_and_update({"news_id": i[0]}, {"$set": {"ref_source.text": content}})
            idx += 1
            if idx % 10 == 0:
                driver.close()
                driver = get_selenium_driver()


def fix_slaute(db):
    news_collection = db['news_collection']
    search_items = []
    for i in news_collection.find({'fact_url': {"$regex": "salute"}},
                                  {"news_id": 1, "fact_url": 1, "ref_source.text": 1}):
        text = i.get("ref_source", {}).get("text", "")
        news_id = i.get("news_id")
        fact_url = i.get("fact_url")
        search_items.append((news_id, fact_url))
    driver = get_selenium_driver()
    index = 0
    for i in tqdm(search_items):
        driver.get(i[1])
        contents = driver.find_elements_by_xpath("//div[@class='col-md-8']//p")
        contents = [i.text for i in contents]
        content = " ".join(contents)
        print(content)
        news_collection.find_one_and_update({"news_id": i[0]}, {"$set": {"ref_source.text": content}})
        index += 1
        if index % 10 == 0:
            driver.close()
            driver = get_selenium_driver()


def fix_pesacheck(db):
    news_collection = db['news_collection']
    search_items = []

    domain = "pesacheck"

    for i in news_collection.find({'fact_url': {"$regex": domain}},
                                  {"news_id": 1, "fact_url": 1, "ref_source.text": 1}):
        text = i.get("ref_source", {}).get("text", "")
        news_id = i.get("news_id")
        fact_url = i.get("fact_url")
        # news_collection.find_one_and_update({"news_id": news_id}, {"$set": {"ref_source.text": text.replace("Lupa", "")}})
        if len(text.split()) < 5:
            search_items.append((news_id, fact_url))
    driver = get_selenium_driver()
    idx = 0
    for i in tqdm(search_items):
        # url_list, domain, content, lang, claim
        return_dic = {}
        url = rule_based_crawl(i[1], driver, i[0], None, None)[0][0]
        try:
            return_dic['ref_source'] = page_in_archive(url=url, driver=driver)
            return_dic['ref_source_url'] = return_dic['ref_source']['url']
            if "archive" in url:
                return_dic["ref_source"]['ref_archive_url'] = url

        except:
            print("ERROR At {}".format(url))
        return_dic['news_id'] = i[0]
        return_dic['ref_source_url'] = url
        news_collection.find_one_and_update({"news_id": i[0]},
                                            {"$set": return_dic})
        idx += 1
        if idx % 10 == 0:
            driver.close()
            driver = get_selenium_driver()


def crawl_from_fact_checking(fake_claim, news_collection, driver):
    fact_site_url = fake_claim['url']
    domain = fake_claim['agency']
    claim = fake_claim['claim']
    try:
        source_url, domain, content, lang, claim = rule_based_crawl(fact_site_url, driver, domain, claim)
    except Exception as e:
        logging.error(str(e))
        print(str(e))
        logging.info("ERROR in {}".format(fact_site_url))
        print("ERROR in {}".format(fact_site_url))
        return
    if len(source_url) == 0:
        return
    logging.info("Fact Check URL:{}, Source URL: {}".format(fact_site_url, source_url))
    fake_claim['source_url_list'] = source_url
    fake_claim['news_source'] = domain
    fake_claim['lang'] = lang
    if content is not None and len(content) > 4:
        fake_claim['ref_source'] = {"text": content, "ref_source_url": source_url}
    else:
        fake_claim['ref_source'] = get_news_source_article(source_url, driver)
    news_collection.find_one_and_update({'id': fake_claim['id']}, {'$set': fake_claim}, upsert=True)
