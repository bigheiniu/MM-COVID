import os
from util.Util import chunkify, multiprocess_function, get_selenium_driver
import pickle
import time
import traceback
import timeout_decorator
from util import Constants
from NewsCrawler.FakeRulesFactCheck import crawl_from_fact_checking, get_from_facebook
key_words_list = ['sars-cov-2', 'covid-19', "coronavirus", "covid"]

from util.Util import get_text_hash


def fetch_fact_check(idx, all_pages_chunkify, main_url,
                     db, crawl_func):
    idx_pages = all_pages_chunkify[idx]
    return_list = []
    news_collection = db[Constants.NEWS_COLLECTION]
    driver = get_selenium_driver()
    for no_page in idx_pages:
        if no_page == -1:
            url = main_url
        else:
            url = main_url.format(no_page)
        return_element = crawl_func(driver, url)

        print("Length for Page {} is {}".format(no_page, len(return_element['info_list'])))
        if len(list(return_element.values())[0]) == 0:
            continue
        else:
            if return_element is None:
                continue

            for i in return_element:
                i['label'] = "fake"
                news_collection.find_one_and_update({'id': i['id']}, {"$set": i}, upsert=True)
                crawl_from_fact_checking(i, news_collection, driver)
            driver.close()
            driver = get_selenium_driver()



def page_in_poynter(driver, url):
    info_list = []
    fact_url_list = []

    driver.get(url)
    urls_one_page = driver.find_elements_by_xpath('//div[@class="post-container"]//a')
    urls_one_page = list(set([i.get_attribute("href") for i in urls_one_page]))
    try:
        for index, new_url in enumerate(urls_one_page):
            driver.get(new_url)
            agency = driver.find_element_by_xpath(
                "//header[@class='entry-header']//p[@class='entry-content__text entry-content__text--org']").text
            time_and_loc = driver.find_element_by_xpath(
                "//header[@class='entry-header']//p[@class='entry-content__text entry-content__text--topinfo']").text
            label = driver.find_element_by_xpath("//h1[@class='entry-title']/span").text
            claim = driver.find_element_by_xpath("//h1[@class='entry-title']").text.replace(label, "")

            fact_check_url = driver.find_element_by_xpath("//div[@class='entry']") \
                .find_element_by_tag_name("a").get_attribute("href")
            fact_url_list.append(fact_check_url)
            explaination = driver.find_element_by_xpath(
                "//p[@class='entry-content__text entry-content__text--explanation']").text
            orginated = \
            driver.find_element_by_xpath("//p[@class='entry-content__text entry-content__text--smaller']").text.split(
                ":")[1]
            info_list.append(
                {"claim": claim,
                 "agency": agency,
                 "time_and_loc": time_and_loc,
                 "label": label,
                 'id': agency + "-" + get_text_hash(fact_check_url),
                 "url": fact_check_url, "explain": explaination, "orginated": orginated, 'poynter_url': new_url})
    except Exception as e:
        traceback.print_tb(e)

    return info_list


@timeout_decorator.timeout(20)
def fix_poynter(driver, element):
    driver.get(element['poynter_url'])
    agency = driver.find_element_by_xpath("//p[text()[contains(., 'Fact-checked by: ')]]").text.replace(
        "Fact-checked by: ", "")
    time_and_loc = driver.find_element_by_xpath("//p[@class='entry-content__text entry-content__text--topinfo']").text
    element['agency'] = agency
    element['time_and_loc'] = time_and_loc
    element['time'] = element['time_and_loc'].split("|")[0]
    element['loc'] = element['time_and_loc'].split("|")
    if len(element['loc']) > 1:
        element['loc'] = element['loc'][1]
    else:
        element['loc'] = "unknown"
    return element

def page_in_other(driver, url, origin_list):
    source_url_list = []
    try:
        driver.get(url)
        urls_text = driver.find_elements_by_xpath("//a[string-length(text())>4]")
        urls = [i.get_attribute("href") for i in urls_text]
        text = [i.text for i in urls_text]
        urls_text = [(i, j) for i, j in zip(urls, text)]
        source_url_list = [i for i in urls_text if any([kw in i[1] for kw in origin_list])]
    except:
        pass

    return source_url_list

def page_in_snope(driver, url):
    urls_one_page = []
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)

        container_list = driver.find_elements_by_xpath('//div[@class="media-list"]/article')
        for i in container_list:
            urls_one_page.append(i.find_element_by_tag_name("a").get_attribute("href"))

        # driver.quit()
        for new_url in urls_one_page:
            driver.get(new_url)
            claim = driver.find_element_by_xpath("//div[@class='claim']/p").text

            label = driver.find_element_by_xpath("//div[@class='media rating']//h5").text
            date = driver.find_element_by_xpath("//span[@class='date date-published']").text

            flag = False
            for key in key_words_list:
                if key in claim.lower():
                    flag = True
            if flag is False:
                continue
            fact_url_list.append(new_url)

            info_url = {"agency": "snopes",
                        "time_and_loc": date,
                        "label": label,
                        'id': "snopes" + "-" + get_text_hash(new_url),
                        "claim": claim,
                        "url": new_url
                        }

            info_list.append(info_url)
        time.sleep(1)

    except:
        pass

    return info_list

def fact_check_crawler(name, page_count, db):

    if name == "poynter":
        main_url = "https://www.poynter.org/ifcn-covid-19-misinformation/page/{}/?covid_countries=0&covid_rating=0&covid_fact_checkers=0&orderby=views&order=DESC#038;covid_rating=0&covid_fact_checkers=0&orderby=views&order=DESC"

        crawl_func = page_in_poynter
    elif name == "snopes":
        # main_url = "https://www.snopes.com/fact-check/rating/false/page/{}"
        main_url = "https://www.snopes.com/fact-check/rating/false/page/{}"
        crawl_func = page_in_snope
    else:
        raise NotImplementedError

    news_collection = db[Constants.NEWS_COLLECTION]
    if page_count > 0:
        all_pages = list(range(1, page_count + 1, 1))
    else:
        all_pages = [-1]
    num_process = os.cpu_count() - 3
    all_pages_chunkify = chunkify(all_pages, num_process)
    multiprocess_function(num_process, function_ref=fetch_fact_check,
                          args=(all_pages_chunkify, main_url,
                                news_collection, crawl_func))



def fake_news_all_in_one(db):
    for name in ['poynter','snopes']:
        fact_check_crawler(name, page_count=100, db=db)

    # fix empty fact checking website by crawling the fact checking urls.
    get_from_facebook(db)

