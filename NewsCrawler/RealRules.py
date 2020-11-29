import os
from util.Util import chunkify, multiprocess_function, get_selenium_driver
import logging
import configparser
import time
from bs4 import BeautifulSoup
import traceback
import requests

key_words_list = ['sars-cov-2', 'covid-19', "coronavirus", "covid"]


def ReliableCrawler(name, page_count, db):
    if name == "cdc":
        main_url = "https://www.cdc.gov/media/archives.html?Sort=Article%20Date%3A%3Adesc&Page={}"
        crawl_func = page_in_cdc
    elif name == "who":
        main_url = "https://www.who.int/news-room/releases/{}"
        crawl_func = page_in_who
    elif name == "nih":
        main_url = "https://search.nih.gov/search/docs?affiliate=nih&dc=565&page={}&query=covid-19&utf8=%E2%9C%93"
        crawl_func = page_in_nih
    elif name == "webmd":
        main_url = "https://www.webmd.com/search/search_results/default.aspx?query=covid19&page={}"
        crawl_func = page_in_webMD
    elif name == "smithsonianmag":
        main_url = "https://www.smithsonianmag.com/search/?q=covid-19&page={}"
        crawl_func = page_in_smithsonianmag
    elif name == "science_daily":
        main_url = "https://www.sciencedaily.com/search/?keyword=covid19#gsc.tab=0&gsc.q=covid%2019%20site%3Awww.sciencedaily.com&gsc.sort=&gsc.page={}"
        crawl_func = page_in_science_daily
    elif name == "healthline":
        main_url = "https://www.healthline.com/health-news?ref=global"
        crawl_func = page_in_healthline

    elif name == "ecdc":
        main_url = "./crawled_data/ecdc.html"
        crawl_func = page_in_ecdc

    elif name == "mnt":
        main_url = "https://www.medicalnewstoday.com/coronavirus"
        crawl_func = page_in_MNT
    elif name == "mayo_clinic":
        main_url = "https://www.mayoclinic.org/diseases-conditions/coronavirus/symptoms-causes/syc-20479963"
        crawl_func = page_in_mayo_clinic
    elif name == "celeveland":
        main_url = "https://newsroom.clevelandclinic.org/category/news-releases/page/{}/"
        crawl_func = page_in_cleveland_clink
    elif name == "snopes":
        main_url = "https://www.snopes.com/news/page/{}"
        crawl_func = page_in_snopes
    elif name == "politico":
        main_url = "https://www.politico.com/search/{}?q=covid19"
        crawl_func = page_in_politico
    elif name == "dn":
        main_url = "{}"
        crawl_func = page_in_dn
    elif name == "publico":
        main_url = "{}"
        crawl_func = page_in_publico
    elif name == "afp":
        main_url = "https://www.afp.com/fr/search/results/covid-19?page={}&f[0]=im_field_tags%3A74"
        crawl_func = page_in_afp
    elif name == "elpais":
        main_url = "https://elpais.com/noticias/covid-19/{}/"
        crawl_func = page_in_elpais
    elif name == "abces":
        main_url = "https://www.abc.es/hemeroteca/resultados-busqueda-avanzada/todo/pagina-{}?tod=covid&nin=19"
        crawl_func = page_in_abces
    elif name == "animalpolitico":
        main_url = "{}"
        crawl_func = page_in_animalpolitico
    elif name == "lemonde":
        main_url = "https://www.lemonde.fr/recherche/?search_keywords=covid-19&start_at=03/01/2020&end_at=26/07/2020&search_sort=relevance_desc&page={}"
        crawl_func = page_in_lemonde
    elif name == "jn":
        main_url = "{}"
        crawl_func = page_in_jn
    elif name == "publico":
        main_url = ""
        crawl_func = page_in_publico
    elif name == "milenio":
        main_url = "https://www.efe.com/efe/espana/busqueda/50000538?q=covid-19&p={}&s=0"
        crawl_func = page_in_milenio
    else:
        raise NotImplementedError


    # TODO: Automatically extract the page number
    if page_count > 0:
        all_pages = list(range(1, page_count + 1, 1))
    else:
        all_pages = [-1]
    num_process = os.cpu_count() - 3
    all_pages_chunkify = chunkify(all_pages, num_process)
    multiprocess_function(num_process, function_ref=fetch_save_collection,
                          args=(all_pages_chunkify, main_url,
                                db, crawl_func))


def fetch_save_collection(idx, all_pages_chunkify, main_url,
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
        try:
            return_element = crawl_func(driver, url)
        except:
            break
        print("Length for Page {} is {}".format(no_page, len(return_element['info_list'])))
        if len(list(return_element.values())[0]) == 0:
            continue
        else:
            if return_element is None:
                continue

            return_element_list = [i if "url" in i.keys() else i.update({'url': j}) for i, j in
                              zip(return_element['info_list'], return_element['fact_url_list'])]
            for i in return_element_list:
                try:
                    i['ref_source'] = crawl_link_article(i['url'])
                    i['ref_source']['ref_source_url'] = i['url']
                    news_collection.find_one_and_update({'id':i['id']},{"$set":i}, upsert=True)
                except:
                    continue

    driver.close()

    news_collection = db[Constants.NEWS_COLLECTION]


    for i in return_list:

        if db.find_one({"id": i['id']}, {"id": 1}) is None:
            news_collection.update({'id': i['id']}, {'$set': i}, upsert=True)
            article_detail = crawl_link_article(i['url'])
            news_collection.find_one_and_update({'id': i['id']}, {'$set': article_detail.update({"agency": i['agency']})}, upsert=True)
            logging.info(f"Success finish {i['id']}.")


def page_in_who(driver, url):
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        documents = driver.find_elements_by_xpath("//div[@class='list-view vertical-list vertical-list--image']"
                                                  "//div[@class='list-view--item vertical-list-item']")
        for doc in documents:
            try:
                url = doc.find_element_by_xpath(".//a[@class='link-container table']").get_attribute("href")
                if "https://www.who.int" not in url:
                    url = "https://www.who.int" + url
                date = doc.find_element_by_xpath(".//div[@class='date']/span").text
                news_title = doc.find_element_by_xpath(".//p[@class='heading text-underline']").text
                if filter(news_title):
                    info_list.append({"agency": "who",
                                      "time_and_loc": date,
                                      "label": 'True',
                                      "claim": news_title,
                                      'lang': "en"
                                      })
                    fact_url_list.append(url)

            except:
                continue

    except:
        pass
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list
    }


def page_in_cdc(driver, url):
    # https://www.cdc.gov/media/archives.html?Sort=Article%20Date%3A%3Adesc&Page=2
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        urls = driver.find_elements_by_xpath("//div[@class='col-md-9']")
        for url in urls:
            url_link = url.find_element_by_xpath(
                ".//div[@class='card-title h4 text-left mb-1 mt-3 mt-md-0']//a").get_attribute("href")
            if "https://www.cdc.gov" not in url_link:
                url_link = "https://www.cdc.gov" + url_link

            title = url.find_element_by_xpath(
                ".//div[@class='card-title h4 text-left mb-1 mt-3 mt-md-0']//a/span/span").text
            try:
                date = url.find_element_by_xpath(".//span[text()[contains(., 'Article Date')]]//following::span").text
            except:
                date = "NONE"
            if filter(title):
                info_list.append({
                    "agency": "cdc",
                    "time_and_loc": date,
                    "label": 'True',
                    "title": title,
                    'lang': "en"
                })

                fact_url_list.append(url_link)

    except Exception as e:
        traceback.print_tb(e.__traceback__)
        pass
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list
    }


def page_in_webMD(driver, url):
    info_list = []
    fact_url_list = []
    # https://www.webmd.com/search/search_results/default.aspx?query=covid19&page=2
    # potential_url_list = []
    try:
        driver.get(url)

        # documents = driver.find_elements_by_xpath("//div[@class='results-container']//p[@class='search-results-doc-title']")
        th = driver.find_elements_by_xpath("//p[@class='search-results-doc-title']")

        for doc in th:
            try:
                html = doc.get_attribute("innerHTML")
                html = BeautifulSoup(html)
                url = html.find('a').get("href")
                news_title = html.find("a").get_text()

                # url = doc.find_element_by_xpath("//a").get_attribute("href")
                # news_title = doc.find_element_by_xpath("//a").text
                if "?" in news_title or "video" in news_title.lower():
                    continue
                if filter(news_title):
                    info_list.append({"agency": "webMD",
                                      "time_and_loc": "NONE",
                                      "label": 'True',
                                      "claim": news_title,
                                      'lang': "en"
                                      })
                    fact_url_list.append(url)

            except Exception as e:
                print(str(e))
                continue

    except Exception as e:
        print(str(e))
        pass
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list
    }


def page_in_nih(driver, url):
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        documents = driver.find_elements_by_xpath("//div[@id='results']//div[@class='content-block-item result']")
        # documents = driver.find_elements_by_xpath("//div[@id='results']//div[@class='content-block-item result']//h4[@class='title']")
        # //h4[@class='title']
        for doc in documents:
            try:
                # thi = doc.find_element_by_xpath("//h4[@class='title']").text

                url = doc.find_element_by_xpath(".//h4[@class='title']//a").get_attribute("href")
                news_title = doc.find_element_by_xpath(".//h4[@class='title']//a").text
                body = doc.find_element_by_xpath(".//span[@class='description']").text
                if filter(news_title) or filter(body):
                    # if filter(news_title):
                    info_list.append({"agency": "nih",
                                      "time_and_loc": "NONE",
                                      "label": 'True',
                                      "title": news_title,
                                      'lang': "en"
                                      })
                    fact_url_list.append(url)

            except Exception as e:
                print(str(e))
                continue

    except Exception as e:
        print(str(e))
        pass
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list
    }


def page_in_science_daily(driver, url):
    info_list = []
    fact_url_list = []
    # https://www.sciencedaily.com/search/?keyword=covid19#gsc.tab=0&gsc.q=covid%2019%20site%3Awww.sciencedaily.com&gsc.sort=&gsc.ref=more%3Areference_terms&gsc.page=5

    try:
        driver.get(url)
        documents = driver.find_elements_by_xpath("//a[@class='gs-title']")

        for doc in documents:
            try:
                url = doc.get_attribute("href")
                news_title = doc.text
                if filter(news_title):
                    info_list.append({"agency": "science_daily",
                                      "time_and_loc": "NONE",
                                      "label": 'True',
                                      "claim": news_title,
                                      'lang': "en"
                                      })
                    fact_url_list.append(url)

            except:
                continue

    except:
        pass
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list
    }


def page_in_healthline(driver, url):
    SCROLL_PAUSE_TIME = 0.5
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            time.sleep(SCROLL_PAUSE_TIME)

            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        documents = driver.find_elements_by_xpath("//li[@class='css-18vzruc']")
        for doc in documents:
            try:
                url = doc.find_element_by_xpath(".//div[@class='css-ps3vwz']"
                                                "/a[@class='css-1818u65']")
                news_title = url.text
                url = url.get_attribute("href")

                if "https://www.healthline.com/" not in url:
                    url = "https://www.healthline.com" + url
                if filter(news_title):
                    info_list.append({"agency": "healthline",
                                      "time_and_loc": "NONE",
                                      "label": 'True',
                                      "claim": news_title,
                                      'lang': "en"
                                      })
                    fact_url_list.append(url)

            except:
                continue



    except:
        pass
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list
    }


def page_in_smithsonianmag(driver, url):
    # url = "https://www.smithsonianmag.com/search/?q=covid-19&page={}".format(page)
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        documents = driver.find_element_by_xpath("//div[@id='resultsList']")
        documents = documents.find_elements_by_xpath(".//h3[@class='headline']/a")
        for doc in documents:
            # url = doc.find_element_by_xpath("//h3[@class='headline']/a").get_attribute("href")
            # title = doc.find_element_by_xpath("//h3[@class='headline']/a").text
            title = doc.text
            url = doc.get_attribute("href")
            if "https://www.smithsonianmag.com" not in url:
                url = "https://www.smithsonianmag.com" + url

            if filter(title) is False:
                continue
            info_list.append({
                "agency": "smithsonianmag",
                "title": title,
                "label": "True",
                "time_and_loc": "NONE",
                'lang': "en"
            })
            fact_url_list.append(url)






    except Exception as e:
        logging.error(e)
        print(str(e))
        pass

    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_undark(driver, url):
    #     https://undark.org/tag/covid19/page/2/
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        documents = driver.find_elements_by_xpath("//div[@class='loop-post-content']")
        for doc in documents:
            url = doc.find_element_by_xpath("//h5/a")
            title = url.text
            url = url.get_attribute("href")
            if filter(title) is False:
                continue
            info_list.append({
                "agency": "undark",
                "title": title,
                "label": "True",
                "time_and_loc": "NONE",
                'lang': "en"
            })
            fact_url_list.append(url)

    except:
        pass

    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_ecdc(driver, url="./crawled_data/ecdc.html"):
    # directly read url
    html = open(url, 'r').readlines()
    html = "\n".join(html)
    soup = BeautifulSoup(html, "html.parser")
    info_list = []
    fact_url_list = []
    articles = soup.findAll("article")
    for article in articles:
        try:
            url = article.find('a', href=True)['href']
            title = article.find("h3").getText()
            if filter(title) is False:
                continue
            if len(title) < 1 or "Video on" in title:
                continue
            if url in fact_url_list:
                continue
            info_list.append({
                "agency": "ecdc",
                "title": title,
                "label": "True",
                "time_and_loc": "NONE",
                'lang': "en"
            })
            fact_url_list.append(url)
        except:
            continue

    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_MNT(driver, url):
    # https: // www.medicalnewstoday.com / coronavirus
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        urls = driver.find_elements_by_xpath("//a[@class='css-ni2lnp']")
        for url in urls:
            href = url.get_attribute("href")
            title = url.text
            if href in fact_url_list:
                continue
            info_list.append(
                {
                    "agency": "mnt",
                    "title": title,
                    "label": "True",
                    "time_and_loc": "NONE",
                    'lang': "en"

                }
            )
            fact_url_list.append(href)
    except:
        pass

    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_mayo_clinic(driver, url):
    # https: // www.mayoclinic.org / diseases - conditions / coronavirus / symptoms - causes / syc - 20479963
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        urls = driver.find_elements_by_xpath("//ul[@id='relatedLinks_e4e21640-9045-439c-9b44-4f676624df46']/li")
        # urls = urls.find_elements_by_xpath(".//li")
        for url in urls:
            url_link = url.find_element_by_xpath(".//a").get_attribute("href")
            title = url.find_element_by_xpath(".//a").text

            if len(title) == 0:
                title = "NONE"

            try:
                date = url.find_element_by_xpath(".//span[@class='rc-date']").text
            except:
                date = "NONE"

            info_list.append({
                "agency": "mayo_clinic",
                "title": title,
                "label": "True",
                "time_and_loc": date,
                'lang': "en"
                # "time_and_loc": "NONE"
            })
            fact_url_list.append(url_link)

    except Exception as e:
        # print(str(e.__traceback__))
        traceback.print_tb(e.__traceback__)

    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_cleveland_clink(driver, url):
    # https://newsroom.clevelandclinic.org/category/news-releases/page/10/
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        # urls = driver.find_elements_by_xpath("//h2[@class='entry-title']")
        urls = driver.find_elements_by_xpath("//a[@class='entry-title-link']")
        for url in urls:
            # date = url.find_element_by_xpath("//span[@class='posted-on']/span[@class='entry-date']").text

            # print(url.get_attribute("innerHTML"))
            title = url.text
            # print(title)
            url = url.get_attribute("href")
            if filter(title):
                info_list.append({
                    "agency": "cleveland_clink",
                    "title": title,
                    "label": "True",
                    "time_and_loc": "NONE",
                    'lang': "en"
                })

                fact_url_list.append(url)
            else:
                continue



    except Exception as e:
        print(str(e))
        pass
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_snopes(driver, url):
    #     https://www.snopes.com/news/page/2/
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        # urls = driver.find_elements_by_xpath("//h5[@class='title']")
        urls = driver.find_elements_by_xpath("//article[@class='media-wrapper']")
        for url in urls:
            title = url.find_element_by_xpath(".//h5[@class='title']").text
            url = url.find_element_by_xpath(".//a").get_attribute("href")
            if filter(title):
                if url in fact_url_list:
                    continue
                info_list.append({
                    "agency": "snopes",
                    "title": title,
                    "label": "True",
                    "time_and_loc": "NONE",
                    'lang': "en"
                })

                fact_url_list.append(url)
            else:
                continue

    except Exception as e:
        print(str(e))
        pass
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_politico(driver, url):
    # https://www.politico.com/search/2?q=covid19
    info_list = []
    fact_url_list = []
    try:
        driver.get(url)
        urls = driver.find_elements_by_xpath("//div[@class='summary']")
        # urls = driver.find_elements_by_xpath("//h3/a[@target='_top']")

        for url in urls:
            title = url.find_element_by_xpath(".//h3/a").text
            # title = url.text
            date = url.find_element_by_xpath(".//p[@class='timestamp']/time").text
            url = url(".//h3/a").get_attribute("href")

            if filter(title):
                info_list.append({
                    "agency": "politico",
                    "title": title,
                    "label": "True",
                    "time_and_loc": date,
                    'lang': "en"
                    # "time_and_loc": "NONE"
                })

                fact_url_list.append(url)
            else:
                continue

    except Exception as e:
        print(str(e))
        pass
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_mit(driver, url="./crawled_data/mit.html"):
    info_list = []
    fact_url_list = []
    html = open(url, 'r').readlines()
    html = "\n".join(html)
    soup = BeautifulSoup(html)
    urls = soup.findAll("li")

    for url in urls:
        try:
            title = url.find("h3").find("a").getText()
        except:
            continue
        print(title)
        url_link = url.find("h3").find("a", href=True)['href']
        if filter(title):
            info_list.append({
                "title": title,
                'agency': "mit",
                'label': "True",
                'date_and_loc': "NONE",
                'lang': "en"
            })
            fact_url_list.append(url_link)
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_animalpolitico(driver, url):
    if str(url) == 0:
        url = "https://www.animalpolitico.com/archivo/?busqueda=coronavirus"
    else:
        url = "https://www.animalpolitico.com/archivo/?busqueda=covid-19"
    info_list = []
    fact_url_list = []
    driver.get(url)
    urls_html = driver.find_elements_by_xpath("//a[@class='ap_note_link']")

    for url in urls_html:
        title = url.get_attribute("title")
        author = url.get_attribute("figure-author")
        url_link = url.get_attribute("href")

        info_list.append({
            "title": title,
            'agency': "animalpolitico",
            'label': "True",
            'date_and_loc': "NONE",
            'author': author,
            "lang": "es",
            'url': url_link
        })
        fact_url_list.append(url_link)
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_milenio(driver, url):
    # sucess
    # https://www.efe.com/efe/espana/busqueda/50000538?q=covid-19&p={}&s=0
    info_list = []
    fact_url_list = []
    driver.get(url)
    urls_html = driver.find_elements_by_xpath("//article/a[@id='link']")
    for url in urls_html:
        url_link = url.get_attribute("href")
        span = url.find_element_by_xpath(".//h3/span").text
        title = url.find_element_by_xpath(".//h3").text
        title = title.replace(span, "")
        try:
            time = url.find_element_by_xpath(".//span[@id='fecha']").text
            location = url.find_element_by_xpath(".//span[@id='origen']").text
        except:
            time = "NONE"
            location = "NONE`"
        print(1)
        info_list.append({
            "title": title,
            'agency': "milenio",
            'label': "True",
            'date_and_loc': time + "-" + location,
            "lang": "es",
            'url': url_link
        })
        fact_url_list.append(url_link)

    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_elpais(driver, url):
    # Success
    #     https://elpais.com/noticias/covid-19/{}/
    info_list = []
    fact_url_list = []
    driver.get(url)
    urls_html = driver.find_elements_by_xpath("//article")
    for url in urls_html:
        url_link = url.find_element_by_xpath(".//h2//a").get_attribute("href")
        title = url.find_element_by_xpath(".//h2//a").text
        try:
            author = url.find_element_by_xpath(".//div/span[@class=' false']/a").text
        except:
            author = "NONE"
        print("1")
        time = url.find_element_by_xpath(".//time").get_attribute("datetime")
        info_list.append({
            "title": title,
            'agency': "elpais",
            'label': "True",
            'date_and_loc': time + "-" + "NONE",
            'author': author,
            "lang": "es",
            'url': url_link,
        })
        fact_url_list.append(url_link)
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_abces(driver, url):
    # success
    # https://www.abc.es/hemeroteca/resultados-busqueda-avanzada/todo/pagina-{}?tod=covid&nin=19

    info_list = []
    fact_url_list = []
    driver.get(url)
    urls_html = driver.find_elements_by_xpath("//li/h2")
    for url in urls_html:
        url_link = url.find_element_by_xpath("./a").get_attribute("href")
        title = url.find_element_by_xpath("./a").get_attribute("title")
        # author = url.find_element_by_xpath(".//div/span[@class=' false']/a").text
        time = url.find_element_by_xpath("./following::span[@class='date']").text
        author = url.find_element_by_xpath("./following::p/span[@class='author']").text
        info_list.append({
            "title": title,
            'agency': "abces",
            'label': "True",
            'date_and_loc': time + "-" + "NONE",
            'author': author,
            "lang": "es",
            'url': url_link
        })
        fact_url_list.append(url_link)
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_lemonde(driver, url):
    # success
    #     https://www.lemonde.fr/recherche/?search_keywords=covid-19&start_at=03/01/2020&end_at=26/07/2020&search_sort=relevance_desc&page={}
    info_list = []
    fact_url_list = []
    driver.get(url)
    urls_html = driver.find_elements_by_xpath("//section[@class='teaser teaser--inline-picture ']")
    for url in urls_html:
        url_link = url.find_element_by_xpath(".//a").get_attribute("href")
        title = url.find_element_by_xpath(".//h3").text

        try:
            author = url.find_element_by_xpath(".//span[contains(@class,'author')]").text
            time = url.find_element_by_xpath(".//span[@class='meta__date']").text.split("-")[0]
        except:
            author = "NONE"
            time = "NONE"

        info_list.append({
            "title": title,
            'agency': "lemonde",
            'label': "True",
            'date_and_loc': time + "-" + "NONE",
            'author': author,
            "lang": "fr",
            'url': url_link
        })
        fact_url_list.append(url_link)
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_afp(driver, url):
    # success
    #     https://www.afp.com/fr/search/results/covid-19?page={}&f[0]=im_field_tags%3A74
    info_list = []
    fact_url_list = []
    driver.get(url)
    urls_html = driver.find_elements_by_xpath("//h4//a")
    for url in urls_html:
        url_link = url.get_attribute("href")
        title = url.text
        time = url.find_element_by_xpath(".//preceding::div").text
        info_list.append({
            "title": title,
            'agency': "afp",
            'label': "True",
            'date_and_loc': time + "-" + "NONE",
            "lang": "fr",
            'url': url_link
        })
        fact_url_list.append(url_link)
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


import time as sleep_time


def page_in_jn(driver, url):
    # success
    #     https://www.afp.com/fr/search/results/covid-19?page={}&f[0]=im_field_tags%3A74
    idx = url
    url = "https://www.jn.pt/pesquisa.html?q=covid-19"
    info_list = []
    fact_url_list = []
    driver.get(url)
    sleep_time.sleep(5)
    # click specific page
    button = driver.find_elements_by_xpath("//div[@class='gsc-cursor']/div")[int(idx)]
    button.click()
    sleep_time.sleep(5)
    urls_html = driver.find_elements_by_xpath("//a[@class='gs-title']")
    for url in urls_html:
        url_link = url.get_attribute("href")
        title = url.text
        time = "NONE"
        info_list.append({
            "title": title,
            'agency': "jn",
            'label': "True",
            'date_and_loc': time,
            "lang": "pt",
            'url': url_link
        })
        fact_url_list.append(url_link)
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_dn(driver, url):
    #     https://www.afp.com/fr/search/results/covid-19?page={}&f[0]=im_field_tags%3A74
    #     https://www.publico.pt/coronavirus
    #     url = "https://www.jn.pt/pesquisa.html?q=covid-19"
    url = "https://www.dn.pt/tag/coronavirus.html"
    info_list = []
    fact_url_list = []
    driver.get(url)
    i = 0
    while i < 10:
        try:
            driver.find_element_by_xpath("//a/span[text()[contains(.,'Ver mais')]]/preceding::a").click()
            sleep_time.sleep(5)
        except:
            print("ERROR")
            th = 1
        i += 1
    urls = driver.find_elements_by_xpath("//article[@class='t-s11-am1']")

    for url in urls:
        url_link = url.find_element_by_xpath(".//a[@class='t-am-text']").get_attribute("href")
        title = url.find_element_by_xpath(".//h2[@class='t-am-title']").text
        time = "NONE"
        info_list.append({
            "title": title,
            'agency': "dn",
            'label': "True",
            'date_and_loc': time,
            "lang": "pt",
            'url': url_link
        })
        fact_url_list.append(url_link)
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


def page_in_publico(driver, url):
    #     https://www.afp.com/fr/search/results/covid-19?page={}&f[0]=im_field_tags%3A74
    #     https://www.publico.pt/coronavirus
    #     url = "https://www.jn.pt/pesquisa.html?q=covid-19"
    url = "https://www.publico.pt/coronavirus"
    info_list = []
    fact_url_list = []
    driver.get(url)
    i = 0
    while i < 5:
        try:
            driver.find_element_by_xpath("//a[text()[contains(.,'Mais artigos')]]").click()
            sleep_time.sleep(5)
        except:
            th = 1
        i += 1
    urls = driver.find_elements_by_xpath("//ul[@id='ul-listing']//div[@class='media-object-section']")

    for url in urls:
        url_link = url.find_element_by_xpath(".//a").get_attribute("href")
        title = url.find_element_by_xpath(".//a/h4[@class='headline']")
        title_text = title.text
        print(title_text)
        try:
            author = url.find_element_by_xpath(".//a[@rel='author']")
            author_text = author.text
        except:
            author_text = "NONE"
        time = "NONE"
        info_list.append({
            "title": title_text,
            'agency': "publico",
            'label': "True",
            'date_and_loc': time,
            'author': author_text,
            "lang": "pt",
            'url': url_link
        })
        fact_url_list.append(url_link)
    return {
        "info_list": info_list,
        "fact_url_list": fact_url_list}


import json


def page_in_archive(driver, url=None):
    # url = "https://archive.md/Q1kAy"
    if url is None:
        url = "https://archive.md/pyjiz"
    #
    # url = "https://archive.fo/aJeCO"
    # url = "https://archive.fo/I1jjJ"
    # url = "http://archive.is/Cx0Cx"
    # url = "https://archive.fo/Lw3yV"
    # url = "http://archive.md/5Sfpt"

    driver.get(url)
    origin_url = driver.find_element_by_xpath(
        "/html/body/center/div[1]/table/tbody/tr[1]/td[3]/form/table/tbody/tr/td[1]/input[1]").get_attribute("value")
    html = driver.find_element_by_xpath("//div[@class='body']").get_attribute("innerHTML")
    article = crawl_link_article(url=url, inner_html=html)

    content = article['text']
    if "facebook" in origin_url:
        try:
            content1 = driver.find_element_by_xpath(".//div[@id='js_3']").text
        except:
            content1 = content

        try:
            content2 = [i.text for i in driver.find_elements_by_xpath(
                "//span[@dir='auto' and @old-class='oi732d6d ik7dh3pa d2edcug0 qv66sw1b c1et5uql a8c37x1j muag1w35 enqfppq2 jq4qci2q a3bd9o3v knj5qynh oo9gr5id']")][
                0]
        except:
            content2 = content

        if len(content1.split()) > len(content.split()):
            content = content1
        if len(content2.split()) > len(content.split()):
            content = content2

        logging.info("Content {}".format(content))
        print("Content {}".format(content))
        comments = driver.find_elements_by_xpath("//div[contains(@aria-label, 'Comment')]")
        scree_name_list = []
        comment_list = []
        for c in comments:
            try:
                reply_user = c.find_element_by_xpath(".//a").get_attribute("href")
                screen_name = reply_user.split("https")[-1].split("?")[0].split("/")[-1]
                comment_text = c.text
                scree_name_list.append(screen_name)
                comment_list.append(comment_text)
            except:
                continue

            # content = driver.find_element_by_xpath("//div[@old-class='_1dwg _1w_m _q7o']")

        # content = driver.find_element_by_xpath("//div[@old-class='_1dwg _1w_m _q7o']")
        return_dic = {"screen_name": scree_name_list, "comment": comment_list, "text": content, "type": "facebook",
                      "url": origin_url}
    elif "twitter" in origin_url:
        # screen_name = origin_url.split("com/")[1].split("/")[0]
        # user_name = driver.find_element_by_xpath("/html/body/center/div[4]/div/div[1]/div/div/div/div/div/div[2]/main/div/div/div/div[1]/div/div[2]/div/section/div/div/div[1]/div/div/div/div/article/div/div[2]/div[2]/div/div/div/div[1]/a/div/div[1]/div[1]/span/span").text
        screen_name = None
        text = None
        # if len(text) > content:
        #     content = text

        try:
            time = driver.find_element_by_xpath(
                "/html/body/center/div[4]/div/div[1]/div/div/div/div/div/div[2]/main/div/div/div/div[1]/div/div[2]/div/section/div/div/div[1]/div/div/div/div/article/div/div[3]/div[4]/div/div[1]/span[1]/span").text
        except:
            time = None
        return_dic = {"screen_name": screen_name, "type": "twitter", 'url': origin_url, 'text': text, 'time': time}
    elif "youtube" in origin_url:
        screen_name = driver.find_elements_by_xpath("//a[@id='author-text']")
        comments = driver.find_elements_by_xpath("//div[@id='main']")
        screen_name = [i.text for i in screen_name]
        comments = [i.text for i in comments]
        return_dic = {"screen_name": screen_name, "comment": comments, "type": "youtube", 'url': origin_url}
    elif "instgram" not in origin_url and "reddit" not in origin_url:
        # consider this as traditional URL
        return_dic = {"type": "news", 'origin_url': origin_url}
        return_dic.update(article)
    else:
        return_dic = {"origin_url": origin_url, 'type': "unknown"}

    # Only keep the top 10 words for the title
    return_dic.update({"text": content, 'url': origin_url, 'title': " ".join(content.split(" ")[:10])})

    return return_dic


# def page_in_webarchive(driver, url):

from lxml import html


def page_in_perma(driver=None, url=None):
    # url = "https://perma.cc/4D68-XLWS"
    driver.get(url)
    response = requests.get(url)
    tree = html.fromstring(response.content)
    th = tree.xpath("//div[@figure-testid='post_message']//text()")
    text_content = []
    text_set = set()
    for i in th:
        if i not in text_set:
            text_content.append(i)
            text_set.add(i)
    th = " ".join(text_content)
    content = " ".join([i.get_attribute("text") for i in content])
    print(content)
    key = url.strip("/").split("/")[-1]
    url = "https://api.perma.cc/v1/public/archives/{}".format(key)
    response = requests.get(url)
    if response.status_code == 200:
        return_json = json.loads(response.content)
        print(return_json)
        origin_url = return_json['url']
        content = return_json['description']
        title = return_json['title']
        time = return_json['creation_timestamp']
        type = origin_url.split("//")[1].split(".")[1]
        return_dic = {"text": content, "type": type, "url": origin_url, 'time':time,"title": " ".join(content.split(" ")[:10])}
        return return_dic
    else:
        return None


from code.util import crawl_link_article, get_text_hash
import pandas as pd
from twitter_user_info_crawler.FetchUserProfileInfo import fetch_user_tweets
from code.util import Constants


def page_in_saglik(driver, url, db):
    driver.get(url)
    kws = ["koronavirüs", "kovid"]
    collection_c = db['news_collection']
    for page in range(2, 12, 1):
        urls_sele = driver.find_elements_by_xpath("//div[@id='bbAlt']//li/a")

        urls = [i.get_attribute("href") for i in urls_sele]
        title = [i.text for i in urls_sele]
        title_url = [(i, j) for i, j in zip(urls, title) if any([kw in j for kw in kws])]
        title_url = pd.DataFrame(title_url, columns=['url', 'title'])
        title_url['label'] = True
        title_url['loc'] = "Turkish"
        title_url['lang'] = "tr"
        title_url['agency'] = "saglik"
        print(len(title_url))
        for i in title_url.iterrows():
            id = i['agency'] + "-" + get_text_hash(i['url'])
            collection_c.insert({
                "$set": {
                    "news_id": id,
                    "ref_source_url": i['url'],
                    "statement": i['title'],
                    "lang": i['lang'],
                    "label": i['label']

                }
            })
            try:
                article = crawl_link_article(i['url'])

                collection_c.find_one_and_update({"news_id": id}, {
                    "$set": {
                        "ref_source_content": article['publish_date']
                    }
                })
            except:
                print("error in get text content")
                continue
        try:
            driver.find_element_by_xpath("//td/a[text()={}]".format(page)).click()
        except:
            print("ERROR in Page: {}".format(page))
            continue


def twitter_users(user, kws, db, lang):
    news_collection = db[Constants.NEWS_COLLECTION]
    tweet_collection = db[Constants.TWEET_COLLECTION]
    news_relate_collection = db[Constants.NEWS_TWEET_RELATION]
    users_collection = db[Constants.USER_PROFILE_RELATION]
    users_tweets = fetch_user_tweets(user, None, kws=kws, limit=1000)
    for user_tweet in users_tweets:
        print(user_tweet)
        news_id = "twitter" + "-" + str(user_tweet['id'])
        news_collection.find_one_and_update({"news_id": news_id}, {"$set": {"news_id": news_id,
                                                                            "label": "real",
                                                                            "statement": user_tweet['tweet'],
                                                                            "ref_source_url": "https://twitter.com/i/web/status/{}".format(
                                                                                user_tweet['id']),
                                                                            "lang": lang,
                                                                            "ref_source": {"text": user_tweet['tweet']}
                                                                            }}, upsert=True)
        news_relate_collection.find_one_and_update({"news_id": news_id}, {"$set": {
            "news_id": news_id,
            "tweet_list": [user_tweet['id']]
        }}, upsert=True)
        tweet_collection.find_one_and_update({"id": user_tweet['id']}, {"$set": user_tweet}, upsert=True)

    posted_tweet_ids = [i['id'] for i in users_tweets]
    users_collection.find_one_and_update({"screen_name": user},
                                         {"$set": {"screen_name": user, "recent_post": posted_tweet_ids}}, upsert=True)


# MoHFW_INDIA
def get_real_twitter(db):
    lang = {"MoHFW_INDIA": "hi", "CovidIndiaSeva": "hi", "WHO": "en", "NIH": 'en',
            "trvrb": "en", "MayoClinic": "en", "SSalud_mx": "es", "MinisteroSalute": 'it', "govpt": "pt",
            "santeprevention": "fr", "EU_OSHA": "en", "EU_Commission": "en", "sanidadgob": "es"}
    twitter_ids = []
    for i in db.news_tweet_relation.find({"news_id": {"$regex": "twitter"}}, {"news_id": 1}):
        news_id = i['news_id']
        tweet_id = int(news_id.split("-")[1])

        content = db.tweet_collection.find_one({"id": {"$in": [tweet_id, str(tweet_id)]}}, {"_id": 0})
        if content is None:
            twitter_ids.append(tweet_id)
            continue
        try:
            user = content['user']['screen_name']
            text = content['full_text']
        except:
            user = content['username']
            text = content['tweet']

        try:
            # print(lang[user])
            db.news_collection.find_one_and_update({"news_id": news_id}, {"$set": {"news_id": news_id,
                                                                                   "label": "real",
                                                                                   "statement": text,
                                                                                   "ref_source_url": "https://twitter.com/i/web/status/{}".format(
                                                                                       tweet_id),
                                                                                   "lang": lang[user],
                                                                                   "ref_source": {"text": text}
                                                                                   }}, upsert=True)
        except:
            print(user)

    with open("dyhdrate_tweet.txt", 'w') as f1:
        for i in twitter_ids:
            f1.write(str(i) + "\n")

from selenium.webdriver.support.ui import WebDriverWait
def fix_europa(url, driver):
    driver.get(url)
    WebDriverWait(driver, 10)
    # ecl-page-header__title
    titles = driver.find_elements_by_xpath("//h1")
    titles = [i.text for i in titles]
    title = titles[0]
    if "Daily News" in title:
        title_tags = driver.find_element_by_xpath("//div[@class='ecl-paragraph']/p")
        title = " ".join([i.text for i in title_tags.find_elements_by_xpath("./strong")])
        content = driver.find_element_by_xpath("/html/body/app-root/app-detail/main/div/div/div[2]/section[1]/div/p[2]").text

    else:
        content = driver.find_element_by_xpath("//div[@class='ecl-col-md-9']").text
    inner_html = driver.page_source
    return {"title":title, "text":content, "html":inner_html}


def fix_mscbs(url, driver):
    driver.get(url)
    title = driver.find_element_by_xpath("//h2").text
    content = "\n".join([i.text for i in driver.find_elements_by_xpath("//section[@role='main']/div")])
    inner_html = driver.page_source
    return {'title':title, "text":content, "html":inner_html}

def fix_portugal(url, driver):
    driver.get(url)

    title = driver.find_element_by_xpath("//h1[@class='title']").text
    content = driver.find_elements_by_xpath("//div[@id='regText']")
    content = "\n".join([i.text for i in content])
    inner_html = driver.page_source
    return {'title':title, "text":content, "html":inner_html}


def filter(claim_text):
    flag = False
    for key in key_words_list:
        if key in claim_text.lower():
            flag = True
            break

    return flag



reliable = """
cdc
who
nih
webmd
smithsonianmag
science_daily
healthline
ecdc
mnt
mayo_clinic
celeveland
snopes
politico
mit
dn
publico
afp
elpais
abces
animalpolitico
lemonde
jn
publico
milenio"""

def real_news_all_in_one(db):
    for name in reliable.split("\n"):
        name = name.strip()
        ReliableCrawler(name, page_count=30, db=db)
    get_real_twitter(db)




