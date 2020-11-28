import requests
from util.Util import crawl_link_article
import logging
def page_in_archive(driver, url=None):
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
    th = tree.xpath("//div[@data-testid='post_message']//text()")
    text_content = []
    text_set = set()
    for i in th:
        if i not in text_set:
            text_content.append(i)
            text_set.add(i)
    content = " ".join(text_content)
    # content = " ".join([i.get_attribute("text") for i in content])
    # print(content)
    key = url.strip("/").split("/")[-1]
    url = "https://api.perma.cc/v1/public/archives/{}".format(key)
    response = requests.get(url)
    if response.status_code == 200:
        return_json = json.loads(response.content)
        print(return_json)
        origin_url = return_json['url']
        content = return_json['description']
        time = return_json['creation_timestamp']
        type = origin_url.split("//")[1].split(".")[1]
        return_dic = {"text": content, "type": type, "url": origin_url, 'time': time,
                      "title": " ".join(content.split(" ")[:10])}
        return return_dic
    else:
        return None