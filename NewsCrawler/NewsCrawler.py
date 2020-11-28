from NewsCrawler.FakeNewsRules import fake_news_all_in_one
from NewsCrawler.RealRules import real_news_all_in_one

def get_news(db):
    for is_fake in ['True',False]:
        if is_fake:
            fake_news_all_in_one(db=db)
        else:
            real_news_all_in_one(db=db)



