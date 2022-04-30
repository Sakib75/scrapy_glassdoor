import scrapy
import json
import pandas as pd
class GscraperSpider(scrapy.Spider):
    name = 'gscraper'
    renames = {'company_name': 'company', 'reviewId': 'review_id','ratingOverall':'review_rating_all','ratingWorkLifeBalance':'review_rating_worklife','ratingCultureAndValues':'review_rating_culture','ratingCareerOpportunities':'review_rating_career','ratingDiversityAndInclusion':'review_rating_diversity','ratingCompensationAndBenefits':'review_rating_comp','ratingSeniorLeadership':'review_rating_senior','jobTitle':'job_title','summary':'review_title','reviewDateTime':'review_info_date'}
    columns = ['company',	'symbol',	'name_glassdoor','review_id',	'review_rating_all',	'review_rating_worklife',	'review_rating_culture',	'review_rating_career',	'review_rating_diversity',	'review_rating_comp',	'review_rating_senior',	'employee_info'	,'review_title',	'review_info',	'pros',	'cons',	'advice','total_reviews','url',	]
    def start_requests(self):
        df_input = pd.read_csv('input/input.csv')
        for i in range(0,len(df_input)):
            com_name = df_input.loc[i,'company']
            symbol = df_input.loc[i,'symbol']
            base_url = df_input.loc[i,'url']
            base_url =  ".".join(base_url.replace('/Overview/','/Reviews/').replace('/Working-at-','/').replace('-EI_','-Reviews-').replace('-Reviews-IE','-Reviews-E').split('.')[:-2])
            base_url = base_url.replace("https://www.glassdoor.com/","https://www.glassdoor.ca/")
            yield scrapy.Request(url=base_url + '.htm?filter.iso3Language=eng', meta={'com_name':com_name,'symbol':symbol,'proxy': 'http://scraperapi:ef5ce54b6e77c240354ac4f5efdd3cc1@proxy-server.scraperapi.com:8001'},)
        # for i in range(1,1000):
        #     yield scrapy.Request(
        # # url="https://www.glassdoor.com/Reviews/Amazon-Reviews-E6036_P4.htm?filter.iso3Language=eng", 
        #     url = f"https://www.glassdoor.com/Reviews/Amazon-Reviews-E6036.htm?filter.iso3Language=eng",
        #     callback=self.parse, 
            # meta={'proxy': 'http://scraperapi:ef5ce54b6e77c240354ac4f5efdd3cc1@proxy-server.scraperapi.com:8001'}

        # yield scrapy.Request(url="https://www.glassdoor.com/Reviews/Amazon-Reviews-E6036.htm?filter.iso3Language=eng", callback=self.parse_ratings)


    def parse(self, response):
        all_reviews = response.xpath("//h2[@data-test='overallReviewCount']/span/strong[1]/text()").get()
        all_reviews = int(all_reviews.replace(',','').strip())
        total_page_no = round(all_reviews/10) + 1

        print(f'Number of Total Pages - {total_page_no}')
        # total_page_no = 2
        for i in range(1,total_page_no):
            if(i == 1):
                dont_filter = True
            else:
                dont_filter = False
            url = response.request.url.split('.htm?filter.iso3Language=eng')[0] + f'_P{i}.htm?filter.iso3Language=eng'
            yield scrapy.Request(url=url, callback=self.parse_ratings, meta={'total_reviews':total_page_no, 'com_name': response.request.meta['com_name'],'symbol': response.request.meta['symbol'],'proxy': 'http://scraperapi:ef5ce54b6e77c240354ac4f5efdd3cc1@proxy-server.scraperapi.com:8001'}, dont_filter=dont_filter,)

    def parse_ratings(self, response):
        script = response.xpath("//article/script").get()
        data = json.loads(script.split('apolloState\":')[-1][0:-11])
        rq = data['ROOT_QUERY']
        revs = rq[list(rq.keys())[2]]
        reviews = revs['reviews']
        for i in range (0,len(reviews)):
            review = reviews[i]
            review['company_name'] = response.request.meta['com_name']
            review['symbol'] = response.request.meta['symbol']
            review['name_glassdoor'] = response.request.meta['com_name']
            review['total_reviews'] = response.request.meta['total_reviews']
            for k,v in self.renames.items():
                review[v] = review.pop(k)
            review['url'] = response.request.url
            review['employee_info'] = response.xpath(f"//ol[@class=' empReviews emp-reviews-feed pl-0']/li[{i+1}]/div/div/div[1]/div/span/text()").get()
            review['review_info'] ="".join((response.xpath(f"//ol[@class=' empReviews emp-reviews-feed pl-0']/li[{i+1}]/div/div/div[2]/div/div/span//text()").getall()))
            
            f = dict()
            for hd in self.columns:
                f[hd] = review[hd]
            yield f

        





