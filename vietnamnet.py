import re
from bs4 import BeautifulSoup
from bs4.element import Tag
from pymongo import MongoClient
import requests
from datetime import datetime
import sys



def get_true_category_name(category: str):
    if category in ['xa-hoi', 'phap-luat']:
        return 'thoi-su'
    elif category in ['oto-xe-may', 'o-to-xe-may']:
        return 'xe'
    elif category in ['thong-tin-truyen-thong', 'khoa-hoc', 'so-hoa', 'suc-manh-so']:
        return 'khoa-hoc-cong-nghe'
    else:
        return category


class VietnamnetCrawler:
    def __init__(self) -> None:
        self.database = 'vietnamnet'
        self.root_url = 'https://vietnamnet.vn'

    def get_category_list(self):
        with open(f'{self.database}/categories/categories.txt', 'r') as file:
            categories = [line.strip() for line in file.readlines()]
        return categories

    def crawl_links(self):
        categories = self.get_category_list()
        for category in categories:
            self.crawl_link_category(category)

    def crawl_link_category(self, category: str):
        print(f'Crawl data for category: {category}')
        article_links = set()
        page_num = 1

        # vietnamnet has unlimited page
        max_page = 25
        while page_num <= max_page:
            print(f"Crawling links [{page_num} / {max_page}]", end='\r')
            sys.stdout.flush()

            url = f'{self.root_url}/{category}-page{page_num - 1}/'
            page_num += 1

            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')

            # find all the link
            h3_tags = soup.find_all(
                'h3', class_=lambda x: 'vnn-title' in x.split())
            for h3_tag in h3_tags:
                a_tag = h3_tag.find('a')
                article_link = a_tag['href']
                article_links.add(article_link + '\n')

        # write to file
        print(
            f'Write {len(article_links)} links to file for category: {category}\n')
        file_path = f'{self.database}/categories/{category}.txt'
        with open(file_path, 'w') as file:
            file.writelines(article_links)

    def crawl_articles(self):
        categories = self.get_category_list()
        for category in categories:
            self.crawl_article_category(category)

    def crawl_article_category(self, category: str):
        client = MongoClient("mongodb://localhost:27017/")
        db = client['newspaper']
        collection = db[get_true_category_name(category)]
        file_path = f'{self.database}/categories/{category}.txt'
        with open(file_path, 'r') as file:
            article_links = [line.strip() for line in file.readlines()]

        articles = []
        fail_attempt = 0
        num_of_articles = len(article_links)

        print(f'Crawl {num_of_articles} articles for category: {category}')
        for index, url in enumerate(article_links):
            print(
                f"Crawling article [{index + 1} / {num_of_articles}]", end='\r')
            sys.stdout.flush()

            if self.root_url not in url:
                url = self.root_url + url

            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')

            try:
                h1_title = soup.find('h1', class_='content-detail-title')
                h2_description = soup.find(
                    'h2', class_='content-detail-sapo sm-sapo-mb-0')
                div_date = soup.find('div', class_='bread-crumb-detail__time')
                div_content = soup.find('div', id='maincontent')
                p_contents = div_content.find_all('p')

                contents = set([p_content.get_text()
                               for p_content in p_contents])
                div_date_info = div_date.get_text().split(',')
                datetime_str = div_date_info[1].strip()
                published_date = datetime.strptime(
                    datetime_str, '%d/%m/%Y - %H:%M')

                article = {
                    'link': url,
                    'published_date': published_date,
                    'title': h1_title.get_text(),
                    'description': h2_description.get_text(),
                    'content': '\n'.join(contents)
                }
                articles.append(article)

            except Exception as e:
                fail_attempt += 1

        print(f'Number of article success: {num_of_articles - fail_attempt}')
        print(f'Number of article failed: {fail_attempt}')
        print('Insert into database\n')
        collection.insert_many(articles)
        client.close()