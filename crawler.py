import re
from bs4 import BeautifulSoup
from bs4.element import Tag
from pymongo import MongoClient
import requests
from datetime import datetime
import sys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver import Chrome, ChromeOptions


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


class DantriCrawler:
    def __init__(self, headless=True, wait_time=15, driver=False) -> None:
        if driver:
            options = ChromeOptions()
            if headless:
                options.add_argument('--headless=new')
            self.driver = Chrome(options=options)
            self.wait = WebDriverWait(self.driver, wait_time)
        self.root_url = 'https://dantri.com.vn'
        self.database = 'dantri'

    def get_category_list(self):
        with open(f'{self.database}/categories/categories.txt', 'r') as file:
            categories = [line.strip() for line in file.readlines()]
        return categories

    def crawl_links(self):
        categories = self.get_category_list()
        for category in categories:
            self.crawl_link_category(category)
        self.driver.quit()

    def crawl_link_category(self, category: str):
        print(f'Crawl data for category: {category}')
        article_links = set()
        page_num = 1

        # dantri has maximum 30 page
        max_page = 30
        while page_num <= max_page:
            print(f"\rCrawling links [{page_num} / {max_page}]", end='')
            sys.stdout.flush()

            url = f'{self.root_url}/{category}/trang-{page_num}.htm'
            page_num += 1

            self.driver.get(url)
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # find all the link
            h3_tags = soup.find_all('h3', class_='article-title')
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

            url = self.root_url + url
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')

            try:
                h1_title = soup.find('h1', class_="title-page detail")
                time_tag = soup.find('time', class_="author-time")
                h2_description = soup.find('h2', class_="singular-sapo")
                div_content = soup.find('div', class_="singular-content")
                p_contents = div_content.find_all('p')

                p_content_str = '\n'.join(
                    [p_content.get_text() for p_content in p_contents])
                published_date = datetime.strptime(
                    time_tag['datetime'], '%Y-%m-%d %H:%M')

                article = {
                    'link': url,
                    'published_date': published_date,
                    'title': h1_title.get_text(),
                    'description': h2_description.get_text().removeprefix('(Dân trí) -'),
                    'content': p_content_str
                }
                articles.append(article)

            except Exception as e:
                fail_attempt += 1

        print(f'Number of article success: {num_of_articles - fail_attempt}')
        print(f'Number of article failed: {fail_attempt}')
        print('Insert into database\n')
        collection.insert_many(articles)
        client.close()


class VnexpressCrawler:
    def __init__(self) -> None:
        self.root_url = 'https://vnexpress.net'
        self.categories = [
            'phap-luat', 'thoi-su', 'the-gioi', 'kinh-doanh',
            'giai-tri', 'the-thao', 'giao-duc', 'suc-khoe',
            'du-lich', 'oto-xe-may', 'khoa-hoc', 'so-hoa'
        ]

    @staticmethod
    def get_category_name(category: str):
        """
        Map real category name to database category name

        Parameters:
        ----------
        category: Real category name

        Returns:
        ----------
        Database category name
        """
        if category in ['phap-luat']:
            return 'thoi-su'
        elif category in ['oto-xe-may']:
            return 'xe'
        elif category in ['khoa-hoc', 'so-hoa']:
            return 'khoa-hoc-cong-nghe'
        else:
            return category

    @staticmethod
    def get_all_links(category='', unique=True):
        """
        Get all article links from database

        Parameters:
        ----------
        category (optional)

        Returns:
        ----------
        Set of links
        """
        links = []
        with MongoClient("mongodb://localhost:27017/") as client:
            db = client['Ganesha_News']
            collection = db['newspaper_v2']

            if category == '':
                for document in collection.find():
                    links.append(document['link'])
            else:
                for document in collection.find({'category': VnexpressCrawler.get_category_name(category)}):
                    links.append(document['link'])

        if unique:
            links = set(links)

        return links

    @staticmethod
    def get_all_black_links(unique=True):
        """
        Get all article links from the black list in database

        Parameters:
        ----------
        Nothing

        Returns:
        ----------
        Set of links
        """
        links = []
        with MongoClient("mongodb://localhost:27017/") as client:
            db = client['Ganesha_News']
            collection = db['black_list']
            for document in collection.find():
                links.append(document['link'])

        if unique:
            links = set(links)

        return links

    def crawl_article_links(self, category: str):
        """
        Crawl all article link for a specific category

        Parameters:
        ----------
        category (str)

        Returns:
        ----------
        List of (link, thumbnail_link), Set of black links (link can't be crawled)
        """
        print(f'Crawl links for category: {category}')
        article_links = self.get_all_links()
        article_black_list = self.get_all_black_links()

        link_and_thumbnails = []
        black_list = set()
        page_num = 1

        # vnexpress has maximum 20 page
        max_page = 20
        while page_num <= max_page:
            print(f"\rCrawling links [{page_num} / {max_page}]", end='')
            # sys.stdout.flush()

            url = f'{self.root_url}/{category}-p{page_num}/'
            page_num += 1

            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # find all the link
                article_tags = soup.find_all('article')
                for article_tag in article_tags:
                    a_tag = article_tag.find('a')
                    article_link = a_tag['href']
                    img_tag = article_tag.find('img')

                    # no img tag mean no thumbnail -> skip
                    if img_tag is None:
                        if article_link not in article_black_list:
                            black_list.add(article_link)
                        continue

                    # thumbnail
                    image_link = None
                    if img_tag.get('src', '').startswith('http'):
                        image_link = img_tag['src']
                    elif img_tag.get('data-src', '').startswith('http'):
                        image_link = img_tag['data-src']

                    # check for duplicated and "black" link
                    if article_link not in article_links and article_link not in article_black_list:
                        article_links.add(article_link)
                        link_and_thumbnails.append((article_link, image_link))

            except Exception as e:
                pass

        print(f"\nFind {len(link_and_thumbnails)} links")
        return link_and_thumbnails, black_list

    def crawl_article_content(self, link: str):
        """
        Crawl article content

        Returns:
        ----------
        Article or tuple(Link, Exception)
        """
        response = requests.get(link)
        soup = BeautifulSoup(response.content, 'html.parser')

        try:
            content_list = []
            h1_title = soup.find('h1', class_='title-detail')
            p_description = soup.find('p', class_='description')
            span_place = p_description.find('span', class_='location-stamp')
            span_date = soup.find('span', class_='date')

            # some article have different tag for date info
            if span_date is None:
                span_date = soup.find('div', class_='date-new')

            # remove Place Text
            description = p_description.get_text()
            if span_place is not None:
                description = description.removeprefix(span_place.get_text())

            # extract date info
            span_date_info = span_date.get_text().split(',')
            date_str = span_date_info[1].strip()
            time_str = span_date_info[2].strip()[:5]
            published_date = datetime.strptime(
                date_str + ' ' + time_str, '%d/%m/%Y %H:%M'
            )

            # loop through all content, only keep p (text) and figure(img)
            article_content = soup.find('article', class_='fck_detail')
            for element in article_content:
                if isinstance(element, Tag):
                    # skip video content
                    if element.find('video') is not None:
                        continue

                    if element.name == 'p':
                        # only select p tag with 1 attr -> article text content
                        if len(element.attrs) == 1 and element.get('class', [''])[0] == 'Normal':
                            content_list.append(element.get_text())

                    # image content
                    elif element.name == 'figure':
                        # extract image link and caption
                        img_tag = element.find('img')

                        # some figure tag empty (the figure tag at the end of article)
                        if img_tag is None:
                            continue

                        image_link = None
                        if img_tag.get('src', '').startswith('http'):
                            image_link = img_tag['src']
                        elif img_tag.get('data-src', '').startswith('http'):
                            image_link = img_tag['data-src']

                        p_caption = element.find('p', class_='Image')
                        caption = ''
                        if p_caption is not None:
                            caption = p_caption.get_text()

                        img_content = f'IMAGECONTENT:{image_link};;{caption}'
                        content_list.append(img_content)

                    # for image article (different article structure)
                    elif element.name == 'div' and 'item_slide_show' in element.get('class', []):
                        # extract image link
                        img_tag = element.find('img')
                        image_link = None
                        if img_tag.get('src', '').startswith('http'):
                            image_link = img_tag['src']
                        elif img_tag.get('data-src', '').startswith('http'):
                            image_link = img_tag['data-src']

                        img_content = f'IMAGECONTENT:{image_link};;'
                        content_list.append(img_content)

                        # extract text content for image
                        div_caption = element.find('div', class_='desc_cation')
                        for p_tag in div_caption.find_all('p', class_='Normal'):
                            content_list.append(p_tag.get_text())

            if len(content_list) > 0:
                return {
                    'link': link,
                    'category': '',
                    'published_date': published_date,
                    'thumbnail': '',
                    'title': h1_title.get_text(),
                    'description': description,
                    'content': content_list
                }
            else:
                raise Exception('NO CONTENT')

        except Exception as e:
            return (link, e)

    def crawl_all_article(self, categories=[]):
        """
        Crawl all articles for all categories and update to database

        Parameters:
        ----------
        Category list (optional): Default use the categories attribute

        Returns:
        ----------
        Nothing
        """
        if len(categories) == 0:
            categories = self.categories

        for category in categories:
            fail_attempt = 0
            articles = []
            article_links, black_list = self.crawl_article_links(category)
            fail_list = []
            print(f'Crawl articles for category: {category}')

            for index, (link, thumbnail) in enumerate(article_links):
                print(
                    f"\rCrawling article [{index + 1} / {len(article_links)}], failed: {fail_attempt}", end=''
                )

                article = self.crawl_article_content(link)
                if isinstance(article, dict):
                    article['thumbnail'] = thumbnail
                    article['category'] = self.get_category_name(category)
                    articles.append(article)
                else:
                    fail_attempt += 1
                    fail_list.append(article)
                    black_list.add(link)

            print(
                f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n'
            )

            # log all the fail attempt
            with open(f'error_log/vnexpress/error-{category}.txt', 'w') as file:
                file.writelines(
                    [f'Link: {item[0]} ;; Exception: {str(item[1])}\n' for item in fail_list])

            # update article and black link to database
            with MongoClient("mongodb://localhost:27017/") as client:
                db = client['Ganesha_News']
                collection = db['newspaper_v2']
                black_collection = db['black_list']

                if len(articles) > 0:
                    collection.insert_many(articles)
                if len(black_list) > 0:
                    black_collection.insert_many(
                        [{'link': link} for link in black_list]
                    )

    def test_number_of_links(self):
        print(len(self.get_all_black_links()))
        print(len(self.get_all_black_links(unique=False)))

        print(len(self.get_all_links()))
        print(len(self.get_all_links(unique=False)))

    def test_crawl_content(self, link=''):
        if link == '':
            link = 'https://vnexpress.net/nha-khoa-hoc-viet-phan-tich-gene-phat-hien-som-ung-thu-gan-4769644.html'
        print(*self.crawl_article_content(link)['content'], sep='\n')

    def test_crawl_links(self):
        link_list, black_list = self.crawl_article_links('suc-khoe')
        print('List')
        print(*link_list, sep='\n')
        print('Black list')
        print(*black_list, sep='\n')


if __name__ == '__main__':
    crawler = VnexpressCrawler()
    crawler.test_number_of_links()
