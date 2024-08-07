import re
from bs4 import BeautifulSoup
from bs4.element import Tag
from pymongo import MongoClient
import requests
from datetime import datetime



class DantriCrawler:
    root_url = 'https://dantri.com.vn'
    web_name = 'dantri'
    categories = [
        "xa-hoi", "phap-luat", "the-gioi", "kinh-doanh",
        "giai-tri", "the-thao", "giao-duc", "suc-khoe",
        "du-lich", "o-to-xe-may", "khoa-hoc-cong-nghe", "suc-manh-so"
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
        if category in ['xa-hoi', 'phap-luat']:
            return 'thoi-su'
        elif category in ['o-to-xe-may']:
            return 'xe'
        elif category in ['suc-manh-so']:
            return 'khoa-hoc-cong-nghe'
        else:
            return category
        
    @staticmethod
    def get_all_links(category=None, unique=True):
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
            filter = {"link": {"$regex": DantriCrawler.web_name}}

            if category is not None:
                filter['category'] = DantriCrawler.get_category_name(category)

            for document in collection.find(filter):
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
            filter = {"link": {"$regex": DantriCrawler.web_name}}
            for document in collection.find(filter):
                links.append(document['link'])

        if unique:
            links = set(links)

        return links
    
    @staticmethod
    def crawl_article_links(category: str):
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
        article_links = set()
        article_black_list = set()

        link_and_thumbnails = []
        black_list = set()
        page_num = 1

        # dantri has maximum 30 page
        max_page = 30
        while page_num <= max_page:
            print(f"\rCrawling links [{page_num} / {max_page}]", end='')

            url = f'{DantriCrawler.root_url}/{category}/trang-{page_num}.htm'
            page_num += 1

            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # find all the link
                article_tags = soup.find_all('article', class_='article-item')
                for article_tag in article_tags:
                    a_tag = article_tag.find('a')
                    article_link = f'{DantriCrawler.root_url}/{a_tag["href"]}'
                    img_tag = article_tag.find('img')

                    # if the category is wrong -> skip
                    if category not in article_link:
                        continue

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
    
    @staticmethod
    def crawl_article_content(link: str):
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
            article_tag = soup.find('article')
            h1_title = article_tag.find('h1')
            time = article_tag.find('time')

            # extract date info
            published_date = datetime.strptime(
                time['datetime'], '%Y-%m-%d %H:%M'
            )

            # normal
            if 'singular-container' in article_tag.get('class', []):
                description_tag = article_tag.find(class_="singular-sapo")
                div_content = article_tag.find('div', class_='singular-content')

                # clean the description
                description = description_tag.get_text().strip().removeprefix('(Dân trí)')
                description = description_tag.get_text().removeprefix(' - ')

                # loop through all content, only keep p (text) and figure(img)
                for element in div_content:
                    if not isinstance(element, Tag):
                        continue

                    # only keep text content (remove author text)
                    if element.name == 'p' and 'text-align:right' not in element.get('style', []):
                        content_list.append(element.get_text().strip())

                    elif element.name == 'figure' and 'image' in element['class']:
                        # extract image link and caption
                        img_tag = element.find('img')

                        image_link = None
                        if img_tag.get('src', '').startswith('http'):
                            image_link = img_tag['src']
                        elif img_tag.get('data-src', '').startswith('http'):
                            image_link = img_tag['data-src']

                        fig_caption = element.find('figcaption')
                        caption = ''
                        if fig_caption is not None:
                            caption = fig_caption.get_text().strip()

                        img_content = f'IMAGECONTENT:{image_link};;{caption}'
                        content_list.append(img_content)

            # dnews and photo-story
            elif 'e-magazine' in article_tag.get('class', []):
                description_tag = article_tag.find(class_="e-magazine__sapo")
                div_content = article_tag.find('div', class_='e-magazine__body')

                # clean the description
                description = description_tag.get_text().strip().removeprefix('(Dân trí)')
                description = description_tag.get_text().removeprefix(' - ')

                # loop through all content, only keep p (text) and figure(img)
                for element in div_content:
                    if not isinstance(element, Tag):
                        continue

                    # only keep text content (remove author text)
                    if element.name == 'p' and 'text-align:right' not in element.get('style', []):
                        content_list.append(element.get_text().strip())

                    elif element.name == 'figure' and 'image' in element['class']:
                        # extract image link and caption
                        img_tag = element.find('img')

                        image_link = None
                        if img_tag.get('src', '').startswith('http'):
                            image_link = img_tag['src']
                        elif img_tag.get('data-src', '').startswith('http'):
                            image_link = img_tag['data-src']

                        fig_caption = element.find('figcaption')
                        caption = ''
                        if fig_caption is not None:
                            caption = fig_caption.get_text().strip()

                        img_content = f'IMAGECONTENT:{image_link};;{caption}'
                        content_list.append(img_content)






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

    @staticmethod
    def crawl_articles(category: str):
        """
        Crawl all articles for the given category, Log all errors

        Parameters:
        ----------
        Category

        Returns:
        ----------
        Article list and Black (link) list
        """

        fail_attempt = 0
        articles = []
        article_links, black_list = DantriCrawler.crawl_article_links(category)
        fail_list = []
        print(f'Crawl articles for category: {category}')

        for index, (link, thumbnail) in enumerate(article_links):
            print(
                f"\rCrawling article [{index + 1} / {len(article_links)}], failed: {fail_attempt}", end=''
            )

            article = DantriCrawler.crawl_article_content(link)
            if isinstance(article, dict):
                article['thumbnail'] = thumbnail
                article['category'] = DantriCrawler.get_category_name(category)
                articles.append(article)
            else:
                fail_attempt += 1
                fail_list.append(article)
                # black_list.add(link)

        print(
            f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n'
        )

        # log all the fail attempt
        with open(f'error_log/{DantriCrawler.web_name}/error-{category}.txt', 'w') as file:
            file.writelines(
                [f'Link: {item[0]} ;; Exception: {str(item[1])}\n' for item in fail_list])

        return articles, black_list

    @staticmethod
    def crawl_all_data(categories=[]):
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
            categories = DantriCrawler.categories

        for category in categories:
            articles, black_list = DantriCrawler.crawl_articles(category)

            # update article and black link to database
            with MongoClient("mongodb://localhost:27017/") as client:
                db = client['Ganesha_News']

                if len(articles) > 0:
                    collection = db['newspaper_v2']
                    collection.insert_many(articles)
                    
                if len(black_list) > 0:
                    black_collection = db['black_list']
                    black_collection.insert_many(
                        [{'link': link} for link in black_list]
                    )

    @staticmethod
    def test_number_of_links():
        print(len(DantriCrawler.get_all_black_links()))
        print(len(DantriCrawler.get_all_black_links(unique=False)))

        print(len(DantriCrawler.get_all_links()))
        print(len(DantriCrawler.get_all_links(unique=False)))

    @staticmethod
    def test_crawl_content(link=''):
        if link == '':
            link = 'https://vnexpress.net/nha-khoa-hoc-viet-phan-tich-gene-phat-hien-som-ung-thu-gan-4769644.html'
        print(*DantriCrawler.crawl_article_content(link)
              ['content'], sep='\n')

    @staticmethod
    def test_crawl_links():
        link_list, black_list = DantriCrawler.crawl_article_links(
            'suc-khoe')
        print('List')
        print(*link_list, sep='\n')
        print('Black list')
        print(*black_list, sep='\n')

if __name__ == '__main__':
    DantriCrawler.test_number_of_links()

