from bs4 import BeautifulSoup
from bs4.element import Tag
from pymongo import MongoClient
import requests
from datetime import datetime


class VnexpressCrawler:
    root_url = 'https://vnexpress.net'
    web_name = 'vnexpress'
    categories = [
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
            filter = {"link": {"$regex": VnexpressCrawler.web_name}}

            if category is not None:
                filter['category'] = VnexpressCrawler.get_category_name(
                    category)

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
            filter = {"link": {"$regex": VnexpressCrawler.web_name}}
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
        article_links = VnexpressCrawler.get_all_links()
        article_black_list = VnexpressCrawler.get_all_black_links()

        link_and_thumbnails = []
        black_list = set()
        page_num = 1

        # vnexpress has maximum 20 page
        max_page = 20
        while page_num <= max_page:
            print(f"\rCrawling links [{page_num} / {max_page}]", end='')

            url = f'{VnexpressCrawler.root_url}/{category}-p{page_num}/'
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
                if not isinstance(element, Tag):
                    continue

                # skip video content
                if element.find('video') is not None:
                    continue

                # only select p tag with 1 attr -> article text content
                if element.name == 'p' and len(element.attrs) == 1 and element.get('class', [''])[0] == 'Normal':
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
        article_links, black_list = VnexpressCrawler.crawl_article_links(
            category)
        fail_list = []
        print(f'Crawl articles for category: {category}')

        for index, (link, thumbnail) in enumerate(article_links):
            print(
                f"\rCrawling article [{index + 1} / {len(article_links)}], failed: {fail_attempt}", end=''
            )

            article = VnexpressCrawler.crawl_article_content(link)
            if isinstance(article, dict):
                article['thumbnail'] = thumbnail
                article['category'] = VnexpressCrawler.get_category_name(category)
                articles.append(article)
            else:
                fail_attempt += 1
                fail_list.append(article)
                black_list.add(link)

        print(
            f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n'
        )

        # log all the fail attempt
        with open(f'error_log/{VnexpressCrawler.web_name}/error-{category}.txt', 'w') as file:
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
            categories = VnexpressCrawler.categories

        for category in categories:
            articles, black_list = VnexpressCrawler.crawl_articles(category)

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
        print(len(VnexpressCrawler.get_all_black_links()))
        print(len(VnexpressCrawler.get_all_black_links(unique=False)))

        print(len(VnexpressCrawler.get_all_links()))
        print(len(VnexpressCrawler.get_all_links(unique=False)))

    @staticmethod
    def test_crawl_content(link=''):
        if link == '':
            link = 'https://vnexpress.net/nha-khoa-hoc-viet-phan-tich-gene-phat-hien-som-ung-thu-gan-4769644.html'
        print(*VnexpressCrawler.crawl_article_content(link)
              ['content'], sep='\n')

    @staticmethod
    def test_crawl_links():
        link_list, black_list = VnexpressCrawler.crawl_article_links(
            'suc-khoe')
        print('List')
        print(*link_list, sep='\n')
        print('Black list')
        print(*black_list, sep='\n')


if __name__ == '__main__':
    VnexpressCrawler().test_number_of_links()
