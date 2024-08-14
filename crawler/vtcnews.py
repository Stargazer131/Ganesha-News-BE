from bs4 import BeautifulSoup
from bs4.element import Tag
from pymongo import MongoClient
import requests
from datetime import datetime


class VtcnewsCrawler:
    categories = [
        "thoi-su-28", "the-gioi-30", "kinh-te-29", "giai-tri-33",
        "the-thao-34", "giao-duc-31", "suc-khoe-35",
        "oto-xe-may-37", "khoa-hoc-cong-nghe-82"
    ]

    web_name = 'vtcnews'
    root_url = 'https://vtcnews.vn'

    @staticmethod
    def get_category_name(category: str):
        """
        Map real category name to database category name.

        Parameters
        ----------
        category : str
            Real category name.

        Returns
        ----------
        str
            Database category name.
        """

        category = category[:-3]

        if category in ['oto-xe-may']:
            return 'xe'
        elif category in ['kinht-te']:
            return 'kinh-doanh'
        else:
            return category

    @staticmethod
    def get_all_links(unique=True):
        """
        Get all article links from the database.

        Parameters
        ----------
        None

        Returns
        ----------
        set
            Set of links.
        """

        with MongoClient("mongodb://localhost:27017/") as client:
            db = client['Ganesha_News']
            collection = db['newspaper_v2']
            cursor = collection.find({"web": VtcnewsCrawler.web_name}, {"link": 1, "_id": 0})
            if unique:
                return set(doc['link'] for doc in cursor)
            else:
                return [doc['link'] for doc in cursor]

    @staticmethod
    def get_all_black_links(unique=True):
        """
        Get all article links from the blacklist in the database.

        Parameters
        ----------
        None

        Returns
        ----------
        set
            Set of links.
        """

        with MongoClient("mongodb://localhost:27017/") as client:
            db = client['Ganesha_News']
            collection = db['black_list']
            cursor = collection.find({"web": VtcnewsCrawler.web_name}, {"link": 1, "_id": 0})
            if unique:
                return set(doc['link'] for doc in cursor)
            else:
                return [doc['link'] for doc in cursor]

    @staticmethod
    def crawl_article_links(category: str, max_page=30):
        """
        Crawl all article link for a specific category.

        Parameters
        ----------
        category : str
            The category from which to crawl article links.
        max_page : int
            Maximum number of pages to crawl from

        Returns
        ----------
        tuple
            A tuple containing:
            - List of (link, thumbnail_link)
            - Set of black links (links that can't be crawled)
        """

        print(f'Crawl links for category: {category}')
        article_links = VtcnewsCrawler.get_all_links()
        article_black_list = VtcnewsCrawler.get_all_black_links()

        link_and_thumbnails = []
        black_list = set()
        page_num = 1

        # vtc news has maximum 30 page
        max_page = min(max_page, 30)
        while page_num <= max_page:
            print(f"\rCrawling links [{page_num} / {max_page}]", end='')

            url = f'{VtcnewsCrawler.root_url}/{category}/trang-{page_num}.html'
            page_num += 1

            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # find all the link
                article_tags = soup.find_all('article')

                for article_tag in article_tags:
                    a_tag = article_tag.find('a')
                    article_link = f'{VtcnewsCrawler.root_url}{a_tag["href"]}'
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
        Crawl article content.

        Returns
        ----------
        tuple
            A tuple containing:
            - Article: The crawled article content.
            - tuple: (Link, Exception) if an error occurs.
        """

        try:
            response = requests.get(link)
            soup = BeautifulSoup(response.content, 'html.parser')

            content_list = []
            article_tag = soup.find('section', class_='nd-detail')
            span_date = article_tag.find('span', class_='time-update')
            h1_title = article_tag.find('h1')
            description_tag = article_tag.find('h2')

            # clean description
            description = description_tag.get_text().strip().removeprefix('(VTC News)')
            description = description.removeprefix(' - ')

            # extract date info
            span_date_info = span_date.get_text().split(',')[1].strip()
            date_str, time_str, _ = span_date_info.split()
            published_date = datetime.strptime(
                date_str.strip() + ' ' + time_str.strip(), '%d/%m/%Y %H:%M:%S'
            )

            div_content = article_tag.find('div', class_="edittor-content")
            for element in div_content:
                if not isinstance(element, Tag):
                    continue

                # text content
                if element.name == 'p' and 'expEdit' not in element.get('class', []) and len(element.get_text()) > 0:
                    content_list.append(element.get_text())

                # image content
                elif element.name == 'figure' and 'expNoEdit' in element.get('class', []):
                    # extract image link and caption
                    img_tag = element.find('img')

                    if img_tag is None:
                        continue

                    image_link = None
                    if img_tag.get('src', '').startswith('http'):
                        image_link = img_tag['src']
                    elif img_tag.get('data-src', '').startswith('http'):
                        image_link = img_tag['data-src']

                    fig_caption = element.find('figcaption')
                    caption = ''
                    if fig_caption is not None:
                        caption = fig_caption.get_text()

                    img_content = f'IMAGECONTENT:{image_link};;{caption}'
                    content_list.append(img_content)

                # image article
                elif element.name == 'div' and 'expNoEdit' in element.get('class', []):
                    for child in element:
                        if not isinstance(child, Tag):
                            continue
                        
                        # extract image link (caption may be?)
                        if child.name == 'figure':
                            img_tag = child.find('img')

                            image_link = None
                            if img_tag.get('src', '').startswith('http'):
                                image_link = img_tag['src']
                            elif img_tag.get('data-src', '').startswith('http'):
                                image_link = img_tag['data-src']

                            fig_caption = element.find('figcaption')
                            caption = ''
                            if fig_caption is not None:
                                caption = fig_caption.get_text()

                            img_content = f'IMAGECONTENT:{image_link};;{caption}'
                            content_list.append(img_content)

                        # extract image list
                        elif child.name == 'div' and child.find('p') is None:
                            image_list = []
                            for index, img_tag in enumerate(child.find_all('img')):
                                image_link = None
                                if img_tag.get('src', '').startswith('http'):
                                    image_link = img_tag['src']
                                elif img_tag.get('data-src', '').startswith('http'):
                                    image_link = img_tag['data-src']

                                img_content = f'IMAGECONTENT:{image_link};;1,{index + 1}'
                                image_list.append(img_content)

                            if len(image_list) > 0:
                                content_list.append(image_list)

                        # extract caption (find the direct child - p tag)
                        elif child.name == 'div' and child.find('p') is not None:
                            content_list.append(child.find('p').get_text())

                        # extract caption (maybe missing)
                        elif child.name == 'p':
                            content_list.append(child.get_text().strip())
                    
            if len(content_list) > 0:
                return {
                    'link': link,
                    'category': '',
                    'published_date': published_date,
                    'thumbnail': '',
                    'title': h1_title.get_text().strip(),
                    'description': description,
                    'content': content_list,
                    'web': VtcnewsCrawler.web_name
                }
            else:
                raise Exception('NO CONTENT')

        except Exception as e:
            return (link, e)

    @staticmethod
    def crawl_articles(category: str):
        """
        Crawl all articles for the given category and log all errors.

        Parameters
        ----------
        category : str
            The category for which to crawl articles.

        Returns
        ----------
        tuple
            - list: List of articles.
            - set: Set of blacklisted links (links that couldn't be crawled).
        """

        fail_attempt = 0
        articles = []
        article_links, black_list = VtcnewsCrawler.crawl_article_links(
            category)
        fail_list = []
        print(f'Crawl articles for category: {category}')

        for index, (link, thumbnail) in enumerate(article_links):
            print(
                f"\rCrawling article [{index + 1} / {len(article_links)}], failed: {fail_attempt}", end=''
            )

            article = VtcnewsCrawler.crawl_article_content(link)
            if isinstance(article, dict):
                article['thumbnail'] = thumbnail
                article['category'] = VtcnewsCrawler.get_category_name(category)
                articles.append(article)
            else:
                fail_attempt += 1
                fail_list.append(article)

                # add the link to black list except for Connection issue
                if not isinstance(article[1], requests.RequestException):
                    black_list.add(link)

        print(
            f'\nSuccess: {len(article_links) - fail_attempt}, Fail: {fail_attempt}\n'
        )

        # log all the fail attempt
        with open(f'error_log/{VtcnewsCrawler.web_name}/error-{category}.txt', 'w') as file:
            file.writelines(
                [f'Link: {item[0]} ;; Exception: {str(item[1])}\n' for item in fail_list])

        return articles, black_list

    @staticmethod
    def crawl_all_data(categories=[]):
        """
        Crawl all articles for all categories and update the database.

        Parameters
        ----------
        category_list : list, optional
            List of categories to crawl. Defaults to using the `categories` attribute.

        Returns
        ----------
        None
        """

        if len(categories) == 0:
            categories = VtcnewsCrawler.categories

        for category in categories:
            articles, black_list = VtcnewsCrawler.crawl_articles(category)

            # update article and black link to database
            with MongoClient("mongodb://localhost:27017/") as client:
                db = client['Ganesha_News']

                if len(articles) > 0:
                    collection = db['newspaper_v2']
                    collection.insert_many(articles)

                if len(black_list) > 0:
                    black_collection = db['black_list']
                    black_collection.insert_many(
                        [{'link': link, 'web': VtcnewsCrawler.web_name} for link in black_list]
                    )

    @staticmethod
    def test_number_of_links():
        print('Black list')
        print(f'All: {len(VtcnewsCrawler.get_all_black_links())}')
        print(
            f'Unique: {len(VtcnewsCrawler.get_all_black_links(unique=False))}\n')

        print('All link')
        print(f'All: {len(VtcnewsCrawler.get_all_links())}')
        print(f'Unique: {len(VtcnewsCrawler.get_all_links(unique=False))}\n')

    @staticmethod
    def test_crawl_content(link):
        article = VtcnewsCrawler.crawl_article_content(link)
        print(*article['content'], sep='\n')


if __name__ == '__main__':
    VtcnewsCrawler.test_number_of_links()