import re
from bs4 import BeautifulSoup
from bs4.element import Tag
from pymongo import MongoClient
import requests
from datetime import datetime



class DantriCrawler:
    root_url = 'https://dantri.com.vn'
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
            filter = {"link": {"$regex": "dantri"}}

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
            filter = {"link": {"$regex": "dantri"}}
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


    # def get_category_list(self):
    #     with open(f'{self.database}/categories/categories.txt', 'r') as file:
    #         categories = [line.strip() for line in file.readlines()]
    #     return categories

    # def crawl_links(self):
    #     categories = self.get_category_list()
    #     for category in categories:
    #         self.crawl_link_category(category)
    #     self.driver.quit()

    # def crawl_link_category(self, category: str):
    #     print(f'Crawl data for category: {category}')
    #     article_links = set()
    #     page_num = 1

    #     # dantri has maximum 30 page
    #     max_page = 30
    #     while page_num <= max_page:
    #         print(f"\rCrawling links [{page_num} / {max_page}]", end='')
    #         sys.stdout.flush()

    #         url = f'{self.root_url}/{category}/trang-{page_num}.htm'
    #         page_num += 1

    #         self.driver.get(url)
    #         page_source = self.driver.page_source
    #         soup = BeautifulSoup(page_source, 'html.parser')

    #         # find all the link
    #         h3_tags = soup.find_all('h3', class_='article-title')
    #         for h3_tag in h3_tags:
    #             a_tag = h3_tag.find('a')
    #             article_link = a_tag['href']
    #             article_links.add(article_link + '\n')

    #     # write to file
    #     print(
    #         f'Write {len(article_links)} links to file for category: {category}\n')
    #     file_path = f'{self.database}/categories/{category}.txt'
    #     with open(file_path, 'w') as file:
    #         file.writelines(article_links)

    # def crawl_articles(self):
    #     categories = self.get_category_list()
    #     for category in categories:
    #         self.crawl_article_category(category)

    # def crawl_article_category(self, category: str):
    #     client = MongoClient("mongodb://localhost:27017/")
    #     db = client['newspaper']
    #     collection = db[get_true_category_name(category)]
    #     file_path = f'{self.database}/categories/{category}.txt'
    #     with open(file_path, 'r') as file:
    #         article_links = [line.strip() for line in file.readlines()]

    #     articles = []
    #     fail_attempt = 0
    #     num_of_articles = len(article_links)

    #     print(f'Crawl {num_of_articles} articles for category: {category}')
    #     for index, url in enumerate(article_links):
    #         print(
    #             f"Crawling article [{index + 1} / {num_of_articles}]", end='\r')
    #         sys.stdout.flush()

    #         url = self.root_url + url
    #         response = requests.get(url)
    #         soup = BeautifulSoup(response.content, 'html.parser')

    #         try:
    #             h1_title = soup.find('h1', class_="title-page detail")
    #             time_tag = soup.find('time', class_="author-time")
    #             h2_description = soup.find('h2', class_="singular-sapo")
    #             div_content = soup.find('div', class_="singular-content")
    #             p_contents = div_content.find_all('p')

    #             p_content_str = '\n'.join(
    #                 [p_content.get_text() for p_content in p_contents])
    #             published_date = datetime.strptime(
    #                 time_tag['datetime'], '%Y-%m-%d %H:%M')

    #             article = {
    #                 'link': url,
    #                 'published_date': published_date,
    #                 'title': h1_title.get_text(),
    #                 'description': h2_description.get_text().removeprefix('(Dân trí) -'),
    #                 'content': p_content_str
    #             }
    #             articles.append(article)

    #         except Exception as e:
    #             fail_attempt += 1

    #     print(f'Number of article success: {num_of_articles - fail_attempt}')
    #     print(f'Number of article failed: {fail_attempt}')
    #     print('Insert into database\n')
    #     collection.insert_many(articles)
    #     client.close()


if __name__ == '__main__':
    links, black_list = DantriCrawler.crawl_article_links('khoa-hoc-cong-nghe')
    with open('link.txt', 'w') as file:
        file.writelines([f'{item[0]}\n{item[1]}\n\n' for item in links])
