import numpy as np
from pymongo import MongoClient
from crawler.dantri import DantriCrawler
from crawler.vietnamnet import VietnamnetCrawler
from crawler.vnexpress import VnexpressCrawler
from crawler.vtcnews import VtcnewsCrawler
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from server import data
import random
from gensim.models import LdaModel
from gensim.corpora import Dictionary
from gensim.matutils import sparse2full


def crawl_new_articles():
    if not data.is_collection_empty_or_not_exist('temporary_newspaper'):
        print('New articles have been crawled!')
        return False
    
    articles = []
    black_list = []
                
    for category in VnexpressCrawler.categories:
        temp_articles, temp_black_list = VnexpressCrawler.crawl_articles(category)
        articles.extend(temp_articles)
        black_list.extend(
            [{"link": link, "web": VnexpressCrawler.web_name} for link in temp_black_list]
        )
        
    for category in DantriCrawler.categories:
        temp_articles, temp_black_list = DantriCrawler.crawl_articles(category)
        articles.extend(temp_articles)
        black_list.extend(
            [{"link": link, "web": DantriCrawler.web_name} for link in temp_black_list]
        )
    
    for category in VietnamnetCrawler.categories:
        temp_articles, temp_black_list = VietnamnetCrawler.crawl_articles(category)
        articles.extend(temp_articles)
        black_list.extend(
            [{"link": link, "web": VietnamnetCrawler.web_name} for link in temp_black_list]
        )
        
    for category in VtcnewsCrawler.categories:
        temp_articles, temp_black_list = VtcnewsCrawler.crawl_articles(category)
        articles.extend(temp_articles)
        black_list.extend(
            [{"link": link, "web": VtcnewsCrawler.web_name} for link in temp_black_list]
        )

    with MongoClient("mongodb://localhost:27017/") as client:
        db = client['Ganesha_News']

        if len(articles) > 0:
            random.shuffle(articles)
            collection = db['temporary_newspaper']
            collection.insert_many(articles)
            
        if len(black_list) > 0:
            black_collection = db['black_list']
            black_collection.insert_many(black_list)

    print(f"\nCrawl {data.total_documents('temporary_newspaper')} new articles!\n")
    return True


def demo_crawl_new_articles(limit=5):
    if not data.is_collection_empty_or_not_exist('temporary_newspaper'):
        print('New articles have been crawled!')
        return False
    
    articles = []
    black_list = []
                
    for category in VnexpressCrawler.categories:
        temp_articles, temp_black_list = VnexpressCrawler.crawl_articles(category, limit)
        articles.extend(temp_articles)
        black_list.extend(
            [{"link": link, "web": VnexpressCrawler.web_name} for link in temp_black_list]
        )

    with MongoClient("mongodb://localhost:27017/") as client:
        db = client['Ganesha_News']

        if len(articles) > 0:
            random.shuffle(articles)
            collection = db['temporary_newspaper']
            collection.insert_many(articles)
            
        if len(black_list) > 0:
            black_collection = db['black_list']
            black_collection.insert_many(black_list)

    print(f"\nCrawl {data.total_documents('temporary_newspaper')} new articles!\n")
    return True


def check_duplicated_titles(similarity_threshold=0.8, time_threshold=5):
    # load database (old) and newly crawled article
    old_articles = data.get_titles('newspaper')
    new_articles = data.get_titles('temporary_newspaper')
    
    print('Preprocessing titles')
    old_titles = data.load_processed_titles()
    
    if len(old_titles) > len(old_articles):
        print('Duplicated titles have been checked!')
        return False
    
    new_titles = [data.process_title(doc['title']) for doc in new_articles]
    
    # create 2 separate matrix 
    vectorizer = TfidfVectorizer(lowercase=False)
    tfidf_matrix = vectorizer.fit_transform(old_titles + new_titles)
    old_tfidf_matrix = tfidf_matrix[ : len(old_titles)]
    new_tfidf_matrix = tfidf_matrix[len(old_titles) : ]
    duplicated_document_ids = set()
    remove_indices = set()
    black_list = []
    
    # check new titles vs old titles
    print('Check with database articles')
    cosine_sim_matrix = cosine_similarity(new_tfidf_matrix, old_tfidf_matrix, dense_output=False)
    rows, cols = cosine_sim_matrix.nonzero()
    values = cosine_sim_matrix.data
    filter_index = np.where(values >= similarity_threshold)[0]
    result = [(rows[i], cols[i]) for i in filter_index]
    
    for i, j in result:
        date_i = new_articles[i]['published_date']
        date_j = old_articles[j]['published_date']
        time_diff = abs((date_i - date_j).total_seconds())

        # if time difference < some hours -> duplicated
        limit = time_threshold * 3600
        if time_diff < limit:
            obj_id = new_articles[i]['_id']
            if obj_id not in duplicated_document_ids:
                duplicated_document_ids.add(obj_id)
                remove_indices.add(i)
                black_list.append({
                    'link': new_articles[i]['link'],
                    'web': new_articles[i]['web']
                })
            
    # check new titles vs new titles
    print('Check among new articles')
    cosine_sim_matrix = cosine_similarity(new_tfidf_matrix, dense_output=False)
    rows, cols = cosine_sim_matrix.nonzero()
    values = cosine_sim_matrix.data
    filter_index = np.where(values >= similarity_threshold)[0]
    result = [(rows[i], cols[i]) for i in filter_index if rows[i] < cols[i]]
    
    for i, j in result:
        date_i = new_articles[i]['published_date']
        date_j = new_articles[j]['published_date']
        time_diff = abs((date_i - date_j).total_seconds())

        # if time difference < some hours -> duplicated
        limit = time_threshold * 3600
        if time_diff < limit:
            obj_id = new_articles[j]['_id']
            if obj_id not in duplicated_document_ids:
                duplicated_document_ids.add(obj_id)
                remove_indices.add(j)
                black_list.append({
                    'link': new_articles[j]['link'],
                    'web': new_articles[j]['web']
                })
                            
    # update black list and temporary database
    with MongoClient("mongodb://localhost:27017/") as client:
        db = client['Ganesha_News']
        collection = db['temporary_newspaper']
        b_collection = db['black_list']
    
        if len(duplicated_document_ids) > 0:
            result = collection.delete_many({'_id': {'$in': list(duplicated_document_ids)}})
            print(f'Deleted {result.deleted_count} duplicated document')

        if len(black_list) > 0:
            result = b_collection.insert_many(black_list)
            print(f'Added {len(result.inserted_ids)} black list document')
            
    # update processed list
    print('Update processed titles list')
    new_titles = [title for i, title in enumerate(new_titles) if i not in remove_indices]
    data.save_processed_titles(old_titles + new_titles)
    
    return True
    

def update_nndescent_index():
    print('Load models')
    nndescent = data.load_nndescent()
    if nndescent.neighbor_graph[0].shape[0] == len(data.load_processed_titles()):
        print('Index graph has been updated!')
        return False

    lda_model = LdaModel.load('data/lda_model/lda_model_30')
    dictionary = Dictionary.load('data/lda_model/dictionary')
    
    print('Processing document content')
    processed_documents = []
    article_content = data.get_content('temporary_newspaper')
    for doc in article_content:
        title = data.process_sentence(doc['title'])
        description = data.process_paragraph(doc['description'])
        content = data.process_content(doc['content'])
        processed_documents.append(title + description + content)
        
    print('Predicting topic distributions')
    corpus = [dictionary.doc2bow(doc) for doc in processed_documents]
    lda_corpus = lda_model[corpus]
    corpus_topic_distributions = np.array([sparse2full(vec, lda_model.num_topics) for vec in lda_corpus])
    
    print('Update the index')
    nndescent.update(corpus_topic_distributions)
    data.save_nndescent(nndescent)
    data.save_neighbor_graph(nndescent.neighbor_graph[0])


def update_database():
    if data.is_collection_empty_or_not_exist('temporary_newspaper'):
        print('Articles have been updated to original database')
        return False

    with MongoClient("mongodb://localhost:27017/") as client:
        db = client['Ganesha_News']
        collection = db['newspaper']
        temp_collection = db['temporary_newspaper']
        articles = list(temp_collection.find({}, {"_id": 0}))

        index = data.total_documents('newspaper')
        for article in articles:
            article['index'] = index
            index += 1

        result = collection.insert_many(articles)
        print(f'Copy {len(result.inserted_ids)} articles to original database')
        temp_collection.drop()
        
        return True


def update_new_articles(demo=False):
    print('\nStep 1: Crawl all new articles')
    if demo:
        demo_crawl_new_articles()
    else:
        crawl_new_articles()
    
    print('\nStep 2: Check for duplicated titles')
    check_duplicated_titles(0.6)

    print('\nStep 3: Update nndescent index')
    update_nndescent_index()
    
    print('\nStep 4: Update database')
    update_database()

    
if __name__ == '__main__':
    update_new_articles()

