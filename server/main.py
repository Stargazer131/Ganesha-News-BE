from typing import Annotated
from fastapi import FastAPI, Query, HTTPException
from pymongo import MongoClient
from server.model import Article, Category, ArticleRecommendation, ShortArticle, PyObjectId
from server.data import load_nndescent


app = FastAPI()
client = MongoClient("mongodb://localhost:27017")
db = client["Ganesha_News"]
nndescent = load_nndescent()


@app.get("/articles/", response_model=list[ShortArticle])
def get_articles_by_category(
    page: Annotated[int, Query(ge=1, le=20)] = 1,
    limit: Annotated[int, Query(ge=10, le=40)] = 20,
    category: Category = Category.latest
):
    query = {}
    fields = {"title": 1, "description": 1, "thumbnail": 1}
    sort_criteria = {"published_date": -1}
    if category != Category.latest:
        query = {"category": category}
    
    articles = db['newspaper'].find(query, fields).sort(sort_criteria).skip((page - 1) * limit).limit(limit)
    return [ShortArticle(**article) for article in articles]


@app.get("/articles/{article_id}", response_model=ArticleRecommendation)
def get_article_and_recommendations_by_id(
    article_id: PyObjectId, 
    limit: Annotated[int, Query(ge=5, le=20)] = 10,
):    
    article = db['newspaper'].find_one({"_id": article_id})
    if article is None:
        raise HTTPException(404, "Article not found")
    
    res_index = nndescent.neighbor_graph[0][article['index']]
    filter_index = [int(num) for num in res_index[1 : limit + 1]]
    query = {"index": {"$in": filter_index}}
    fields = {"title": 1, "description": 1, "thumbnail": 1}

    recommendation_list = db['newspaper'].find(query, fields)
    article = Article(**article)
    recommendations = [ShortArticle(**item) for item in recommendation_list]
    return ArticleRecommendation(article=article, recommendations=recommendations)


@app.get("/search/")
def get_articles_by_keyword(
    keyword: str,
    limit: Annotated[int, Query(ge=1, le=50)] = 30,
    page: Annotated[int, Query(ge=1, le=100)] = 1,
):
    title_query = {
        "$or": [
            { "title": { "$regex": f"\\s+{keyword}$", "$options": "i" } },
            { "title": { "$regex": f"^{keyword}\\s+", "$options": "i" } },
            { "title": { "$regex": f"\\s+{keyword}\\s+", "$options": "i" } }
        ]
    }
    description_query = {
        "title": { "$regex": f"\\s+{keyword}\\s+", "$options": "i" },
    }
    fields = {"title": 1, "description": 1, "thumbnail": 1}
    sort_criteria = {"published_date": -1}

    title_articles = list(db['newspaper'].find(title_query, fields).sort(sort_criteria))
    title_ids = [article["_id"] for article in title_articles]
    description_query["_id"] = {"$nin": title_ids}
    description_articles = list(db['newspaper'].find(description_query, fields).sort(sort_criteria))

    articles = title_articles + description_articles
    start_index = min(len(articles) - 1, (page - 1) * limit)
    end_index = min(len(articles), page * limit)

    return {
        "articles": [ShortArticle(**article) for article in articles[start_index : end_index]],
        "total": len(articles)
    }
