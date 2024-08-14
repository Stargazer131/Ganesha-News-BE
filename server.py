import random
from typing import Annotated
from fastapi import FastAPI, Query, HTTPException
from pymongo import MongoClient
from model import Article, Category, ArticleRecommendation, ShortArticle, PyObjectId


app = FastAPI()
client = MongoClient("mongodb://localhost:27017")
db = client["Ganesha_News"]



@app.get("/articles/", response_model=list[ShortArticle])
def get_articles_by_category(
    page: Annotated[int, Query(ge=1, le=20)] = 1,
    limit: Annotated[int, Query(ge=10, le=40)] = 20,
    category: Category = Category.latest
):
    filter = {}
    if category != Category.latest:
        filter = {"category": category}

    articles = db['newspaper_v2'].find(
        filter,
        {"title": 1, "description": 1, "thumbnail": 1}
    ).sort({"published_date": -1}).skip( (page - 1) * limit).limit(limit)

    return [ShortArticle(**article) for article in articles]


@app.get("/article/{article_id}", response_model=ArticleRecommendation)
def get_article_and_recommendations_by_id(article_id: PyObjectId):    
    article = db['newspaper_v2'].find_one({"_id": article_id})

    if article is None:
        raise HTTPException(404, "Article not found")

    total_docs = db['newspaper_v2'].count_documents({})
    index_list = [random.randint(0, total_docs - 1) for i in range(10)]
    recommendation_list = db['newspaper_v2'].find(
        {"index": {"$in": index_list}},
        {"title": 1, "description": 1, "thumbnail": 1}
    )

    article = Article(**article)
    recommendations = [ShortArticle(**item) for item in recommendation_list]

    return ArticleRecommendation(article=article, recommendations=recommendations)

