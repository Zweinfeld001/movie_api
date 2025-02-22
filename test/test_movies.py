from fastapi.testclient import TestClient

from src.api.server import app

import json

client = TestClient(app)

prefix = "/Users/zach/Desktop/CSC_365/Assignment4/"
prefix = ""


def test_get_movie():
    response = client.get("/movies/44")
    assert response.status_code == 200

    with open(prefix + "test/movies/44.json", encoding="utf-8") as f:
        assert response.json() == json.load(f)


def test_movies():
    response = client.get("/movies/")
    assert response.status_code == 200

    with open(prefix + "test/movies/root.json", encoding="utf-8") as f:
        assert response.json() == json.load(f)


def test_get_movie2():
    # tests null character in top characters
    response = client.get("/movies/436")
    assert response.status_code == 200

    with open(prefix + "test/movies/436.json", encoding="utf-8") as f:
        assert response.json() == json.load(f)


def test_sort_filter():
    response = client.get("/movies/?name=big&limit=50&offset=0&sort=rating")
    assert response.status_code == 200

    with open(
        prefix + "test/movies/movies-name=big&limit=50&offset=0&sort=rating.json",
        encoding="utf-8",
    ) as f:
        assert response.json() == json.load(f)


def test_404():
    response = client.get("/movies/1")
    assert response.status_code == 404
