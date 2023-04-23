from fastapi.testclient import TestClient

from src.api.server import app

import json

client = TestClient(app)

prefix = "/Users/zach/Desktop/CSC_365/Assignment2/"
prefix = ""


def test_get_character_lines():
    response = client.get("/lines/7421")
    assert response.status_code == 200

    with open(prefix + "test/lines/7421.json", encoding="utf-8") as f:
        assert response.json() == json.load(f)


def test_list_characters_lines_num_sort():
    response = client.get("/lines/?token=hello&limit=50&sort=lines_with_token")
    assert response.status_code == 200

    with open(prefix +
              "test/lines/lines-token=hello&limit=50&sort=lines_with_token.json",
              encoding="utf-8") as f:
        assert response.json() == json.load(f)


def test_list_character_lines_name_sort():
    response = client.get("/lines/?token=hello&limit=50&sort=name")
    assert response.status_code == 200

    with open(prefix +
              "test/lines/lines-token=hello&limit=50&sort=name.json",
              encoding="utf-8") as f:
        assert response.json() == json.load(f)


def test_list_characters_lines_movie_sort():
    response = client.get("/lines/?token=hello&limit=50&sort=movie")
    assert response.status_code == 200

    with open(prefix +
              "test/lines/lines-token=hello&limit=50&sort=movie.json",
              encoding="utf-8") as f:
        assert response.json() == json.load(f)


def test_get_lines_spoken_to():
    response = client.get("/lines_spoken_to/?id=7423&sort=number_of_lines")
    assert response.status_code == 200

    with open(prefix +
              "test/lines/lines_spoken_to=id=7423&sort=number_of_lines.json",
              encoding="utf-8") as f:
        assert response.json() == json.load(f)


def test_404():
    response = client.get("/lines/400")
    assert response.status_code == 404
