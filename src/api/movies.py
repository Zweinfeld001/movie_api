from fastapi import APIRouter, HTTPException
from enum import Enum
from src import database as db
from fastapi.params import Query
import sqlalchemy

router = APIRouter()


@router.get("/movies/{movie_id}", tags=["movies"])
def get_movie(movie_id: int):
    """
    This endpoint returns a single movie by its identifier. For each movie it returns:
    * `movie_id`: the internal id of the movie.
    * `title`: The title of the movie.
    * `top_characters`: A list of characters that are in the movie. The characters
      are ordered by the number of lines they have in the movie. The top five
      characters are listed.

    Each character is represented by a dictionary with the following keys:
    * `character_id`: the internal id of the character.
    * `character`: The name of the character.
    * `num_lines`: The number of lines the character has in the movie.

    """
    movie_info = sqlalchemy.select(
        db.movies.c.title
    ).where(db.movies.c.movie_id == movie_id)
    character_info = sqlalchemy.select(
        db.characters.c.character_id,
        db.characters.c.name,
        db.characters.c.num_lines,
    ).select_from(db.movies.join(db.characters)).where(db.movies.c.movie_id == movie_id)\
        .limit(5)\
        .order_by(sqlalchemy.desc(db.characters.c.num_lines), db.characters.c.character_id)

    with db.engine.connect() as conn:
        movie_info = conn.execute(movie_info).fetchone()
        if not movie_info:
            raise HTTPException(status_code=404, detail="movie not found.")
        character_info = conn.execute(character_info)

        top_characters = [
            {"character_id": row.character_id,
             "character": row.name,
             "num_lines": row.num_lines}
            for row in character_info]

        json = {
            "movie_id": movie_id,
            "title": movie_info.title,
            "top_characters": top_characters
        }
    return json

# print(get_movie(0))


class movie_sort_options(str, Enum):
    movie_title = "movie_title"
    year = "year"
    rating = "rating"


@router.get("/movies/", tags=["movies"])
def list_movies(
    name: str = "",
    limit: int = Query(50, ge=1, le=250),
    offset: int = Query(0, ge=0),
    sort: movie_sort_options = movie_sort_options.movie_title,
):
    """
    This endpoint returns a list of movies. For each movie it returns:
    * `movie_id`: the internal id of the movie. Can be used to query the
      `/movies/{movie_id}` endpoint.
    * `movie_title`: The title of the movie.
    * `year`: The year the movie was released.
    * `imdb_rating`: The IMDB rating of the movie.
    * `imdb_votes`: The number of IMDB votes for the movie.

    You can filter for movies whose titles contain a string by using the
    `name` query parameter.

    You can also sort the results by using the `sort` query parameter:
    * `movie_title` - Sort by movie title alphabetically.
    * `year` - Sort by year of release, earliest to latest.
    * `rating` - Sort by rating, highest to lowest.

    The `limit` and `offset` query
    parameters are used for pagination. The `limit` query parameter specifies the
    maximum number of results to return. The `offset` query parameter specifies the
    number of results to skip before returning results.
    """
    if sort is movie_sort_options.movie_title:
        order_by = db.movies.c.title
    elif sort is movie_sort_options.year:
        order_by = db.movies.c.year
    elif sort is movie_sort_options.rating:
        order_by = sqlalchemy.desc(db.movies.c.imdb_rating)
    else:
        assert False

    stmt = (
        sqlalchemy.select(
            db.movies.c.movie_id,
            db.movies.c.title,
            db.movies.c.year,
            db.movies.c.imdb_rating,
            db.movies.c.imdb_votes,
        )
            .limit(limit)
            .offset(offset)
            .order_by(order_by, db.movies.c.movie_id)
    )

    # filter only if name parameter is passed
    if name != "":
        stmt = stmt.where(db.movies.c.title.ilike(f"%{name}%"))

    with db.engine.connect() as conn:
        result = conn.execute(stmt)
        json = []
        for row in result:
            json.append(
                {
                    "movie_id": row.movie_id,
                    "movie_title": row.title,
                    "year": row.year,
                    "imdb_rating": row.imdb_rating,
                    "imdb_votes": row.imdb_votes,
                }
            )

    return json


# print(list_movies("", limit=50, offset=0, sort=movie_sort_options.movie_title))
