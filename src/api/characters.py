from fastapi import APIRouter, HTTPException
from enum import Enum

from fastapi.params import Query
from src import database as db
import sqlalchemy


router = APIRouter()


def get_top_conv_characters(id, conn):
    all_convs = sqlalchemy.select(
        db.conversations.c.conversation_id,
        db.conversations.c.character1_id.label("c1_id"),
        db.conversations.c.character2_id.label("c2_id")
    ).where((db.conversations.c.character1_id == id)
            | (db.conversations.c.character2_id == id))
    all_convs = conn.execute(all_convs).fetchall()

    top_conversations = {}
    for conv in all_convs:

        other_id = conv.c1_id if conv.c1_id != id else conv.c2_id
        if other_id in top_conversations.keys():
            top_conversations[other_id]["number_of_lines_together"] += db.conv_to_num_lines.get(conv[0])
        else:  # Initialize
            other_info = sqlalchemy.select(
                db.characters.c.name,
                db.characters.c.gender
            ).where(db.characters.c.character_id == other_id)
            other_info = conn.execute(other_info).fetchone()
            top_conversations[other_id] = {
                "character_id": other_id,
                "character": other_info.name,
                "gender": other_info.gender,
                "number_of_lines_together": db.conv_to_num_lines.get(conv.conversation_id)
            }
    top_conversations = list(top_conversations.values())
    top_conversations.sort(key=lambda x: (-x["number_of_lines_together"], x["character_id"]))
    return top_conversations


@router.get("/characters/{id}", tags=["characters"])
def get_character(id: int):
    """
    This endpoint returns a single character by its identifier. For each character
    it returns:
    * `character_id`: the internal id of the character. Can be used to query the
      `/characters/{character_id}` endpoint.
    * `character`: The name of the character.
    * `movie`: The movie the character is from.
    * `gender`: The gender of the character.
    * `top_conversations`: A list of characters that the character has the most
      conversations with. The characters are listed in order of the number of
      lines together. These conversations are described below.

    Each conversation is represented by a dictionary with the following keys:
    * `character_id`: the internal id of the character.
    * `character`: The name of the character.
    * `gender`: The gender of the character.
    * `number_of_lines_together`: The number of lines the character has with the
      originally queried character.
    """

    character_info = sqlalchemy.select(
        db.characters.c.name,
        db.characters.c.movie_id,
        db.characters.c.gender,
    ).where(db.characters.c.character_id == id)

    with db.engine.connect() as conn:
        character_info = conn.execute(character_info).fetchone()
        if not character_info:
            raise HTTPException(status_code=404, detail="character not found.")
        movie_id = character_info.movie_id
        movie = conn.execute(sqlalchemy.select(
            db.movies.c.title
        ).where(db.movies.c.movie_id == movie_id)).fetchone().title
        top_conversation_info = get_top_conv_characters(id, conn)
        json = {
            "character_id": id,
            "character": character_info.name,
            "movie": movie,
            "gender": character_info.gender,
            "top_conversations": top_conversation_info
        }
    return json


class character_sort_options(str, Enum):
    character = "character"
    movie = "movie"
    number_of_lines = "number_of_lines"


@router.get("/characters/", tags=["characters"])
def list_characters(
    name: str = "",
    limit: int = Query(50, ge=1, le=250),
    offset: int = Query(0, ge=0),
    sort: character_sort_options = character_sort_options.character,
):
    """
    This endpoint returns a list of characters. For each character it returns:
    * `character_id`: the internal id of the character. Can be used to query the
      `/characters/{character_id}` endpoint.
    * `character`: The name of the character.
    * `movie`: The movie the character is from.
    * `number_of_lines`: The number of lines the character has in the movie.

    You can filter for characters whose name contains a string by using the
    `name` query parameter.

    You can also sort the results by using the `sort` query parameter:
    * `character` - Sort by character name alphabetically.
    * `movie` - Sort by movie title alphabetically.
    * `number_of_lines` - Sort by number of lines, highest to lowest.

    The `limit` and `offset` query
    parameters are used for pagination. The `limit` query parameter specifies the
    maximum number of results to return. The `offset` query parameter specifies the
    number of results to skip before returning results.
    """

    if sort is character_sort_options.character:
        order_by = db.characters.c.name
    elif sort is character_sort_options.movie:
        order_by = db.movies.c.title
    elif sort is character_sort_options.number_of_lines:
        whens = [
            (db.characters.c.character_id == id_, num_lines)
            for id_, num_lines in db.chars_to_num_lines.items()
        ]
        num_lines = sqlalchemy.case(*whens, else_=0).label('num_lines')
        order_by = sqlalchemy.desc(num_lines)
    else:
        assert False

    stmt = (
        sqlalchemy.select(
            db.characters.c.character_id,
            db.characters.c.name,
            db.characters.c.movie_id,
        )
            .select_from(
            db.characters.join(
                db.movies,
                db.characters.c.movie_id == db.movies.c.movie_id
            ))
            .limit(limit)
            .offset(offset)
            .order_by(order_by, db.characters.c.character_id)
    )

    # filter only if name parameter is passed
    if name != "":
        stmt = stmt.where(db.characters.c.name.ilike(f"%{name}%"))

    with db.engine.connect() as conn:
        result = conn.execute(stmt)
        json = []
        for row in result:
            movie = conn.execute(
                sqlalchemy.select(
                    db.movies.c.title
                ).where(db.movies.c.movie_id == row.movie_id)
            ).fetchone()

            json.append(
                {
                    "character_id": row.character_id,
                    "character": row.name,
                    "movie": movie.title,
                    "number_of_lines": db.chars_to_num_lines.get(row.character_id)
                }
            )

    return json

# print(list_characters(name="amy", limit=6, offset=0, sort=character_sort_options.number_of_lines))
