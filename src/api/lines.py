from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.params import Query

from enum import Enum

from src import database as db
import sqlalchemy

from collections import OrderedDict

router = APIRouter()


@router.get("/lines/{id}", tags=["lines"])
def get_character_lines(id: int):
    """
    This endpoint returns a list of lines spoken by the character
    whose id is given.

    For each line spoken by the character it returns:
    * `line_id`: the internal id of the line.
    * `conv_id`: the id of the conversation the line is a part of
    * `line_sort`: the index of where the line occurred in the conversation
    * `said_to`: the character that `id` said the line to.
    * `movie`: the name of the movie the line is from.
    * `line_text`: the text of the line

    The lines will be sorted by `line_id`.
    """

    stmt = sqlalchemy.select(
        db.lines.c.line_id,
        db.lines.c.conversation_id,
        db.lines.c.line_sort,
        db.lines.c.movie_id,
        db.lines.c.line_text
    ).where(db.lines.c.character_id == id).order_by(db.lines.c.line_id)

    with db.engine.connect() as conn:
        line_info = conn.execute(stmt).fetchall()
        print(line_info)
        if len(line_info) == 0:
            raise HTTPException(status_code=404, detail="character not found or character has no lines.")
        movie = conn.execute(sqlalchemy.select(
            db.movies.c.title
        ).where(db.movies.c.movie_id == line_info[0].movie_id)).fetchone()

        json = []
        for line in line_info:
            said_to = conn.execute(sqlalchemy.select(
                db.conversations.c.character1_id,
                db.conversations.c.character2_id
            ).where(db.conversations.c.conversation_id == line.conversation_id)).fetchone()

            said_to_id = said_to.character1_id if said_to.character1_id != id else said_to.character2_id

            said_to_name = conn.execute(sqlalchemy.select(
                db.characters.c.name
            ).where(db.characters.c.character_id == said_to_id)).fetchone()

            json.append(
                {
                    "line_id": line.line_id,
                    "conv_id": line.conversation_id,
                    "line_sort": line.line_sort,
                    "said_to": said_to_name.name,
                    "movie": movie.title,
                    "line_text": line.line_text
                }
            )
    return json


class line_sort_options(str, Enum):
    name = "name"
    movie = "movie"
    lines_with_token = "lines_with_token"


@router.get("/lines/", tags=["lines"])
def list_characters_lines(
        token: str,
        limit: int = Query(50, ge=1, le=250),
        sort: line_sort_options = line_sort_options.name):
    """
    This endpoint returns a list of characters who have a line
    containing `token`.

    For each character that has spoken a line with `token` it returns:
    * `name` - The name of the character
    * `movie` - The name of the movie the character is from
    * `lines_with_token` - a list of lines spoken by the character that
    contain 'token`.

    You can also sort the results by using the `sort` query parameter:
    * `name` - Sort by character name alphabetically.
    * `movie` - Sort by movie title alphabetically.
    * `lines_with_token` - Sort by number of lines the character has
    containing `token`, highest to lowest. NOTE: you may experience a
    delay when sorting by lines_with_token, as the database needs to
    calculate lines_with_token for every character before it can sort.

    The `limit` query
    parameters are used for pagination. The `limit` query parameter specifies the
    maximum number of results to return.
    """
    token = token.lower()

    if sort is line_sort_options.name:
        order_by = db.characters.c.name
    elif sort is line_sort_options.movie:
        order_by = db.movies.c.title
    elif sort is line_sort_options.lines_with_token:
        pass
    else:
        assert False

    stmt = (
        sqlalchemy.select(
            sqlalchemy.func.max(db.characters.c.character_id).label("c_id"),
            db.characters.c.name,
            sqlalchemy.func.array_agg(db.lines.c.line_text).label("lines"),
            sqlalchemy.func.min(db.movies.c.title).label("movie"),
        )
            .select_from(
            db.lines.join(
                db.characters,
                db.characters.c.character_id == db.lines.c.character_id,
            ).join(
                db.movies,
                db.characters.c.movie_id == db.movies.c.movie_id,
            )
        )
            .where(db.lines.c.line_text.ilike(f"%{token}%"))
            .group_by(db.characters.c.name, db.movies.c.title)
    )

    if sort != line_sort_options.lines_with_token:
        stmt = stmt.order_by(order_by).limit(limit)

    with db.engine.connect() as conn:
        result = conn.execute(stmt)

        json = [
            {
                "name": row.name,
                "c_id": row.c_id,
                "movie": row.movie,
                "lines_with_token": [line for line in row.lines]
            }
            for row in result.fetchall()]

    if sort == line_sort_options.lines_with_token:
        json.sort(key=lambda x: (-len(x["lines_with_token"]), x["c_id"]))
        return json[:limit]

    return json


class lines_spoken_to_sort_options(str, Enum):
    name = "name"
    number_of_lines = "number_of_lines"


@router.get("/lines_spoken_to/", tags=["lines"])
def get_lines_spoken_to(
        id: int,
        sort: lines_spoken_to_sort_options = lines_spoken_to_sort_options.name):
    """
    This endpoint returns a list of the lines spoken to the character
    whose id is given.

    For each character that has interacted with `id` it returns:
    * A list of lines that character has spoken to `id`

    You can also sort the results by using the `sort` query parameter:
    * `name` - Sort by character name alphabetically.
    * `number_of_lines` - Sort by number of lines the character has
    spoken to 'id', highest to lowest.
    """

    stmt = sqlalchemy.select(
        db.lines.c.line_id,
        db.lines.c.character_id,
        db.lines.c.conversation_id,
        db.lines.c.line_text,
        db.characters.c.name,
    ).join(
        db.conversations,
        db.conversations.c.conversation_id == db.lines.c.conversation_id
    ).join(
        db.characters,
        db.characters.c.character_id == db.lines.c.character_id
    ).where(
        (db.conversations.c.character1_id == id) | (db.conversations.c.character2_id == id)
    ).where(
        db.characters.c.character_id.in_([db.conversations.c.character1_id, db.conversations.c.character2_id])
    )

    with db.engine.connect() as conn:
        result = conn.execute(stmt).fetchall()
        json = OrderedDict()
        for line in result:
            if line.name in json.keys():
                json[line.name].append(line.line_text)
            elif line.character_id != id:  # Initialize
                json[line.name] = [line.line_text]

    out = [{name: lines} for name, lines in json.items()]

    if sort == lines_spoken_to_sort_options.name:
        out.sort(key=lambda x: list(x.keys())[0])
    elif sort == lines_spoken_to_sort_options.number_of_lines:
        out.sort(key=lambda x: -len(list(x.values())[0]))

    return out
