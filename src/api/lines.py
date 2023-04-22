from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.params import Query

from enum import Enum
import Levenshtein

from src import database as db

router = APIRouter()


def process_id(x):
    try:
        x = int(x)
        return x
    except ValueError:  # String
        x = x.upper()
        for char in db.characters.values():
            if char.name == x:
                return char.id
        for i in range(1, 4):
            for char in db.characters.values():
                if Levenshtein.distance(x, char.name) <= i:
                    return char.id


@router.get("/lines/{id}", tags=["lines"])
def get_character_lines(id: str):
    """
    This endpoint returns a list of lines spoken by the character
    whose id OR name is given. If a string is given and no character
    names match exactly, it will find the closest character name that
    does exist in the dataset.

    For each line spoken by the character it returns:
    * `line_id`: the internal id of the line.
    * `conv_id`: the id of the conversation the line is a part of
    * `line_sort`: the index of where the line occurred in the conversation
    * `said_to`: the character that `id` said the line to.
    * `movie`: the name of the movie the line is from.
    * `line_text`: the text of the line

    The lines will be sorted by `line_id`.
    """

    id = process_id(id)

    character = db.characters.get(id)

    if character:
        out = []
        for line_id in character.line_ids:
            line = db.lines.get(line_id)
            conversation = db.conversations.get(line.conv_id)
            said_to_id = conversation.c1_id if conversation.c1_id != id else conversation.c2_id
            json = {
                "line_id": line_id,
                "conv_id": line.conv_id,
                "line_sort": line.line_sort,
                "said_to": db.characters.get(said_to_id).name,
                "movie": db.movies.get(line.movie_id).title,
                "line_text": line.line_text
            }
            out.append(json)
        return out

    raise HTTPException(status_code=404, detail="character not found.")


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
    lines = {}
    chars = list(db.characters.values())

    if sort == line_sort_options.name:
        chars = sorted(chars, key=lambda x: (x.name, x.id))
    elif sort == line_sort_options.movie:
        chars = sorted(chars, key=lambda x: (db.movies.get(x.movie_id).title, x.id))

    for character in chars:
        for line_id in character.line_ids:
            line = db.lines.get(line_id)
            if token in line.line_text:
                char_id = line.c_id
                if char_id in lines:
                    lines.get(char_id).get("lines_with_token").append(line.line_text)
                else:  # Initialize
                    lines[char_id] = {
                        "name": line.name,
                        "c_id": char_id,
                        "movie": line.movie,
                        "lines_with_token": [line.line_text]
                    }
                    if sort != line_sort_options.lines_with_token and (len(lines.values()) == limit or len(lines.values()) == len(list(db.characters.values()))):
                        return list(lines.values())

    if sort == line_sort_options.lines_with_token:
        lines = list(sorted(lines.values(), key=lambda x: (-len(x.get("lines_with_token")), x.get("c_id"))))
        return lines[:limit]

    return list(lines.values())


class lines_spoken_to_sort_options(str, Enum):
    name = "name"
    number_of_lines = "number_of_lines"


@router.get("/lines_spoken_to/", tags=["lines"])
def get_lines_spoken_to(
        id: str,
        sort: lines_spoken_to_sort_options = lines_spoken_to_sort_options.name):
    """
    This endpoint returns a list of the lines spoken to the character
    whose id OR name is given. If a string is given and no character
    names match exactly, it will find the closest character name that
    does exist in the dataset.

    For each character that has interacted with `id` it returns:
    * A list of lines that character has spoken to `id`

    You can also sort the results by using the `sort` query parameter:
    * `name` - Sort by character name alphabetically.
    * `number_of_lines` - Sort by number of lines the character has
    spoken to 'id', highest to lowest.
    """
    id = process_id(id)

    conversed = [conversation.id for conversation in db.conversations.values()
                 if conversation.c1_id == id or conversation.c2_id == id]
    dicts = []
    for conversation_id in conversed:
        lines = db.conversations_to_lines.get(conversation_id)
        for line_id in lines:
            line = db.lines.get(line_id)
            name = db.characters.get(line.c_id).name
            if line.c_id != id:
                found = False
                for dict in dicts:
                    if dict.get(name):
                        found = True
                        dict.get(name).append(line.line_text)
                        break
                if not found:
                    dicts.append({name: [line.line_text]})

    # Sort
    if sort == lines_spoken_to_sort_options.name:
        dicts = sorted(dicts, key=lambda x: (list(x.keys())[0], process_id(list(x.keys())[0])))
    elif sort == lines_spoken_to_sort_options.number_of_lines:
        dicts = sorted(dicts, key=lambda x: (-len(list(x.values())[0]), process_id(list(x.keys())[0])))

    return dicts
