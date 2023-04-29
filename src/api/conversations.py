from fastapi import APIRouter, HTTPException
from src import database as db
from pydantic import BaseModel
from typing import List
from datetime import datetime
from src.datatypes import Conversation, Line
import sqlalchemy


# FastAPI is inferring what the request body should look like
# based on the following two classes.
class LinesJson(BaseModel):
    character_id: int
    line_text: str


class ConversationJson(BaseModel):
    character_1_id: int
    character_2_id: int
    lines: List[LinesJson]


router = APIRouter()


@router.post("/movies/{movie_id}/conversations/", tags=["movies"])
def add_conversation(movie_id: int, conversation: ConversationJson):
    """
    This endpoint adds a conversation to a movie. The conversation is represented
    by the two characters involved in the conversation and a series of lines between
    those characters in the movie.

    The endpoint ensures that all characters are part of the referenced movie,
    that the characters are not the same, and that the lines of a conversation
    match the characters involved in the conversation.

    Line sort is set based on the order in which the lines are provided in the
    request body.

    The endpoint returns the id of the resulting conversation that was created.
    """

    c1_id = conversation.character_1_id
    c2_id = conversation.character_2_id

    if c1_id == c2_id:
        raise HTTPException(status_code=400, detail="character ids are the same.")

    stmt = sqlalchemy.select(
        db.characters.c.movie_id
    ).where((db.characters.c.character_id == c1_id) | (db.characters.c.character_id == c2_id))

    with db.engine.connect() as conn:
        result = [row.movie_id for row in conn.execute(stmt).fetchall()]

        if len(result) < 2:
            raise HTTPException(status_code=404, detail="character(s) not found.")

        if result[0] != result[1]:
            raise HTTPException(status_code=400, detail="characters are not from the same movie.")

        if result[0] != movie_id:
            raise HTTPException(status_code=400, detail="character(s) are not from the movie provided in movie_id.")

        conv_id = conn.execute(
            sqlalchemy.select(
                db.conversations.c.conversation_id
            )
            .order_by(sqlalchemy.desc(db.conversations.c.conversation_id))
            .limit(1)
        ).fetchone().conversation_id + 1

        conv_to_upload = {
            "conversation_id": conv_id,
            "character1_id": c1_id,
            "character2_id": c2_id,
            "movie_id": movie_id
        }

        conn.execute(db.conversations.insert().values(**conv_to_upload))
        print(conv_to_upload)

        current_line_id = conn.execute(
            sqlalchemy.select(
                db.lines.c.line_id
            )
            .order_by(sqlalchemy.desc(db.lines.c.line_id))
            .limit(1)
        ).fetchone().line_id + 1

        current_line_sort = 1
        lines = conversation.lines

        c1_lines = 0
        c2_lines = 0
        for line in lines:
            line_to_upload = {
                "line_id": current_line_id,
                "character_id": line.character_id,
                "movie_id": movie_id,
                "conversation_id": conv_id,
                "line_sort": current_line_sort,
                "line_text": line.line_text
            }
            conn.execute(db.lines.insert().values(**line_to_upload))
            current_line_id += 1
            current_line_sort += 1

            if line.character_id == c1_id:
                c1_lines += 1
            elif line.character_id == c2_id:
                c2_lines += 1

        db.chars_to_num_lines[c1_id] += c1_lines
        db.chars_to_num_lines[c2_id] += c2_lines
        db.conv_to_num_lines[conv_id] = c1_lines + c2_lines

        conn.commit()

    return conv_id


    # c1_id = conversation.character_1_id
    # c2_id = conversation.character_2_id
    #
    # if not(db.characters.get(c1_id) and db.characters.get(c2_id)):
    #     raise HTTPException(status_code=404, detail="character(s) not found.")
    #
    # if c1_id == c2_id:
    #     raise HTTPException(status_code=400, detail="character ids are the same.")
    #
    # c1_movie_id = db.characters.get(c1_id).movie_id
    # c2_movie_id = db.characters.get(c2_id).movie_id
    #
    # if c1_movie_id != c2_movie_id:
    #     raise HTTPException(status_code=400, detail="characters are not from the same movie.")
    #
    # if c1_movie_id != movie_id:
    #     raise HTTPException(status_code=400, detail="character(s) are not from the movie provided in movie_id.")
    #
    # info_to_upload = {"post_call_time": datetime.now(), "movie_id_added_to": movie_id}
    # db.upload_new_log("movie_conversations_log.csv", [info_to_upload])
    #
    # conv_id = list(db.conversations.keys())[-1] + 1
    # conv_to_upload = {
    #     "conversation_id": conv_id,
    #     "character1_id": c1_id,
    #     "character2_id": c2_id,
    #     "movie_id": movie_id
    # }
    # db.upload_new_log("conversations.csv", [conv_to_upload])
    #
    # current_line_id = list(db.lines.keys())[-1] + 1
    # current_line_sort = 1
    # lines = conversation.lines
    # lines_to_upload = []
    #
    # c1_lines = 0
    # c2_lines = 0
    # for line in lines:
    #     line_info = {
    #         "line_id": current_line_id,
    #         "character_id": line.character_id,
    #         "movie_id": movie_id,
    #         "conversation_id": conv_id,
    #         "line_sort": current_line_sort,
    #         "line_text": line.line_text
    #     }
    #     lines_to_upload.append(line_info)
    #     current_line_id += 1
    #     current_line_sort += 1
    #
    #     if line.character_id == c1_id:
    #         c1_lines += 1
    #     elif line.character_id == c2_id:
    #         c2_lines += 1
    #
    #     new_line = Line(
    #         id=current_line_id,
    #         c_id=line.character_id,
    #         movie_id=movie_id,
    #         conv_id=conv_id,
    #         line_sort=current_line_sort,
    #         line_text=line.line_text)
    #
    #     db.lines[current_line_id] = new_line
    # # If multiple calls were made at the same time, this could cause a
    # # problem, especially if a user tried a read operation simultaneously.
    #
    # db.upload_new_log("lines.csv", lines_to_upload)
    #
    # db.characters.get(c1_id).num_lines += c1_lines
    # db.characters.get(c2_id).num_lines += c2_lines
    #
    # conversation = Conversation(id=conv_id, c1_id=c1_id, c2_id=c2_id, movie_id=movie_id, num_lines=len(lines_to_upload))
    # db.conversations[conv_id] = conversation
    #
    # return conv_id


# conversation = ConversationJson(
#     character_1_id=0,
#     character_2_id=1,
#     lines=[
#         LinesJson(character_id=0, line_text="Hello"),
#         LinesJson(character_id=1, line_text="Hi there!"),
#         # LinesJson(character_id=0, line_text="How are you?"),
#         # LinesJson(character_id=1, line_text="I'm doing well, thanks. How about you?"),
#         # LinesJson(character_id=0, line_text="I'm good too, thanks for asking.")
#     ]
# )
#
# print(add_conversation(0, conversation))
