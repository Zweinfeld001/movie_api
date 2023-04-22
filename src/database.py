import csv
from src.datatypes import Character, Movie, Conversation, Line
import ast

prefix = "/Users/zach/Desktop/CSC_365/Assignment2/"
prefix = ""


def try_parse(type, val):
    try:
        return type(val)
    except ValueError:
        return None


with open(prefix + "movies.csv", mode="r", encoding="utf8") as csv_file:
    movies = {
        try_parse(int, row["movie_id"]): Movie(
            try_parse(int, row["movie_id"]),
            row["title"] or None,
            row["year"] or None,
            try_parse(float, row["imdb_rating"]),
            try_parse(int, row["imdb_votes"]),
            row["raw_script_url"] or None,
        )
        for row in csv.DictReader(csv_file, skipinitialspace=True)
    }

with open(prefix + "characters.csv", mode="r", encoding="utf8") as csv_file:
    characters = {}
    for row in csv.DictReader(csv_file, skipinitialspace=True):
        char = Character(
            try_parse(int, row["character_id"]),
            row["name"] or None,
            try_parse(int, row["movie_id"]),
            row["gender"] or None,
            try_parse(int, row["age"]),
            ast.literal_eval(row["line_ids"]),
            0,
        )
        characters[char.id] = char

with open(prefix + "conversations.csv", mode="r", encoding="utf8") as csv_file:
    conversations = {}
    for row in csv.DictReader(csv_file, skipinitialspace=True):
        conv = Conversation(
            try_parse(int, row["conversation_id"]),
            try_parse(int, row["character1_id"]),
            try_parse(int, row["character2_id"]),
            try_parse(int, row["movie_id"]),
            0,
        )
        conversations[conv.id] = conv

with open(prefix + "lines.csv", mode="r", encoding="utf8") as csv_file:
    lines = {}
    for row in csv.DictReader(csv_file, skipinitialspace=True):
        line = Line(
            try_parse(int, row["line_id"]),
            try_parse(int, row["character_id"]),
            try_parse(int, row["movie_id"]),
            try_parse(int, row["conversation_id"]),
            try_parse(int, row["line_sort"]),
            row["line_text"],
            row["name"],
            row["movie"]
        )
        lines[line.id] = line
        c = characters.get(line.c_id)
        if c:
            c.num_lines += 1

        conv = conversations.get(line.conv_id)
        if conv:
            conv.num_lines += 1

with open(prefix + "conversations_to_lines.csv", mode="r", encoding="utf8") as csv_file:
    conversations_to_lines = {}
    for row in csv.DictReader(csv_file, skipinitialspace=True):
        conversations_to_lines[int(row["conversation_id"])] = ast.literal_eval(row["line_ids"])
