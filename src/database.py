import csv
from src.datatypes import Character, Movie, Conversation, Line
import os
import io
from supabase import Client, create_client
import dotenv

# DO NOT CHANGE THIS TO BE HARDCODED. ONLY PULL FROM ENVIRONMENT VARIABLES.
dotenv.load_dotenv()
supabase_api_key = os.environ.get("SUPABASE_API_KEY")
supabase_url = os.environ.get("SUPABASE_URL")

if supabase_api_key is None or supabase_url is None:
    raise Exception(
        "You must set the SUPABASE_API_KEY and SUPABASE_URL environment variables."
    )

supabase: Client = create_client(supabase_url, supabase_api_key)

sess = supabase.auth.get_session()


def upload_new_log(file, logs_to_upload):
    """
    file: "conversations.csv", "lines.csv", or "movie_conversations_log.csv"
    logs_to_upload: list of dictionaries, where each dict is a log to upload
    """
    # If this function is called multiple times during different writes,
    # there is a potential for errors. The logs would be pulled down locally
    # in different environments, then edited and re-uploaded differently,
    # which could cause one of the writes to be ignored.

    file_csv = (
        supabase.storage.from_("movie-api")
            .download(file)
            .decode("utf-8")
    )
    logs = []
    for row in csv.DictReader(io.StringIO(file_csv), skipinitialspace=True):
        logs.append(row)
    for log in logs_to_upload:
        logs.append(log)
    output = io.StringIO()
    mapper = {
        "movie_conversations_log.csv": ["post_call_time", "movie_id_added_to"],
        "lines.csv": ["line_id", "character_id", "movie_id", "conversation_id", "line_sort", "line_text"],
        "conversations.csv": ["conversation_id", "character1_id", "character2_id", "movie_id"]
    }
    csv_writer = csv.DictWriter(
        output, fieldnames=mapper[file]
    )
    csv_writer.writeheader()
    csv_writer.writerows(logs)
    supabase.storage.from_("movie-api").upload(
        file,
        bytes(output.getvalue(), "utf-8"),
        {"x-upsert": "true"},
    )


def try_parse(type, val):
    try:
        return type(val)
    except ValueError:
        return None


movies_csv = (
    supabase.storage.from_("movie-api")
    .download("movies.csv")
    .decode("utf-8")
)
movies = {
    try_parse(int, row["movie_id"]): Movie(
        try_parse(int, row["movie_id"]),
        row["title"] or None,
        row["year"] or None,
        try_parse(float, row["imdb_rating"]),
        try_parse(int, row["imdb_votes"]),
        row["raw_script_url"] or None,
    )
    for row in csv.DictReader(io.StringIO(movies_csv), skipinitialspace=True)
    }

characters_csv = (
    supabase.storage.from_("movie-api")
    .download("characters.csv")
    .decode("utf-8")
)
characters = {}
for row in csv.DictReader(io.StringIO(characters_csv), skipinitialspace=True):
    char = Character(
        try_parse(int, row["character_id"]),
        row["name"] or None,
        try_parse(int, row["movie_id"]),
        row["gender"] or None,
        try_parse(int, row["age"]),
        0,
    )
    characters[char.id] = char

conversations_csv = (
    supabase.storage.from_("movie-api")
    .download("conversations.csv")
    .decode("utf-8")
)
conversations = {}
for row in csv.DictReader(io.StringIO(conversations_csv), skipinitialspace=True):
    conv = Conversation(
        try_parse(int, row["conversation_id"]),
        try_parse(int, row["character1_id"]),
        try_parse(int, row["character2_id"]),
        try_parse(int, row["movie_id"]),
        0,
    )
    conversations[conv.id] = conv

lines_csv = (
    supabase.storage.from_("movie-api")
    .download("lines.csv")
    .decode("utf-8")
)
lines = {}
for row in csv.DictReader(io.StringIO(lines_csv), skipinitialspace=True):
    line = Line(
        try_parse(int, row["line_id"]),
        try_parse(int, row["character_id"]),
        try_parse(int, row["movie_id"]),
        try_parse(int, row["conversation_id"]),
        try_parse(int, row["line_sort"]),
        row["line_text"],
    )
    lines[line.id] = line
    c = characters.get(line.c_id)
    if c:
        c.num_lines += 1

    conv = conversations.get(line.conv_id)
    if conv:
        conv.num_lines += 1
