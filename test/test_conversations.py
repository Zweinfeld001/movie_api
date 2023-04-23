import unittest

from fastapi.testclient import TestClient

from src.api.server import app
from src.api.conversations import add_conversation, LinesJson, ConversationJson
from fastapi import HTTPException

import json

client = TestClient(app)


class TestAddConversation(unittest.TestCase):

    def test_add_conversation(self):
        conversation = ConversationJson(
            character_1_id=7421,
            character_2_id=7423,
            lines=[
                LinesJson(character_id=7421, line_text="Hi Miller"),
                LinesJson(character_id=7423, line_text="Hi there!"),
                LinesJson(character_id=7421, line_text="How are you doing today? This line is a test.")
            ]
        )

        add_conversation(502, conversation)
        response_7421 = client.get("/lines/7421").json()
        response_7423 = client.get("lines/7423").json()
        assert "Hi Miller" in [x.get("line_text") for x in response_7421]
        assert "Hi there!" in [x.get("line_text") for x in response_7423]
        assert "How are you doing today? This line is a test." in [x.get("line_text") for x in response_7421]

        # Check to see if `number_of_lines_together` updated from its original value of 25
        char_7421 = client.get("/characters/7421").json()
        assert char_7421.get("top_conversations")[0].get("number_of_lines_together") > 25

    def test_chars_not_found(self):
        conversation = ConversationJson(
            character_1_id=0,
            character_2_id=12,
            lines=[]
        )
        with self.assertRaises(HTTPException):
            add_conversation(0, conversation)

    def test_chars_same(self):
        conversation = ConversationJson(
            character_1_id=0,
            character_2_id=0,
            lines=[]
        )
        with self.assertRaises(HTTPException):
            add_conversation(0, conversation)

    def test_chars_from_different_movies(self):
        conversation = ConversationJson(
            character_1_id=0,
            character_2_id=49,
            lines=[]
        )
        with self.assertRaises(HTTPException):
            add_conversation(0, conversation)

    def test_both_chars_from_different_movie(self):
        conversation = ConversationJson(
            character_1_id=0,
            character_2_id=1,
            lines=[]
        )
        with self.assertRaises(HTTPException):
            add_conversation(3, conversation)
