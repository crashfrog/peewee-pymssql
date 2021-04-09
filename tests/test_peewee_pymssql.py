#!/usr/bin/env python

"""Tests for `peewee_pymssql` package."""


import unittest
import logging

from peewee_pymssql import peewee_pymssql
from peewee_pymssql.peewee_pymssql import MssqlDatabase, MssqlMetadata
from playhouse.reflection import Introspector
from pwiz import print_models

class TestPeewee_pymssql(unittest.TestCase):
    """Tests for `peewee_pymssql` package."""

    def setUp(self):
        "Set log level"
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
        self.log = logging.getLogger(self.__class__.__name__)

    # def tearDown(self):
    #     """Tear down test fixtures, if any."""
    #     pass

    def test_monkey_patches(self):
        """Test patch to peewee.Introspector"""
        db = MssqlDatabase("", host="", user="", password="")
        intro = Introspector.from_database(db, 'dbo')
        self.assertIsInstance(intro, Introspector)
        self.assertIsInstance(intro.metadata, MssqlMetadata)