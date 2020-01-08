import _io
import asyncio
import datetime
import os
import sqlite3
import string
from typing import List, Set

import requests_async as requests


class TooManyRequests(Exception):
    """Too many requests"""


class DB:
    def __init__(self, filename: str = 'allo_hint.db'):
        self.file_db: str = filename
        self.db: sqlite3.Connection = self.connect()

    def connect(self) -> sqlite3.Connection:
        self.db = sqlite3.connect(self.file_db)
        return self.db

    def disconnect(self):
        self.db.close()

    def __fetchall(self, data: sqlite3.Cursor) -> List[str]:
        return [row[0] for row in data.fetchall()]

    def select(self, table: str) -> List[str]:
        query = f'select * from {table}'
        cursor = self.db.execute(query)

        return self.__fetchall(cursor)

    def insertmany(self, table: str, column: str, data: Set[str] or List[str]):
        param: List = [
            (value,) for value in data
        ]
        self.db.executemany(f"insert into {table}({column}) values (?)", param)
        self.db.commit()


class Hints:
    url: str = 'https://allo.ua/ua/catalogsearch/ajax/suggest/?currentTheme=main&currentLocale=uk_UA'
    alphabet: str = string.ascii_lowercase
    char_list: List = []
    hint_list: List = []
    hint_list_from_db: List = []
    c = 0

    def __init__(self, max_request=100):
        self.end: int = max_request
        self.file_chars_name: str = 'chars_file.txt'
        self.db_obj = DB()

    def file_reader(self, to_write: str = None) -> List[str]:
        data: List = []
        if to_write == '' or to_write:
            file: _io.TextIOWrapper = open(self.file_chars_name, 'w+')
            file.write(to_write)
            file.close()
        elif os.path.isfile(self.file_chars_name):
            file: _io.TextIOWrapper = open(self.file_chars_name)
            string_from_file = file.read()
            if string_from_file:
                data: List[str] = string_from_file.split(',')
            file.close()
        return data

    async def connect(self):
        self.hint_list_from_db: List[str] = self.db_obj.select('hints')
        char_list: List[str] = self.file_reader()

        if char_list:
            self.char_list: List[str] = char_list
        else:
            self.create_char_list()

    def create_char_list(self):
        self.char_list.extend(self.alphabet)
        self.char_list.extend(
            [
                chr_2 + chr_1
                for chr_2 in self.alphabet
                for chr_1 in self.alphabet
            ]
        )
        self.char_list.extend(
            [
                chr_3 + chr_2 + chr_1
                for chr_3 in self.alphabet
                for chr_2 in self.alphabet
                for chr_1 in self.alphabet
            ]
        )

    async def disconnect(self):
        self.tasks.cancelled()

        if self.hint_list_from_db:
            hint_set_from_db: Set[str] = set(self.hint_list_from_db)
            hint_set: Set[str] = set(self.hint_list)
            if len(hint_set_from_db) <= len(hint_set):
                data: Set[str] = hint_set_from_db.difference(hint_set)
            else:
                data: Set[str] = hint_set.difference(hint_set_from_db)
        else:
            data: Set[str] = set(self.hint_list)
        self.db_obj.insertmany('hints', 'hint', data)

        self.file_reader(','.join(self.char_list))

    async def req(self, char: str):
        post: requests.models.Response = await requests.post(
            self.url,
            {'q': char},
            timeout=3
        )
        if post.status_code == 429:
            raise TooManyRequests
        try:
            response: List[str] = (post.json()).get('query')
        except AttributeError:
            pass
        except TooManyRequests:
            print('Too Many Requests')
            self.char_list.append(char)
        else:
            if response:
                self.hint_list.extend(response)
        finally:
            self.char_list.remove(char)

    async def collect_tusks(self):
        self.tasks = asyncio.gather(
            *[
                self.req(char)
                for char in self.char_list[:self.end]
            ]
        )

        await asyncio.gather(self.tasks)

    async def run(self):
        await self.connect()
        while self.char_list:
            await self.collect_tusks()
            self.c += 1
            print(self.c)
        await self.disconnect()


if __name__ == '__main__':
    t = datetime.datetime.now()
    print("START")
    hint = Hints()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(hint.run())
    except KeyboardInterrupt:
        loop.run_until_complete(hint.disconnect())
    print("END")
    print('TIME', datetime.datetime.now() - t)
