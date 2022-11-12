# Author: vugonz @ GitHub
# Python version 3.10.8
#
# Scuffed code, needs refactoring
import os

import requests # might be opted out, but single http request with aiohttp is ugly
import aiohttp
import asyncio

import datetime

import sqlite3
import bs4
import re

class Log:
    """ A is an entity describing a new notification posted to the webhook"""
    # TODO define a parser for a Log in a different class 
    def __init__(self, link, title, desc, date, author):
        self.title = str(title.string)
        self.link = re.sub("\/\?.*", "", link)
        desc = bs4.BeautifulSoup(desc.text, "lxml")
        self.desc = desc.find("p").text
        self.date = str(date.string)[:-4]
        self.author = str(author.string)

class Payload:
    """ Webhook JSON payload built from a Log """
    def __new__(self, username: str, log: Log, image: bs4.element.Tag):
        return {
            "username": username,
            "content": "", 
            "embeds": [{
                "title": f":newspaper: {log.title}", 
                "author": {
                    "name": log.author
                }, 
                "color": 16777215, 
                "url": log.link, 
                "description": log.desc,
                "image": {
                    "url": "" if image is None else image['src'],
                },
                "footer": {
                    "text": log.date 
                }
            }]
        }


def update_etag(etag: str, date: str) -> None:
    """ Update ETag if changed """
    with open("etag.txt", "w+") as f:
        f.write(f"{etag}\n{date}")

def init_db() -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    """ Establishes connection to DB and initializes logging Table if not yet initialized """
    conn = sqlite3.connect("bot.db")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS logs (link TEXT NOT NULL PRIMARY KEY, title TEXT, date TEXT, time TEXT)")
    conn.commit()
    return (conn, cursor)

def parse(content: requests.Response.content) -> list[Log]:
    """ Parse the contents of XML file and return a list of built Logs """
    soup = bs4.BeautifulSoup(content, "xml")

    entries = []
    for item in soup.find_all("item"):
        for link in item.find("link"):
            entries.append(Log(link, item.find("title"), item.find("description"), item.find("pubDate"), item.find("creator")))

    return entries

def is_log_in_db(log: Log, cursor: sqlite3.Cursor) -> bool:
    """ Check for Log in DB, INDEXED with link """
    cursor.execute("SELECT link FROM logs WHERE link=?;", (log.link,))
    return cursor.fetchall()

async def register_to_db(logs: list[Log], conn: sqlite3.Connection, cursor: sqlite3.Connection) -> None:
    """ Registers new Log to DB """
    for e in logs:
        cursor.execute("INSERT INTO logs VALUES(?, ?, ?, ?)", (e.link, e.title, datetime.date.today(), datetime.datetime.now().strftime("%H:%M:%S")))
    conn.commit()

async def post_to_hooks(url: str, log: Log, session: aiohttp.ClientSession) -> None:

    # try and fetch image with alt corresponding to the title
    r = await session.get(log.link)
    page_html = bs4.BeautifulSoup(await r.text(), "lxml")
    img = page_html.find(attrs={"alt": log.title})

    # build payload
    payload = Payload("Alerta CM", log, img)

    # POST request to endpoint
    r = await session.post(url, json=payload)

async def main():
    # Get last read ETag, if empty file, write *\n*
    with open("etag.txt", "a+") as f:
        if(not f.tell()):
            f.write("*\n*")
        f.seek(0, 0)
        etag = f.readline()[:-1]
        last_modified = f.readline()
    
    # Make conditional GET request to resource
    response = requests.get(os.environ.get("TARGET_URL"), headers={'If-None-Match': etag, 'If-Modified-Since': last_modified})

    # 304 response indicates no changes to resource
    if(response.status_code == 304):
        return
    
    # Update ETag value
    update_etag(response.headers['etag'], response.headers['last-modified'])
    # Establish DB Connection
    conn, cursor = init_db()
    # Parse XML file
    entries = parse(response.content)

    new_entries = []
    # Only keep entries that are not duplicate
    for log in entries:
        if not is_log_in_db(log, cursor):
            new_entries.append(log)
            
    # environment variable set as {code} HOOKS="value1"$"value2"$"value3" {code}
    webhooks = os.environ.get("HOOKS").split("$")

    # Run all Webhook post requests and DB writes asynchronously
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in webhooks:
            for log in new_entries:
                # If a big number of requests is made to the same endpoint (many new Logs), host might return 429 too many requests
                tasks.append(post_to_hooks(url, log, session))

        tasks.append(register_to_db(new_entries, conn, cursor))

        await asyncio.gather(*tasks)


asyncio.run(main())