import time
import os
from bs4 import BeautifulSoup


def soup_from_url(driver, url):
    driver.get(url)
    time.sleep(5)
    content = driver.page_source
    return BeautifulSoup(content, features="lxml")


def read_soup(filename):
    with open(filename, "r") as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
        return soup