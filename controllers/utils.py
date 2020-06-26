import time
import os
from bs4 import BeautifulSoup

def soup_from_url(driver, url):
    driver.get(url)
    time.sleep(5)
    content = driver.page_source
    return BeautifulSoup(content, features="lxml")

def save(filename, soup):
    filename = "soups/" + filename + ".html"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as file:
        file.write(str(soup))

def read_soup(filename):
    filename = "soups/"+filename+".html"
    with open(filename, "r") as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
        return soup