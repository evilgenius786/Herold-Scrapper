# -*- coding: utf8 -*-

import csv
import json
import os
import threading
import time
import traceback

import cfscrape
from bs4 import BeautifulSoup

semaphore = threading.Semaphore(1)
write = threading.Semaphore(1)
outcsv = "Out-Herold.csv"
errorfile = "Error-Herold.txt"
incsv = "Input-Harold.csv"
encoding = "utf8"
headers = ["Company", "Street", "Zip", "City", "Telephone", "Email", "Website", "Keyword", "URL"]
scraped = []


def scrape(url, kw=""):
    with semaphore:
        try:
            print("Scraping", url)
            soup = get(url)
            addr = getText(soup, 'div', 'class', "address")
            data = {
                "Company": getText(soup, 'h1', 'itemprop', "name"),
                "Street": addr.split(",")[0].strip(),
                "Zip": addr.split(", ")[1].split(' ')[0].strip(),
                "City": " ".join(addr.split(", ")[1].split(' ')[1:]),
                "Telephone": getHref(soup, 'a', "data-category", "Telefonnummer").replace("tel:", ""),
                "Email": getHref(soup, 'a', "data-category", "E-Mail").replace("mailto:", ""),
                "Website": getHref(soup, 'a', "data-category", "Weblink"),
                "Keyword": kw,
                "URL": url
            }
            print(json.dumps(data, indent=4))
            append(data)
            scraped.append(url)
        except:
            print("Error on", url)
            with open(errorfile, 'a', encoding=encoding) as efile:
                efile.write(url + "\n")
            traceback.print_exc()


def main():
    # scrape('https://www.herold.at/gelbe-seiten/wien/6DgsB/glaserei-harald-lackinger-eu/', "")
    # return
    global semaphore, scraped
    logo()
    if not os.path.isfile(outcsv):
        with open(outcsv, 'w', newline='', encoding=encoding) as outfile:
            csv.DictWriter(outfile, fieldnames=headers).writeheader()
    if not os.path.isfile(incsv):
        with open(incsv, 'w', newline='', encoding=encoding) as infile:
            csv.writer(infile).writerow(
                ["Wintergärten", "Steiermark", "Wien", "Burgenland", "Kärnten", "Niederösterreich", "Oberösterreich",
                 "Salzburg", "Tirol", "Vorarlberg"])
    with open(outcsv, encoding=encoding, errors='ignore') as ofile:
        for line in csv.DictReader(ofile):
            scraped.append(line['URL'])
    print("Already scraped listings", scraped)
    print("Loading data...")
    kws = []
    dists = []
    with open(incsv, encoding=encoding, errors='ignore') as infile:
        for row in csv.reader(infile):
            kws.append(row[0])
            dists.append(row[1:])
    threads = []
    for kw in kws:
        for dist in dists:
            for d in dist:
                print(kw, d)
                url = f"https://www.herold.at/gelbe-seiten/{d}/was_{kw}/"
                print("Working on", url)
                home_soup = get(url)
                for section in home_soup.find_all('section', {"data-detail-url": True}):
                    t = threading.Thread(target=scrape, args=(section['data-detail-url'], kw,))
                    t.start()
                    threads.append(t)
                if len(home_soup.find_all('a', {"class": "page-link"})) > 0:
                    for a in home_soup.find_all('a', {"class": "page-link"})[:-1]:
                        url = a['href']
                        print("Working on", url)
                        soup = get(url)
                        for section in soup.find_all('section', {"data-detail-url": True}):
                            t = threading.Thread(target=scrape, args=(section['data-detail-url'],))
                            t.start()
                            threads.append(t)
    for thread in threads:
        thread.join()
    print("Done with scraping!")


def append(data):
    with write:
        with open(outcsv, 'a', newline="", encoding=encoding, errors='ignore') as outfile:
            csv.DictWriter(outfile, fieldnames=headers).writerow(data)
        if data['URL'] not in scraped:
            exist = os.path.isfile(outcsv.replace(".csv", f"-{data['Keyword']}.csv"))
            with open(outcsv.replace(".csv", f"-{data['Keyword']}.csv"), 'a', newline="", encoding=encoding,
                      errors='ignore') as outfile:
                c = csv.DictWriter(outfile, fieldnames=headers)
                if not exist:
                    c.writeheader()
                c.writerow(data)


def getHref(soup, tag, atttrib, Class):
    try:
        return soup.find(tag, {atttrib: Class})['href']
    except:
        return ""


def getText(soup, tag, atttrib, Class):
    try:
        return soup.find(tag, {atttrib: Class}).text.strip()
    except:
        return ""


def get(url):
    return BeautifulSoup(cfscrape.create_scraper().get(url).text, 'lxml')


def logo():
    os.system('color 0a')
    print(rf"""
        __________                   .__          
        \____    /____   ____ ______ |  | _____   
          /     //  _ \ /  _ \\____ \|  | \__  \  
         /     /(  <_> |  <_> )  |_> >  |__/ __ \_
        /_______ \____/ \____/|   __/|____(____  /
                \/            |__|             \/ 
===========================================================
      zoopla.co.uk (UK) scraper by github.com/evilgenius786
===========================================================
[+] Resumable
[+] Multithreaded
[+] Without browser
[+] CSV and JSON output
[+] Exception Handling
[+] Super fast and efficient
[+] Log/error/progress reporting
___________________________________________________________
Error file: {errorfile}
Output CSV file: {outcsv}
Output JSON dir: ./JSON
___________________________________________________________
""")


if __name__ == '__main__':
    main()
