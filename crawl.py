#!/usr/bin/env python3.7
import sys
import argparse
import hashlib
from collections import defaultdict
from traceback import format_exc
import multiprocessing as mp
import csv
from urllib.parse import urlparse, urlunparse
from pathlib import Path
import time
from enum import Enum

import requests
from bs4 import BeautifulSoup

MAX_DEPTH_LEVEL = 2


class URLError(Enum):
    EXTERNAL_DOMAIN = 1
    DOWNLOAD_ERROR = 2
    PARSE_ERROR = 3


class InvalidUrl(Exception):
    pass


def validate_url(url: str):
    """
    Make sure URL is valid
    :param url:
    :return:
    """
    try:
        return urlparse(url)
    except KeyboardInterrupt:
        return None


def parse_url(queue: mp.Queue, duplicate_images_urls, visited_urls,
              valid_urls_fd, invalid_urls_fd, ol: mp.Lock):
    """
    Main worker code
    :param queue: interprocess Queue
    :param duplicate_images_urls: interprocess dict to store duplicate images
    :param visited_urls: interprocess list to store visited urls
    :param valid_urls_fd: file descriptor for output
    :param invalid_urls_fd: file descriptor for output
    :param ol: lock to control output
    :return:
    """
    while True:
        invalid_urls = []
        valid_urls = []
        url, level = queue.get()

        # skip deep urls
        if level > MAX_DEPTH_LEVEL:
            queue.task_done()
            continue

        # skip visited urls
        if url in visited_urls:
            queue.task_done()
            continue

        try:
            with ol:
                sys.stdout.write(
                    "url=%r, depth=%r(%r), process=%r \n" % (url, level, MAX_DEPTH_LEVEL, mp.current_process()))
                sys.stdout.flush()

            url_parsed = validate_url(url)

            if url_parsed is None:
                invalid_urls.append((url, URLError.PARSE_ERROR))
                raise InvalidUrl

            try:
                resp = requests.get(url)
                resp.raise_for_status()
            except requests.exceptions.RequestException:
                invalid_urls.append((url, URLError.DOWNLOAD_ERROR))
                raise InvalidUrl

            # output this URL as visited
            with ol:
                vcsv = csv.writer(valid_urls_fd)
                vcsv.writerow([url, level])
                valid_urls_fd.flush()

            content_type = resp.headers.get("Content-Type")
            if content_type and "text/html" in content_type:
                soup = BeautifulSoup(resp.text, features="html.parser")
                links = [str(l.get('href')) for l in soup.find_all('a')]
                links.extend([str(s.get('src')) for s in soup.find_all('img')])

                for link in links:
                    p = validate_url(link)
                    if p is None:
                        invalid_urls.append((link, URLError.PARSE_ERROR))
                        continue
                    # resolve relative paths and supply netloc where missing
                    t = list(p[:])
                    t[0] = url_parsed.scheme if p.scheme == '' else p.scheme
                    t[1] = url_parsed.netloc if p.netloc == '' else p.netloc
                    if ".." in p.path:
                        tmp = Path("/{}/{}".format(url_parsed.path, p.path)).resolve()
                        t[2] = "" if tmp is None else str(tmp)
                    t[-1] = ""
                    new_link = urlunparse(t)
                    p = validate_url(new_link)
                    if p is None:  # restrict to the same domain
                        invalid_urls.append((new_link, URLError.PARSE_ERROR))
                        continue
                    if p.netloc != url_parsed.netloc:
                        invalid_urls.append((new_link, URLError.EXTERNAL_DOMAIN))
                        continue
                    valid_urls.append(new_link)

                # add links back to queue
                for u in set(valid_urls):
                    if u not in visited_urls:
                        queue.put((u, level + 1))

            elif content_type and content_type.startswith("image/"):
                # image content processing
                h = hashlib.md5(resp.content).hexdigest()
                duplicate_images_urls.append((h, url))

        except InvalidUrl:
            pass
        except Exception:
            with ol:
                sys.stderr.write("Error processing URL=%r, exc=%r \n" % (url, format_exc()))
                sys.stderr.flush()

        with ol:
            icsv = csv.writer(invalid_urls_fd)
            for (u, e) in invalid_urls:
                icsv.writerow([u, str(e)])
            invalid_urls_fd.flush()

        visited_urls.append(url)
        queue.task_done()


def output_dupimgs(duplicate_img_fd, duplicate_images_urls):
    """
    Outputs duplocate URLs to a file
    :param duplicate_img_fd: file handle
    :param duplicate_images_urls: list of tuples (hash, url)
    :return:
    """
    cs = csv.writer(duplicate_img_fd)
    cs.writerow(["URL", "md5"])
    dp_imgs = defaultdict(lambda: [])
    for (h, u) in duplicate_images_urls:
        dp_imgs[h].append(u)

    for h, urls in dp_imgs.items():
        if len(urls) > 1:
            for u in urls:
                cs.writerow([u, h])


class Crawler(object):
    """
    Crawler code
    """

    def __init__(self, starting_url, valid_urls_fn, invalid_urls_fn, duplicate_img_fn):

        self.manager = mp.Manager()
        self.queue = mp.JoinableQueue()
        self.output_lock = mp.Lock()
        self.duplicate_images_urls = self.manager.list()
        self.visited_urls = self.manager.list()
        self.valid_urls_fd = open(valid_urls_fn, 'w')
        self.invalid_urls_fd = open(invalid_urls_fn, 'w')
        self.duplicate_img_fd = open(duplicate_img_fn, 'w')
        self.workers = []
        for _ in range(mp.cpu_count()):
            p = mp.Process(
                target=parse_url,
                args=(
                    self.queue,
                    self.duplicate_images_urls,
                    self.visited_urls,
                    self.valid_urls_fd,
                    self.invalid_urls_fd,
                    self.output_lock
                )
            )
            p.start()
            self.workers.append(p)

        try:
            self.queue.put((starting_url, 0))
            self.queue.join()
        finally:
            output_dupimgs(self.duplicate_img_fd, self.duplicate_images_urls)
            self.manager.shutdown()
            sys.stdout.write("Shutting down...\n")
            for i, worker in enumerate(self.workers):
                try:
                    worker.terminate()
                except Exception:
                    sys.stdout.write("Unable to terminate worker %s !" % i)

            # close file descriptors
            self.valid_urls_fd.close()
            self.invalid_urls_fd.close()
            self.duplicate_img_fd.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("url", type=str, help="start web crawling from this URL")
    parser.add_argument("--visited", type=str, default="visited.csv", help="CSV file to store visited links")
    parser.add_argument("--invalid", type=str, default="invalid.csv", help="CSV file to store invalid links")
    parser.add_argument("--dupimgs", type=str, default="dupimgs.csv",
                        help="CSV file to store links of duplicate images")
    parser.add_argument("--depth", type=int, default=2, help="Max link retrieval depth")
    args = parser.parse_args()
    MAX_DEPTH_LEVEL = args.depth
    Crawler(
        starting_url=args.url,
        valid_urls_fn=args.visited,
        invalid_urls_fn=args.invalid,
        duplicate_img_fn=args.dupimgs
    )
