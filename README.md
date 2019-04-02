# What
Crawl URLs given specific starting URL and collect valid and invalid links alongside with links to duplicate images.


# Installation

* make sure you have python3.7 in your environment
* run `pip install -r requirements.txt`


# How to run

```
./crawl.py --help
usage: crawl.py [-h] [--visited VISITED] [--invalid INVALID]
                [--dupimgs DUPIMGS] [--depth DEPTH]
                url

positional arguments:
  url                start web crawling from this URL

optional arguments:
  -h, --help         show this help message and exit
  --visited VISITED  CSV file to store visited links
  --invalid INVALID  CSV file to store invalid links
  --dupimgs DUPIMGS  CSV file to store links of duplicate images
  --depth DEPTH      Max link retrieval depth

```


# Examples

Start crawling from `http://guardicore.com` with the maximum depth of 1:
```
./crawl.py "http://guardicore.com" --depth 1
```

This will create 3 new CSV files:

```
$ head visited.csv
http://www.guardicore.com,0
https://www.guardicore.com/wp-content/uploads/2019/02/gc_home_partner_logos_azure_opt.png,1
https://www.guardicore.com/labs/research-academic/,1
https://www.guardicore.com/wp-content/uploads/2019/02/guardicore-logo-white-space.png,1

$ head invalid.csv
https://guardicore.allbound.com/,URLError.EXTERNAL_DOMAIN
https://threatintelligence.guardicore.com/,URLError.EXTERNAL_DOMAIN
https://www.youtube.com/watch?v=o3n1FO3Jyoo,URLError.EXTERNAL_DOMAIN

 $ head dupimgs.csv
URL,md5

```


The last file is empty, since there were no duplicate images found.