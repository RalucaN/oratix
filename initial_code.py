

# Importing libraries

from bs4 import BeautifulSoup
import pandas as pd
import requests
import urllib.request
import json
import time
import os

# User Inputs

topic='machine learning' # Select a topic from the list available here: https://www.ted.com/topics

filename=f"slug/TED_Talk_{topic}_URLs.txt"


# Version 1 - using api request

def api_scraping(topic, filename):
    urls = []
    page_number=0
    total_pages=0
    f = open(filename, 'w')
    
    while True:
        # The API endpoint to request
        endpoint = "https://zenith-prod-alt.ted.com/api/search"

        # The headers for the request
        headers = {
        "Accept": "*/*",
        "Content-Type": "application/json"
      }

      # The data for the request
        data = [{
              "indexName": "coyote_models_acme_videos_alias_21e1372f285984be956cd03b7ad3406e",
              "params": {
                  "attributeForDistinct": "objectID",
                  "distinct": 1,
                  "facetFilters": [
                      [
                          "tags:"+topic
                      ]
                  ],
                  "facets": [
                      "subtitle_languages",
                      "tags"
                  ],
                  "highlightPostTag": "__/ais-highlight__",
                  "highlightPreTag": "__ais-highlight__",
                  "hitsPerPage": 24,
                  "maxValuesPerFacet": 500,
                  "page": page_number,
                  "query": "",
                  "tagFilters": ""
              }
          },
          {
              "indexName": "coyote_models_acme_videos_alias_21e1372f285984be956cd03b7ad3406e",
              "params": {
                  "analytics": False,
                  "attributeForDistinct": "objectID",
                  "clickAnalytics": False,
                  "distinct": 1,
                  "facets": "tags",
                  "highlightPostTag": "__/ais-highlight__",
                  "highlightPreTag": "__ais-highlight__",
                  "hitsPerPage": 0,
                  "maxValuesPerFacet": 500,
                  "page": page_number,
                  "query": ""
              }
          }]



        # Send the POST request and get the response
        response = requests.post(endpoint, headers=headers, data=json.dumps(data))
        if page_number==0:
            total_pages=response.json()['results'][0]['nbPages']

        for slug in response.json()['results'][0]['hits']:
            urls.append(slug['slug'])
        print(f"{page_number} / {total_pages}")
        page_number+=1
        if page_number>total_pages:
            break
    f.write('\n'.join(urls))
    f.close()

    print(f"Done.{len(urls)} URLs for topic {topic} have been saved in {f}.")
    return



api_scraping(topic,filename)


# function to get id

def getBuildID():
    response=requests.get("https://ted.com")
    buildID=str(response.content).split("buildId\":\"")[1].split("\"")[0]
    return buildID

# function to build url

def buildDataURL(slug):
    daily_id=getBuildID()
    base=f"https://www.ted.com/_next/data/{daily_id}/talks/"
    mid=".json?slug="
    url=base+slug+mid+slug
    return url

# function to get slug data

def getSlugData(url):
    response=requests.get(url)
    try:
        return response.json()
    except ValueError:
        print(url)
        print(response)
        issues[url]=response

# function to export json data for a list of slugs

def extract_json_by_slug(path_to_json,slugs, retry_limit=3):
    count=0
    max_count=10

    data_list=[]
    slugs_retry={}

    for i, slug in enumerate(slugs):
        count+=1
        slugURL = buildDataURL(slug)
        data_json = getSlugData(slugURL)

        if data_json is None: #checking if the respose is null
            if slug in slugs_retry:
                slugs_retry[slug] += 1
            else:
                slugs_retry[slug] = 1
        else:
            json_object = json.dumps(data_json)
            file_slug=slug if len(slug)<100 else slug[:101]
            
            with open(f"{path_to_json}{file_slug}.json", "w") as outfile:
                outfile.write(json_object)

        # adding throttling/wait time to stay within rate rate limiting parameters     
        if count>max_count:
            time.sleep(10)
            count=0
        else:
            time.sleep(3)
        
        # Print progress for every 100 slugs processed
        if (i+1) % 100 == 0:
            print(f"Processed {i+1} out of {len(slugs)} slugs.")
    
    # Retry slugs that failed but haven't reached the retry limit
    slugs_to_retry = [slug for slug, retries in slugs_retry.items() if retries < retry_limit]
    
    print(f"There are {len(slugs_to_retry)} slugs that still return None after retries.")
    
    if len(slugs_to_retry)!=0:
        extract_json_by_slug(slugs_to_retry, retry_limit)
        
    if len(issues) > 0:
        print(issues)
    return slugs_to_retry,issues


# Data quality check: function to check for null jsons and missing transcripts
missing=[]
nulls=[]

def check_null_missing_transcript(path_to_json):
    count_missing=0
    count_null=0
    
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    for jfile in json_files:
        with open(os.path.join(path_to_json, jfile)) as json_file:
            json_text = json.load(json_file)
            if json_text is None:
                count_null+=1
                nulls.append(jfile)
            elif json_text["pageProps"]["transcriptData"]["translation"] is None:
                count_missing+=1
                missing.append(jfile)
    print("Summary:")        
    print(f"There are {count_null} null jsons files")
    print(f"There are {count_missing} jsons files with missing transcripts")

# define which slugs to be loaded and where to save json files
path_to_json = 'jsons/'
path_to_slug = 'slug/'
slug_files = [pos_slug for pos_slug in os.listdir(path_to_slug) if pos_slug.endswith('.txt')]

slugs_combined=[]

for sfile in slug_files:
    print(sfile)
    with open(os.path.join(path_to_slug, sfile)) as slug_file:
        loaded=slug_file.read().splitlines()
        print(f"{len(loaded)} slugs found")
        slugs_combined.append(loaded)

# combining all slugs and removing duplicates        
slugs = list(set([item for sublist in slugs_combined for item in sublist]))

issues={}
extract_json_by_slug(path_to_json,slugs)



check_null_missing_transcript(path_to_json)

