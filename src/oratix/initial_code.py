#//////////////////////////////////////////////////////////////////////////////
#------------------------------------------------------------------------------
#                                                                             #
#             ____  _____         _______ _______   __                        #
#            / __ \|  __ \     /\|__   __|_   _\ \ / /                        #
#           | |  | | |__) |   /  \  | |    | |  \ V /                         #
#           | |  | |  _  /   / /\ \ | |    | |   > <                          #
#           | |__| | | \ \  / ____ \| |   _| |_ / . \                         #
#            \____/|_|  \_\/_/    \_\_|  |_____/_/ \_\                        #
#                                                                             #
#------------------------------------------------------------------------------
#\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
# Importing libraries
import requests
import json
import time
import os

# =============================================================================
# | Configs
# =============================================================================

host = "https://ted.com"
path_to_json = 'jsons/'
path_to_slug = 'slug/'

issues = {}
missing = []
nulls = []
slugs_combined = []

searchApiRequestBody=[{
              "indexName": "coyote_models_acme_videos_alias_21e1372f285984be956cd03b7ad3406e",  # TODO: THIS MIGHT NEED RETRIEVING DYNAMICALLY
              "params": {
                  "attributeForDistinct": "objectID",
                  "distinct": 1,
                  "facetFilters": [
                      [
                          "tags: TOPIC_PLACEHOLDER"
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
                  "page": 0,
                  "query": "",
                  "tagFilters": ""
              }
          },
          {
              "indexName": "coyote_models_acme_videos_alias_21e1372f285984be956cd03b7ad3406e",  # TODO: THIS MIGHT NEED RETRIEVING DYNAMICALLY
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
                  "page": 0,
                  "query": ""
              }
          }]

searchApiHeaders = {
            "Accept": "*/*",
            "Content-Type": "application/json"
        }

searchApiEndpoint = "https://zenith-prod-alt.ted.com/api/search"

# =============================================================================
# | GET/SET methods
# =============================================================================


def getSearchApiEndpoint():
    return searchApiEndpoint


def setSearchApiEndpoint(newEndpoint):
    global searchApiEndpoint
    searchApiEndpoint = newEndpoint


def getSearchApiBody():
    return searchApiRequestBody


def setSearchApiBody(requestBody):
    global searchApiRequestBody
    searchApiRequestBody = requestBody


def loadSearchRequestBodyFromFile(requestBodyFile):
    global searchApiRequestBody
    searchApiRequestBody = json.load(requestBodyFile)


def saveSearchRequestBodyToFile(requestBodyFile):
    with open(requestBodyFile, "w") as outfile:
        outfile.write(searchApiRequestBody)


def getHeaders():
    return searchApiHeaders


def setHeaders(headers):
    global searchApiHeaders
    searchApiHeaders = headers


def getHost():
    return host


def setHost(newHost):
    global host
    host = newHost


# =============================================================================
# | Helper methods
# =============================================================================

# function to get id
def getBuildID():
    start = time.time()
    print(f"> Retrieving the buildID from {host}")
    response = requests.get(host)
    buildID = str(response.content).split("buildId\":\"")[1].split("\"")[0]
    end = time.time()
    elapsed = end - start 
    print(f"> Finished in {round(elapsed, 1)} seconds")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    return buildID


def getTopics(url):
    start = time.time()
    print(f"> Retrieving the topics from {url}")
    sourceCode = str(requests.get(url).content).replace("\\", "")
    jsonData = sourceCode.split('type="application/json">')[1].split('</script>')[0]
    topicSlugs = []
    listElements = json.loads(jsonData)["props"]["pageProps"]["list"]
    for elem in listElements:
        itemsList = elem["items"]
        for item in itemsList:
            topicSlugs.append(item["slug"])
    end = time.time()
    elapsed = end - start 
    print(f"> Finished in {round(elapsed, 1)} seconds")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    return topicSlugs


# function to build url
def buildDataURL(slug):
    daily_id = getBuildID()
    base = f"{host}/_next/data/{daily_id}/talks/"
    mid = ".json?slug="
    url = base+slug+mid+slug
    return url


# function to get slug data
def getSlugData(url):
    start = time.time()
    print(f"> Retrieving the slug data from {url}")
    response = requests.get(url)
    try:
        end = time.time()
        elapsed = end - start 
        print(f"> Finished in {round(elapsed, 1)} seconds")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        return response.json()
    except ValueError:
        print(f"> There has been an issue with the slug data from {url}!")
        print(f"RESPONSE: {response}")
        issues[url]=response
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")      


def updateSearchApiReqBody(page, topic):
    body = searchApiRequestBody.replace('"page": 0', f'"page": {page}')
    body = body.replace('TOPIC_PLACEHOLDER', f'{topic}')
    return body


# Version 1 - using api request
def api_scraping(topic):
    start = time.time()
    print(f"> Scrapping all slugs in topic: {topic}")
    urls = []
    page_number = 0
    total_pages = 0
    filename = f"slug/TED_Talk_{topic}_URLs.txt"
    f = open(filename, 'w')

    while True:
        requestBody = updateSearchApiReqBody(page_number, topic)
        # Get SLUGS from search API by topic tag with pagination
        response = requests.post(searchApiEndpoint, 
                                 headers=searchApiHeaders,
                                 data=json.dumps(requestBody))
        if page_number == 0:
            total_pages = response.json()['results'][0]['nbPages']

        for slug in response.json()['results'][0]['hits']:
            urls.append(slug['slug'])
        print(f"{page_number} / {total_pages}")
        page_number += 1
        if page_number > total_pages:
            break
    f.write('\n'.join(urls))
    f.close()

    print(f"> {len(urls)} URLs for topic {topic} have been saved in {f}.")
    end = time.time()
    elapsed = end - start 
    print(f"> Finished in {round(elapsed, 1)} seconds")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    return urls


def discoverSlugDataFiles():
    slug_files = [pos_slug for pos_slug in os.listdir(path_to_slug)
                  if pos_slug.endswith('.txt')]
    for sfile in slug_files:
        print(sfile)
        with open(os.path.join(path_to_slug, sfile)) as slug_file:
            loaded = slug_file.read().splitlines()
            print(f"{len(loaded)} slugs found")
            slugs_combined.append(loaded)
    # combining all slugs and removing duplicates
    return list(set([item for sublist in slugs_combined for item in sublist]))


# function to export json data for a list of slugs
def extract_json_by_slug(path_to_json, slugs, retry_limit=3):
    count = 0
    max_count = 10
    slugs_retry = {}

    start = time.time()
    print(f"> Extracting json data for all slugs. Might take a while")
    for i, slug in enumerate(slugs):
        count += 1
        slugURL = buildDataURL(slug)
        data_json = getSlugData(slugURL)

        # checking if the respose is null
        if data_json is None: 
            if slug in slugs_retry:
                slugs_retry[slug] += 1
            else:
                slugs_retry[slug] = 1
        else:
            json_object = json.dumps(data_json)
            file_slug = slug if len(slug) < 100 else slug[:101]

            with open(f"{path_to_json}{file_slug}.json", "w") as outfile:
                outfile.write(json_object)

        # adding throttling/wait time to stay within rate rate limiting
        if count > max_count:
            time.sleep(10)
            count = 0
        else:
            time.sleep(3)

        # Print progress for every 100 slugs processed
        if (i+1) % 100 == 0:
            print(f"Processed {i+1} out of {len(slugs)} slugs.")

    # Retry slugs that failed but haven't reached the retry limit
    slugs_to_retry = [slug for slug,
                      retries in slugs_retry.items() if retries < retry_limit]

    print(f"{len(slugs_to_retry)} slugs return None after {retry_limit} tries")

    if len(slugs_to_retry) != 0:
        extract_json_by_slug(slugs_to_retry, retry_limit)

    if len(issues) > 0:
        print(issues)

    end = time.time()
    elapsed = end - start 
    print(f"> Finished in {round(elapsed, 1)} seconds")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    return slugs_to_retry, issues


# Data quality check: function to check for null jsons and missing transcripts
def check_null_missing_transcript(path_to_json):
    count_missing = 0
    count_null = 0

    json_files = [pos_json for pos_json in os.listdir(path_to_json)
                  if pos_json.endswith('.json')]
    for jfile in json_files:
        with open(os.path.join(path_to_json, jfile)) as json_file:
            jText = json.load(json_file)
            if jText is None:
                count_null += 1
                nulls.append(jfile)
            elif jText["pageProps"]["transcriptData"]["translation"] is None:
                count_missing += 1
                missing.append(jfile)
    print("> Summary:")
    print(f"There are {count_null} null jsons files")
    print(f"There are {count_missing} jsons files with missing transcripts")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")


# =============================================================================
# | Execution
# =============================================================================

topics=getTopics(f"{host}/topics")
api_scraping(topics[0])  # <== TODO: fails when slugs folder structure is missing
extract_json_by_slug(path_to_json, discoverSlugDataFiles())
check_null_missing_transcript(path_to_json)