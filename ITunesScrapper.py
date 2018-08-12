#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  6 21:27:18 2018
https://itunes.apple.com/us/podcast/id294055449
@author: Andi Samijono
"""
import requests
import re
import json
import pandas as pd
import sqlite3
from sqlite3 import Error
## Functions

def parse_scraped_podcast_page(podcast_page):
   script_search = re.search('<script type="text/javascript" charset="utf-8">its\.serverData=(.*)</script>', podcast_page)
   if script_search:
       podcast_data = json.loads(script_search.group(1))
       country_code = podcast_data['storePlatformData']['product-dv-product']['meta']['storefront']['cc']
       podcast_itunes_id, podcast = list(podcast_data['storePlatformData']['product-dv-product']['results'].items())[0]
       popularity_map = podcast_data['pageData']['podcastPageData']['popularityMap']['podcastEpisode']
       episodes = podcast.get('children')
       related_podcast_ids = podcast_data['pageData']['podcastPageData']['listenersAlsoBought']

       if not episodes:
           print (ValueError('No Podcast Info'))
           return {}

       return {
           'podcast': podcast,
           'podcast_itunes_id': podcast_itunes_id,
           'country_code': country_code,
           'popularity_map': popularity_map,
           'episodes': episodes,
           'related_podcast_ids': related_podcast_ids,
       }
   else:
       print(ValueError('No Podcast Info'))
       return {}
      
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()
 ## main function
if __name__ == '__main__':
    ## Scraping
    podcastCSV = pd.read_csv('Podcastlist.csv')
    podcastList = podcastCSV.itunes_id
    dictPodcast = {}
    for i in range (0, 10): ## TO do all use range (0, len(podcastList))
        ## Get a dictionary from url
        podcastID = podcastList[i]
        print("This is currently scraped podcast", podcastID)
        url = 'https://itunes.apple.com/us/podcast/id%d' %podcastID
        head = {
                'User-Agent': 'iTunes/11.5.2 (Macintosh; Intel Mac OS X 10.7.2) AppleWebKit/534.52.7'
                } 
        response = requests.get(url, headers = head)
        decoded= response.content.decode('utf-8')
        temp = parse_scraped_podcast_page(decoded)
        if (temp == {}):
            print("skipping this",podcastID )
        dictPodcast[podcastID] = temp
    
        
    ## Section 2 to SQL
    conn = sqlite3.connect("Podcast.db")
    cursor = conn.cursor()
    ## drop table podcast
    dropStatement = "DROP TABLE podcast"
    cursor.execute(dropStatement)
    ## drop table episode
    dropStatement = "DROP TABLE episode"
    cursor.execute(dropStatement)
    ## drop table relEpisode
    dropStatement = "DROP TABLE relEpisode"
    cursor.execute(dropStatement)
    ## Make table for podcast data
    sql_command = """
    CREATE TABLE podcast (
    podSequenceId INTEGER PRIMARY KEY,
    podcastID INTEGER,
    artistID INTEGER,
    artistName STRING,
    artisturl STRING,
    copyright STRING,
    feedUrl STRING,
    id INTEGER,
    isNews BOOLEAN,
    isNotSubscribeable BOOLEAN,
    kind STRING,
    name STRING,
    nameRaw STRING,
    nameSortValue STRING,
    podcastLanguageName STRING,
    podcastType STRING,
    podcastWebsiteUrl,
    releaseDate STRING,
    releaseDateTime STRING,
    shortUrl STRING,
    tellAFriendMessageContentsUrl STRING,
    url STRING
    );"""
    
    cursor.execute(sql_command)
    ## Make table for episode data
    sql_command = """
    CREATE TABLE episode (
    epiSequenceID INTEGER PRIMARY KEY,
    podSequenceID INTEGER FORIEGN KEY,
    episodeID INTEGER,
    podcastID INTEGER FORIEGN KEY,
    popularity DECIMAL(6,5),
    artistID INTEGER,
    artistName STRING,
    artisturl STRING,
    collectionId INTEGER,
    collectionName STRING,
    feedUrl STRING,
    id INTEGER,
    kind STRING,
    name STRING,
    nameRaw STRING,
    podcastEpisodeGuid STRING,
    podcastEpisodeType STRING,
    podcastEpisodeWebsiteUrl STRING,
    releaseDate STRING,
    releaseDateTime STRING,
    shortUrl STRING,
    url STRING
    );"""
    
    cursor.execute(sql_command)
    
    ## Make table for relEpisode data
    sql_command = """
    CREATE TABLE relEpisode (
    relEpisodeID INTEGER,
    relPodcastID INTEGER FORIEGN KEY,
    podSqeuenceID INTEGER FORIEGN KEY,
    podcastID INTEGER,
    popularity DECIMAL(6,5)
    );"""
    
    cursor.execute(sql_command)
    
    episodeSequence = 0
    dictrelPodcast = {}

    for i in range(0, len(dictPodcast)):
            print("Current Podcast",podcastList[i])
            ## podcast data
            podcastData = dictPodcast[podcastList[i]]['podcast']
            Table0 = pd.DataFrame({'podSequenceID': i, 'podcastID':podcastList[i], 'artistID':podcastData["artistId"],
                                   'artistName':podcastData["artistName"],'artisturl':podcastData["artistUrl"],
                                   'copyright':podcastData["copyright"],'feedUrl':podcastData["feedUrl"],
                                   'id':podcastData["id"], 'isNews':podcastData['isNews'] , 
                                   'isNotSubscribeable':podcastData["isNotSubscribeable"] ,
                                   'kind':podcastData["kind"],'name':podcastData["name"],
                                   'nameRaw':podcastData["nameRaw"],'nameSortValue':podcastData['nameSortValue'] , 
                                   'podcastLanguageName':podcastData['podcastLanguageName'] ,'podcastType':podcastData['podcastType'] , 
                                   'podcastWebsiteUrl': podcastData["podcastWebsiteUrl"],
                                   'releaseDate':podcastData["releaseDate"],
                                   'releaseDateTime':podcastData["releaseDateTime"],
                                   'shortUrl':podcastData["shortUrl"],
                                   'tellAFriendMessageContentsUrl': podcastData["tellAFriendMessageContentsUrl"],
                                   'url':podcastData["url"] }, index = [i])
            Table0.to_sql('podcast', conn, if_exists = 'append', index = False)
            ## episode data
            episode = dictPodcast[podcastList[i]]["episodes"]
            episodeKeys = list(map(int,episode.keys()))
            popularityData = dictPodcast[podcastList[i]]["popularity_map"]
            for j in range(0, len(episodeKeys)):
                episodeSequence = episodeSequence+1
                episodeData = episode[str(episodeKeys[j])]
                Table1 = pd.DataFrame({'epiSequenceID':episodeSequence,'podSequenceID':i, 'episodeID':episodeKeys[j], 'podcastID': podcastList[i], 
                                       'popularity':popularityData[str(episodeKeys[j])],
                                       'artistID':episodeData["artistId"],'artistName':episodeData["artistName"],'artisturl':episodeData["artistUrl"],
                                       'collectionId':episodeData["collectionId"],'collectionName':episodeData["collectionName"],
                                       'feedUrl':episodeData["feedUrl"],'id':episodeData["id"],'kind':episodeData["kind"],
                                       'name':episodeData["name"],'nameRaw':episodeData["nameRaw"],
                                                         'podcastEpisodeGuid':episodeData["podcastEpisodeGuid"],
                                                         'podcastEpisodeType':episodeData["podcastEpisodeGuid"],
                                                         'podcastEpisodeWebsiteUrl':episodeData["podcastEpisodeWebsiteUrl"],
                                                                                               'releaseDate':episodeData["releaseDate"],
                                                                                               'releaseDateTime':episodeData["releaseDateTime"],
                                                                                               'shortUrl':episodeData["shortUrl"],
                                                                                                   'url':episodeData["url"] }, index = [j])
                Table1.to_sql('episode', conn, if_exists = 'append', index = False)  
            ## Related podcast data    
            relPodID = dictPodcast[podcastList[i]]['related_podcast_ids']
            for j in range (0, len(relPodID)):
                relPodcastID = int(relPodID[j])
                print("related podcast ID", relPodcastID)
                url = 'https://itunes.apple.com/us/podcast/id%d' %relPodcastID
                head = {
                'User-Agent': 'iTunes/11.5.2 (Macintosh; Intel Mac OS X 10.7.2) AppleWebKit/534.52.7'
                } 
                response = requests.get(url, headers = head)
                decoded= response.content.decode('utf-8')
                temp =  parse_scraped_podcast_page(decoded)
                if (temp == {}):
                    print("skipping this",relPodcastID )
                    continue
                dictrelPodcast[relPodcastID] = temp
                relPodEpisode = list(map(int,dictrelPodcast[relPodcastID]["episodes"].keys()))
                relPopularity = dictrelPodcast[relPodcastID]["popularity_map"]
                for k in range(0, len(relPodEpisode)):
                    Table2 = pd.DataFrame({'relEpisodeID':relPodEpisode[k],
                                           'relPodcastID':relPodcastID, 'podSqeuenceID': i, 'podcastID':podcastList[i],
                                           'popularity':relPopularity[str(relPodEpisode[k])]}, index = [k])
                    Table2.to_sql('relEpisode', conn, if_exists = 'append', index = False)
     
    # Check the data
    SQLPodcastData = pd.read_sql('select * from podcast', conn)
    SQLEpisodeData = pd.read_sql('select * from episode', conn)
    SQLrelEpiData = pd.read_sql('select * from relEpisode', conn)
    
  