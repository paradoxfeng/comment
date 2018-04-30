from config import *
import requests
from lxml import etree
import grequests
from mongoqueue import Mongoqueue

queue = Mongoqueue(db='test',table='detail_album_url')


album_urls = []
def fetch_start_urls(start_urls):
    # for url in start_urls:
    #     for x in range(65,90):
    #         response = requests.get(url.format(initial=x),headers=HEADERS)
    #         html = etree.HTML(response.text)
    #         artists = html.xpath('//ul[@id="m-artist-box"]/li')
    #         for artist in artists:
    #             artist_name = artist.xpath('.//a[@class="nm nm-icn f-thide s-fc0"]/text()')[0]
    #             artist_id = artist.xpath('.//a[@class="nm nm-icn f-thide s-fc0"]/@href')[0][11:].replace('=', '')
    #             url = 'http://music.163.com/artist/album?id={id}&limit=2000'.format(id=artist_id)
    #             album_urls.append(url)
    for url in start_urls:
        responses = [requests.get(url.format(initial=x),headers=HEADERS) for x in range(65,91)]
        for response in responses:
            html = etree.HTML(response.text)
            artists = html.xpath('//ul[@id="m-artist-box"]/li')
            for artist in artists:
                artist_name = artist.xpath('.//a[@class="nm nm-icn f-thide s-fc0"]/text()')[0]
                artist_id = artist.xpath('.//a[@class="nm nm-icn f-thide s-fc0"]/@href')[0][11:].replace('=', '')
                url = 'http://music.163.com/artist/album?id={id}&limit=2000'.format(id=artist_id)
                album_urls.append(url)


def parse_album_urls(response):
    # for url in album_urls:
    #     response = requests.get(url,headers=HEADERS)
    html = etree.HTML(response.text)
    albums_href = html.xpath('//*[@id="m-song-module"]/li//a[@class="msk"]/@href')
    for album_href in albums_href:
        album_id = album_href.replace('/album?id=','')
        url = 'http://music.163.com/album?id={id}'.format(id=album_id)
        queue.push(url)


def process_album_urls(album_urls):
    rs = (grequests.get(url,headers=HEADERS) for url in album_urls)
    responses = grequests.imap(rs)
    for response in responses:
        parse_album_urls(response)


if __name__ == '__main__':
    fetch_start_urls(START_URLS)
    process_album_urls(album_urls)