from mongoqueue import Mongoqueue
import grequests
from lxml import etree
from config import HEADERS

urlqueue = Mongoqueue(db='test',table='detail_album_url')
hot_url_queue = Mongoqueue(db='test',table='hot_comment_urls')


def parse_song_url():
    while urlqueue:
        try:
            urls = []
            for x in range(1000):
                url = urlqueue.pop()
                print('从队列拿到',url)
                urls.append(url)
        except KeyError:
            print('队列没有数据')
        else:
            rs = (grequests.get(u,headers=HEADERS) for u in urls)
            responses = grequests.imap(rs)
            for response in responses:
                parse(response)
        finally:
            rs = (grequests.get(u, headers=HEADERS) for u in urls)
            responses = grequests.imap(rs)
            for response in responses:
                parse(response)

def parse(response):
    html = etree.HTML(response.text)
    songs = html.xpath('//ul[@class="f-hide"]/li')
    for song in songs:
        song_id = song.xpath('./a/@href')[0].replace('/song?id=', '')
        url = 'http://music.163.com/api/v1/resource/comments/R_SO_4_{song_id}'.format(song_id=song_id)
        hot_url_queue.push(url)
        urlqueue.complete(response.url)


if __name__ == '__main__':
    parse_song_url()





