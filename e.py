from mongoqueue import Mongoqueue
import grequests
from config import HEADERS
import json

hot_url_queue = Mongoqueue(db='test',table='hot_comment_urls')
comment_url_queue = Mongoqueue(db='test',table='comment_urls')


def get_comment_url():
    while hot_url_queue:
        try:
            urls = []
            for x in range(1000):
                url = hot_url_queue.pop()
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
    song_id = response.url.replace('http://music.163.com/api/v1/resource/comments/R_SO_4_', '')
    text = response.text
    result = json.loads(text)
    total = result['total']
    integer = int(total) // 100
    for x in range(integer + 1):
        url = 'http://music.163.com/api/v1/resource/comments/R_SO_4_{song_id}?limit=100&offset={offset}'.format(song_id=song_id, offset=x * 100)
        comment_url_queue.push(url)
        hot_url_queue.complete(response.url)


if __name__ == '__main__':
    get_comment_url()