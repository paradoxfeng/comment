from mongoqueue import Mongoqueue
import grequests
from config import HEADERS
import json

comment_url_queue = Mongoqueue(db='test',table='comment_urls')
comment_queue = Mongoqueue(db='test',table='comment')


def parse_comment_url():
    while comment_url_queue:
        try:
            urls = []
            for x in range(1000):
                url = comment_url_queue.pop()
                print('从队列拿到',url)
                urls.append(url)
        except KeyError:
            print('队列没有数据')
        else:
            rs = (grequests.get(u,headers=HEADERS,timeout=100) for u in urls)
            responses = grequests.imap(rs)
            for response in responses:
                parse(response)
        finally:
            rs = (grequests.get(u, headers=HEADERS) for u in urls)
            responses = grequests.imap(rs)
            for response in responses:
                parse(response)

def parse(response):
    try:
        text = response.text
        result = json.loads(text)
        comments = result['comments']
        for comment in comments:
            comment = comment['content']
            comment_queue.push_comment(comment)
            comment_url_queue.complete(response.url)
    except json.JSONDecodeError as e:
        print(e)


if __name__ == '__main__':
    parse_comment_url()