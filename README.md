依次运行c，d，e，f即可完成所有评论的抓取。
mongoqueue.py 用于实现分布式进程的mongodb队列，可以实现对爬取失败的url进行重新抓取
实现了协程，抓取速度还可以，但是比较容易封ip，暂时没做ip代理的切换