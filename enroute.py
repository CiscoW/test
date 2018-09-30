# coding:utf-8

from pyquery import PyQuery as pq
import Queue
import datetime
import threading
from week_time_shifts import week_to_time
from mysql import MySQLClass
import time

queue = Queue.Queue(maxsize=-1)


class RequestPageThread(threading.Thread):

    def __init__(self, url):
        threading.Thread.__init__(self)
        self.url = url

    def run(self):
        # 请求url
        page_source = pq(self.url)
        # 获取当前页面爬取时间
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 将数据放进队列中
        queue.put([page_source, now_time])


class ParsePageThread(threading.Thread):
    def run(self):
        crawl_result = []
        while not queue.empty():
            # 从队列中获取数据 先进先出
            queue_data = queue.get()
            page_source = queue_data[0]
            crawl_time = queue_data[1]

            # 获取表头
            head = page_source('.prettyTable.fullWidth thead')
            # 通过表头获取表体
            body = head.siblings()

            for tr_item in body.items():
                tds = tr_item('td')
                # 用于存放一行数据
                temp_data = []
                # 用于判断哪些列要转数据格式
                col = 0
                for td_item in tds.items():
                    temp_data.append(td_item.text())
                    if col == 2:
                        provenance = td_item.text()
                        start_index = provenance.rfind('(') + 1
                        end_index = provenance.rfind(')')
                        converted_data = provenance[start_index:end_index]
                        temp_data.append(converted_data)

                    elif col == 3:
                        scheduled_departure_time = td_item.text()
                        start_index = scheduled_departure_time.find(u' ')
                        end_index = scheduled_departure_time.find(u' ', start_index + 1)
                        week = scheduled_departure_time[0:start_index]
                        time = scheduled_departure_time[start_index + 1:end_index]
                        week_time = week_to_time(week, time)
                        temp_data.append(week_time)
                    elif col == 4:
                        departure_time = td_item.text()
                        if departure_time == '':
                            week_time = None
                        else:
                            start_index = departure_time.find(u' ')
                            end_index = departure_time.find(u' ', start_index + 1)
                            week = departure_time[0:start_index]
                            time = departure_time[start_index + 1:end_index]
                            week_time = week_to_time(week, time)
                        temp_data.append(week_time)

                    elif col == 5:
                        estimated_arrival_time = td_item.text()
                        start_index = estimated_arrival_time.find(u' ')
                        end_index = estimated_arrival_time.find(u' ', start_index + 1)
                        week = estimated_arrival_time[0:start_index]
                        time = estimated_arrival_time[start_index + 1:end_index]
                        week_time = week_to_time(week, time)
                        temp_data.append(week_time)

                    col += 1

                temp_data.append(crawl_time)
                crawl_result.append(temp_data)

        # 连接数据库并写入数据
        mysql = MySQLClass(host='127.0.0.1', port=3306, user='root', passwd='', db='flightawaredata')
        mysql.insert_into('flightawareenroute', crawl_result, 11)
        mysql.close()
        print str(len(crawl_result)) + u' 条数据保存完成!'
        print '-*-*' * 20


def enroute_run():
    url = 'http://flightaware.com/live/airport/ZSAM/enroute?;offset=%d;order=estimatedarrivaltime;sort=ASC'
    url_list = [url % (i * 20) for i in xrange(0, 8)]
    while True:
        print time.ctime()
        thread_list = []
        for url in url_list:
            request_page_thread = RequestPageThread(url)
            thread_list.append(request_page_thread)
            request_page_thread.start()

        for request_page_thread in thread_list:
            request_page_thread.join()

        parse_page_thread = ParsePageThread()
        parse_page_thread.start()

        print time.ctime()
        print u'队列数据量: ' + str(queue.qsize())
        print '-*-*' * 20
        time.sleep(30)


if __name__ == '__main__':
    enroute_run()
