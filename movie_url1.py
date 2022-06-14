from time import sleep

import pymysql
# import urllib.parse
# import urllib.request
# from bs4 import BeautifulSoup
import requests


class db_connector:

    def __init__(self, host='127.0.0.1', port=10306, user='root', password='123456', db_n='movie', charset='utf8'):
        self.db = pymysql.connect(host=host,
                                  port=port,
                                  user=user,
                                  password=password,
                                  db=db_n,
                                  charset=charset
                                  )
        self.cursor = self.db.cursor()

    # def insert_data(self, data):
    #     sql = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
    #     self.cursor.execute(sql, data)
    #     self.db.commit()

    # def update_data(self, data):
    #     sql = "UPDATE test_table SET name=%s, age=%s WHERE id=%s"
    #     self.cursor.execute(sql, data)
    #     self.db.commit()

    def op(self, sql):
        self.cursor.execute(sql)
        self.db.commit()

    def select_data(self, col='*', table='movie_id'):
        sql = "SELECT " + col + " FROM " + table
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def query_data(self, query):
        sql = query
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def query_cursor(self, query):
        sql = query
        self.cursor.execute(sql)
        return self.cursor

    def close_db(self):
        self.cursor.close()
        self.db.close()

    def __del__(self):
        if self.db.open:
            self.db.close()
        self.cursor.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db.open:
            self.db.close()
        self.cursor.close()

    def __enter__(self):
        return self


# def url_query(movie_title):
#     domain = 'https://www.imdb.com'
#     search_url = domain + '/find?q=' + urllib.parse.quote_plus(movie_title)
#     with urllib.request.urlopen(search_url) as response:
#         html = None
#         try:  # 添加了try语句
#             html = response.read().decode('utf-8')
#         except Exception as e:
#             page = e.partial
#             res = page.decode('utf-8')
#         if html:
#             soup = BeautifulSoup(html, 'html.parser')
#             # Get url of 1st search result
#             try:
#                 title = soup.find('table', class_='findList').tr.a['href']
#                 movie_url = domain + title
#                 print(movie_url)
#                 return movie_url
#             except AttributeError:
#                 pass
#         else:
#             return None


# if __name__ == '__main__':

import threading
from movie_url import get
import json


class spider:
    def __init__(self, thread_num=100):
        self.thread_num = thread_num
        self.threads = []
        self.host = '127.0.0.1'
        self.user = 'root'
        self.password = '123456'
        self.database = 'movie'
        self.port = 10306
        self.charset = 'utf8'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.147 Safari/537.36'}
        self.connector = db_connector(self.host, self.port, self.user, self.password, self.database, self.charset)
        self.operator = db_connector(self.host, self.port, self.user, self.password, self.database, self.charset)

        # 使用 cursor() 方法创建一个游标对象 cursor
        self.sql = "SELECT movieId, imdbId FROM movie_links WHERE movieId IN (SELECT movie_id FROM movie_info WHERE language is NULL OR language='Unknown' ORDER BY movie_id DESC);"
        # print(sql)
        self.cursor = self.connector.query_cursor(self.sql)

        self.mutex_ac = threading.Lock()
        self.mutex_op = threading.Lock()

    def th(self):
        print('start')

        self.mutex_ac.acquire()
        # 使用 fetchone() 方法获取单条数据.
        data = self.cursor.fetchone()
        self.mutex_ac.release()

        # while data:
        while data:
            imdbid = data[1]
            # print(data)
            web_id = 'tt' + '0' * (7 - len(imdbid)) + imdbid
            link = 'https://www.imdb.com/title/' + web_id + '/'
            print(link)
            try:
                resp_t = requests.get(link, headers=self.headers).text
                # sleep(2)
                # print(resp_t)
                info = get(resp_t)
                # print(info)

                sql = "UPDATE movie_info SET web_id = '%s', language ='%s' WHERE movie_id = %s" % (
                    web_id,  info['language'], data[0])
                print(sql)
                self.mutex_op.acquire()
                self.operator.op(sql)
                self.mutex_op.release()
                print(data[0] + ' done')
                self.mutex_ac.acquire()
                data = self.cursor.fetchone()
                self.mutex_ac.release()
            except Exception as e:
                print(e)
                self.mutex_ac.acquire()
                data = self.cursor.fetchone()
                self.mutex_ac.release()
                continue

            # self.mutex_ac.acquire()
            # cursor = self.connector.query_cursor(sql)
            # data = self.cursor.fetchone()
            # self.mutex_ac.release()

        # 关闭数据库连接
        self.mutex_ac.acquire()
        self.connector.close_db()
        self.operator.close_db()
        self.mutex_ac.release()

    def thread_exec(self):
        for i in range(0, self.thread_num):
            t = threading.Thread(target=self.th, args=())
            print('threat%d creating' % (i))
            self.threads.append(t)
        for i in self.threads:
            i.start()


if __name__ == '__main__':
    spider = spider(5)
    spider.thread_exec()


# movie url example : https://www.imdb.com/title/tt0120169

# genres = ['Mystery', 'Documentary', 'Animation', 'Fantasy', 'War', 'Children', 'Action', 'Western', 'Sci-Fi',
#           'Thriller', 'Adventure', 'Musical', 'Horror', 'Crime', 'Comedy', 'Romance', '(no genres listed)', 'IMAX',
#           'Drama', 'Film-Noir']
# print
#     connector = db_connector(host, port, user, password, database, charset)
#     operator = db_connector(host, port, user, password, database, charset)
#
#     # 使用 cursor() 方法创建一个游标对象 cursor
#     sql = "SELECT movieId, imdbId FROM movie_links WHERE movieId IN (SELECT movie_id FROM movie_info WHERE rate IS NULL);"
#     # print(sql)
#     # mutex.acquire()
#     cursor = connector.query_cursor(sql)
#     # 使用 fetchone() 方法获取单条数据.
#     data = cursor.fetchone()
#     # mutex.release()
#
#     # sleep(5)
#
#     while data:
#         imdbid = data[1]
#         # print(data)
#         web_id = 'tt' + '0' * (7 - len(imdbid)) + imdbid
#         link = 'https://www.imdb.com/title/' + web_id + '/'
#         print(link)
#         resp_t = requests.get(link).text
#         # print(resp_t)
#         info = get(resp_t)
#         # print(info)
#
#         sql = "UPDATE movie_info SET web_id = '%s', duration=%d, language ='%s', storyline= '%s', rate='%.1f' WHERE movie_id = %s" % (
#             web_id, info['duration'], info['language'], info['storyline'], info['rate'], data[0])
#         # print(sql)
#         # mutex.acquire()
#         operator.op(sql)
#         data = cursor.fetchone()
#         # mutex.release()
#         print(data[0] + ' done')
#
#     # 关闭数据库连接
#
#     connector.close_db()
#     operator.close_db()
