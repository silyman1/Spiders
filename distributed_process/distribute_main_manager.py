#-*-coding:utf-8-*-

from multiprocessing.managers import BaseManager
from multiprocessing import freeze_support,Process
import Queue
import time
from distributed_process.urlmanager import  UrlManager
from distributed_process.datastorer import  DataStorer
class SuperManager(BaseManager):

	def get_url_queue(self,url_queue)
		return url_queue
	def get_result_queue(self,result_queue)
		return result_queue
	def create_manager(self,url_q,result_q):
		BaseManager.register('get_url_queue',callable= self.get_url_queue)
		BaseManager.register('get_result_queue',callable= self.get_result_queue)

		manager = BaseManager(address = ('127.0.0.1',22222),authkey='begin')

		return manager
	def url_schedule(self,root_url,url_q,link_q):
		urlmanager = UrlManager()
		urlmanager.add_new_url(root_url)
		while True:
			while urlmanager.has_new_url():
				new_url = urlmanager.get_new_url()
				url_q.put(new_url)
				print 'send %s to spider_node'%new_url
				print u'已抓取 %d 的链接'%urlmanager.crawled_urls_size()
				print u'队列中还有%d的链接'%urlmanager.new_urls_size()
				if urlmanager.crawled_urls_size()>=2000:
					url_q.put('shutdown')
					print u'已达到2000条,正在通知爬虫节点'
					urlmanager.save_urls_process_status(urlmanager.new_urls,'new_urls.txt')
					urlmanager.save_urls_process_status(urlmanager.crawled_urls,'crawled_urls.txt')
					return
				try:
					if not link_q.empty():
						urls = link_q.get()
						urlmanager.add_new_urls(urls)
					else:
						print 'no links send to manager...please wait'
						time.sleep(0.2)
	def result_solve_schedule(self,result_q,link_q,store_q):
		while True:
			try:
				if not result_q.empty():
					content = result_q.get()
					if content['new_urls'] == 'shutdown':
						print u"收到终止通知，即将结束分析进程"
						store_q.put('shutdown')
						return
					link_q.put(content['new_urls'])
					store_q.put(content['data'])
				else:
					print 'no results send to result_solve_schedule...please wait'
					time.sleep(0.2)	
			except BaseException,e:
				print 'result_solve_schedule error..error reason:',e
				time.sleep(0.1)
	def store_solve_schedule(self,store_q):
		datastorer = DataStorer()
		while True:
		 	if not store_q.empty():
		 		data = store_q.get()
		 		if data == 'shutdown':
		 			print u"收到终止通知，即将结束存储进程"
		 			datastorer.output_end()
		 		datastorer.store.store_data(data)
		 	except BaseException,e:
				print 'store_solve_schedule error..error reason:',e
				time.sleep(0.1)
if __name__ == "__main__":
	url_queue = Queue.Queue()
	result_queue = Queue.Queue()
	link_queue = Queue.Queue()
	store_queue = Queue.Queue()

	root_url =

	manager = SuperManager()
	manager.create_manager(url_queue, result_queue)

	url_process = Process(target=manager.url_schedule,args=(root_url,url_queue,link_queue,))
	result_solve_process = Process(target=manager.result_solve_schedule,args=(result_queue,link_queue,store_queue,))
	store_process = Process(target=manager.store_solve_schedule,args=(store_queue,))

	url_process.start() 
	result_solve_process.start()
	store_process.start()

