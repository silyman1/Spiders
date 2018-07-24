#-*-coding:utf-8-*-

from multiprocessing.managers import BaseManager
from multiprocessing import freeze_support,Process,Queue
import time
import urllib2

from distributed_process.urlmanager import  UrlManager
from distributed_process.datastorer import  DataStorer

def get_url_queue():
	global url_queue
	return url_queue
def get_result_queue():
	global result_queue
	return result_queue

class Testpro(BaseManager):
	def ooo(self):
		print 'ppp'

class Schedule_Manager(object):	
	def gettime(self):
		return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())) 

	def create_manager(self,url_q,result_q):
		BaseManager.register('get_url_queue',callable= get_url_queue)
		BaseManager.register('get_result_queue',callable= get_result_queue)

		manager = BaseManager(address = ('',5555),authkey='ppp')

		return manager

	def url_schedule(self,root_url,url_q,link_q):
		urlmanager = UrlManager()
		urlmanager.add_new_url(root_url)
		print self.gettime(),'start url_process......'
		while True:
			while (urlmanager.has_new_url() or link_q):
				if urlmanager.has_new_url():
					new_url = urlmanager.get_new_url()
					url_q.put(new_url)
					print 'send %s to spider_node'%new_url
					print u'当前已抓取 %d 的链接'%urlmanager.crawled_urls_size()
					print u'队列中还有%d的链接'%urlmanager.new_urls_size()
				if urlmanager.crawled_urls_size()>=50:
					url_q.put('shutdown')
					print u'已达到2000条,正在通知爬虫节点'
					urlmanager.save_urls_process_status(urlmanager.new_urls,'new_urls.txt')
					urlmanager.save_urls_process_status(urlmanager.crawled_urls,'crawled_urls.txt')
					print 'url_process exit...'
					return
				try:
					if not link_q.empty():
						urls = link_q.get()
						urlmanager.add_new_urls(urls)
					else:
						print 'no urls send to url_schedule...please wait'
						time.sleep(2)	
				except BaseException,e:
						print 'no links send to manager...please wait..reason:',e
						time.sleep(0.2)
		return 
	def result_solve_schedule(self,result_q,link_q,store_q):
		print self.gettime(),'result_solve_process......'
		while True:
			try:
				if not result_q.empty():
					content = result_q.get()
					if content['new_urls'] == 'shutdown':
						print u"收到终止通知，即将结束分析进程"
						store_q.put('shutdown')
						return
					print u"%d链接 放入link_queue"%len(content['new_urls'])
					print u"%s 放入store_queue"%content['data']['title']
					link_q.put(content['new_urls'])
					store_q.put(content['data'])
				else:
					#print 'no results send to result_solve_schedule...please wait'
					time.sleep(2)	
			except BaseException,e:
				print 'result_solve_schedule error..error reason:',e
				time.sleep(0.1)
	def store_solve_schedule(self,store_q):
		datastorer = DataStorer()
		print self.gettime(),'start store_solve_process......'
		count = 0
		while True:
			try:
				if not store_q.empty():
			 		data = store_q.get()
			 		if data == 'shutdown':
			 			print u"收到终止通知，即将结束存储进程"
						if len(datastorer.datas)>0:
							datastorer.output_html()
							datastorer.output_db()
			 			datastorer.output_end()
			 			return
			 		count = count +1
			 		data['title'] = u'NO.'+str(count)+data['title']
			 		print u'正在存储第%d条...%s...'%(count,data['title'])
			 		datastorer.store_data(data)
			 		# if signal == 'stop':
			 		# 	print u'已获取20000条数据，正在通知其他进程结束'
						# result_q.put({'new_urls':'shutdown','data':'shutdown'})
						# url_q.put('shutdown')
						# b= input()
			 		# 	return
			 	else:
					#print 'no data send to store_solve_schedule...please wait'
					time.sleep(2)	
		 	except BaseException,e:
				print 'store_solve_schedule error..error reason:',e
				time.sleep(0.1)
	def test(self):
		print 'test'
if __name__ == "__main__":
	freeze_support()
	url_queue = Queue()
	result_queue = Queue()
	link_queue = Queue()
	store_queue = Queue()
	query_word = "多肉植物"
	word = urllib2.quote(query_word)
	base_url = 'https://baike.baidu.com'

	root_url = 'https://baike.baidu.com'+'/item/' + word
	# mmm = Testpro()
	# test_pro = Process(target=mmm.ooo)
	# test_pro.start()
	manager_instance = Schedule_Manager()
	manager = manager_instance.create_manager(url_queue, result_queue)
	#manager.start()
	# if os.name == 'posix':
	# 	test_pro = Process(target=manager_instance.test)
	# 	url_process = Process(target=manager_instance.url_schedule,args=(root_url,url_queue,link_queue,))
	# 	result_solve_process = Process(target=manager_instance.result_solve_schedule,args=(result_queue,link_queue,store_queue,))
	# 	store_process = Process(target=manager_instance.store_solve_schedule,args=(store_queue,))
	# elif os.name == 'nt':
	# 	test_pro = Process(target=test)
	# 	url_process = Process(target=url_schedule,args=(root_url,url_queue,link_queue,))
	# 	result_solve_process = Process(target=result_solve_schedule,args=(result_queue,link_queue,store_queue,))
	# 	store_process = Process(target=store_solve_schedule,args=(store_queue,))
	test_pro = Process(target=manager_instance.test)
	url_process = Process(target=manager_instance.url_schedule,args=(root_url,url_queue,link_queue,))
	result_solve_process = Process(target=manager_instance.result_solve_schedule,args=(result_queue,link_queue,store_queue,))
	store_process = Process(target=manager_instance.store_solve_schedule,args=(store_queue,))	
	test_pro.start()
	url_process.start() 
	result_solve_process.start()
	store_process.start()

	manager.get_server().serve_forever()

