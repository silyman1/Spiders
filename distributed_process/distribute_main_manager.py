#-*-coding:utf-8-*-

from multiprocessing.managers import BaseManager
from multiprocessing import freeze_support,Process,Queue,Lock

from datetime import datetime
import urllib2
import sys
import socket
import time
from distributed_process.urlmanager import  UrlManager
from distributed_process.datastorer import  DataStorer

def get_url_queue():
	global url_queue
	return url_queue
def get_result_queue():
	global result_queue
	return result_queue

def flag_func(flag_queue):
	flag =False

	if not flag_queue.empty() and flag_queue.get('flag1') and flag_queue.get('flag2') and flag_queue.get('flag3'):

		flag = True
	return flag
def count_t(count = [0]):
	def increaser():
		count[0]  = count[0] + 1
		return count[0]
	return increaser
class Schedule_Manager(object):	
	"""my-defined spider controller,modified multiprocessing packet's managers.py 
	   add my_serve_forever class function... """

	def __init__(self):
		self.lock = Lock()
		self.inner_flag = 1
	def gettime(self):
		return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())) 

	def record_time(self,name,start_time,end_time):
		with self.lock:
			f = open("time.log",'a+')
			f.write("%s start:%s\n"%(name,start_time.strftime('[%Y-%m-%d %H:%M:%S]')))
			f.write("%s end:%s\n"%(name,end_time.strftime('[%Y-%m-%d %H:%M:%S]')))
			f.write("%s total_time:%ss\n"%(name,(end_time-start_time).seconds))
			f.write("+++++++++++++++++++++++++++++++++++++\n")
			f.close()

	def create_manager(self):
		BaseManager.register('get_url_queue',callable= get_url_queue)
		BaseManager.register('get_result_queue',callable= get_result_queue)
		manager = BaseManager(address = ('',5555),authkey='ppp')

		return manager

	def url_schedule(self,flag_q,root_url,url_q,link_q):
		start_time = datetime.now()
		urlmanager = UrlManager()
		size_load = urlmanager.crawled_urls_size()
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
				if urlmanager.crawled_urls_size()- size_load >=3:#每次爬取指定数目，断点续爬
					url_q.put('shutdown')
					print u'已达到2000条,正在通知爬虫节点'
					urlmanager.save_urls_process_status(urlmanager.new_urls,r'new_urls.txt')
					urlmanager.save_urls_process_status(urlmanager.crawled_urls,r'crawled_urls.txt')
					print 'url_process exit...'
					end_time = datetime.now()
					self.record_time('url_process', start_time, end_time)
					flag_q.put({"flag1":self.inner_flag})
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

	def result_solve_schedule(self,flag_q,result_q,link_q,store_q):
		inner_flag = 1
		start_time = datetime.now()
		print self.gettime(),'result_solve_process......'
		while True:
			try:
				if not result_q.empty():
					content = result_q.get()
					if content['new_urls'] == 'shutdown':
						print u"收到终止通知，即将结束分析进程"
						store_q.put('shutdown')
						end_time = datetime.now()
						self.record_time('result_process', start_time, end_time)
						flag_q.put({"flag2":self.inner_flag})
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
	def store_solve_schedule(self,flag_q,store_q):
		inner_flag = 1

		start_time = datetime.now()
		datastorer = DataStorer()
		count = 0
		print self.gettime(),'start store_solve_process......'
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
			 			print u'存储进程结束......'
			 			end_time = datetime.now()
			 			self.record_time('store_process', start_time, end_time)
			 			flag_q.put({"flag3":self.inner_flag})
			 			return
			 		counter = count_t()
			 		count = counter()
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
def main_start(url_process,result_solve_process,store_process):

	url_process.start() 
	result_solve_process.start()
	store_process.start()
def start_server():

	manager = create_manager()
	manager.get_server().serve_forever()
if __name__ == "__main__":

	flag = True
	freeze_support()
	url_queue = Queue()
	result_queue = Queue()
	link_queue = Queue()
	store_queue = Queue()
	flag_queue = Queue()
	query_word = "多肉植物"
	word = urllib2.quote(query_word)
	base_url = 'https://baike.baidu.com'

	root_url = 'https://baike.baidu.com'+'/item/' + word

	manager_instance = Schedule_Manager()

	manager = manager_instance.create_manager()

	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                
	s.bind(('127.0.0.1',9999))
	s.listen(5)
	print 'now server start .....waiting for spider node to connect' 
	sock,addr = s.accept()
	data = sock.recv(1024)
	i = 0
	while 1:
		if data.decode("utf-8") =='start' and flag:
			print 'spider node connecting successfully'  
			data = ''
			flag = False
			i = i+1
			print 'start ................'
			url_process = Process(target=manager_instance.url_schedule,args=(flag_queue,root_url,url_queue,link_queue,))
			result_solve_process = Process(target=manager_instance.result_solve_schedule,args=(flag_queue,result_queue,link_queue,store_queue,))
			store_process = Process(target=manager_instance.store_solve_schedule,args=(flag_queue,store_queue,))	
			url_process.start() 
			result_solve_process.start()
			store_process.start()

			manager.get_server().my_serve_forever(sock)

			print 'NO.%d to call the controller point'%i
			print 'waiting another...'
	
			sock,addr = s.accept()
			data = sock.recv(1024)

		print 'waiting for flag'
		flag = flag_func(flag_queue)
		print flag ,'*********************'