from multiprocessing.managers import BaseManager
import Queue
import time
from distributed_process.htmldownloader import HtmlDownloader
from distributed_process.htmlparser import  HtmlParser

class Spider_Manager(object):
	"""docstring for Spider_Manager"""
	def __init__(self, arg):
		BaseManager.register('get_task_url_queue')
		BaseManager.register('get_result_queue')

		server_addr = '127.0.0.1'
		print 'connecting to %s'%server_addr
		self.manager = BaseManager(address = (server_addr,22222),authkey='begin')
		self.manager.connect()

		self.url_queue = self.manager.get_task_url_queue()
		self.result_queue = self.manager.get_result_queue()

		self.htmldownloader = HtmlDownloader()
		self.htmlparser = HtmlParser()

		print 'finish init spider_manager......'

	def crawl(self,base_url):
		while True:
			try:
				url = self.url_queue().get()
				if url  == 'shutdown':
					print u'节点收到主控通知，正在停止进程。。'
					self.result_queue.put(('new_urls':'shutdown','data':'shutdown'))
					return
				print u'爬虫节点正在解析%s'%url.encode('utf-8')

				content  = self.htmldownloader.download(url)
				new_urls = self.htmlparser.parse_urls(content,url,base_url)
				data = self.htmlparser.parse_data(content,url)
				self.result_queue.put(('new_urls':new_urls,'data':data))

			except EOFError,e:
				print u'连接工作节点失败，reason：',e
				return
			except Exception,e:
				print u'crawling failed，reason：',e
