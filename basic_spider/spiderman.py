#-*-coding:utf-8-*-

from basic_spider.urlmanager import UrlManager
from basic_spider.htmldownloader import HtmlDownloader
from basic_spider.htmlparser import HtmlParser
from basic_spider.datastorer import DataStorer
import urllib2
import urlparse
class SpiderMan(object):
	def __init__(self):
		self.UrlManager = UrlManager()
		self.HtmlDownloader = HtmlDownloader()
		self.HtmlParser = HtmlParser()
		self.DataStorer = DataStorer()

	def crawl(self,root_url,base_url):
		self.UrlManager.add_new_url(root_url)

		while(self.UrlManager.has_new_url() and self.UrlManager.crawled_urls_size()<100):
			try:
				url = self.UrlManager.get_new_url()
				content = self.HtmlDownloader.download(url)
				urls,data = self.HtmlParser.parser(content,url,base_url)
				self.UrlManager.add_new_urls(urls)
				self.DataStorer.store_data(data)
				print u'已抓取 %d 的链接'%self.UrlManager.crawled_urls_size()
				print u'队列中还有%d的链接'%self.UrlManager.new_urls_size()
			except Exception,e:
				print 'crawl failed ...'
				print 'reason:',e
		choice = raw_input('enter "a" only store to db,"b" only output html, "c" both will be done')
		if choice =='a':

			self.DataStorer.output_db()
		elif choice == "b":
			self.DataStorer.output_html()
		elif choice == 'c':
			self.DataStorer.output_db()
			self.DataStorer.output_html()
		else:
			pass

if __name__ == '__main__':
	spider_man = SpiderMan()
	base_url = 'https://baike.baidu.com'
	word = urllib2.quote("中南大学")
	root_url =base_url+'/item/'+word
	spider_man.crawl(root_url,base_url)

