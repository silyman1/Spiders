# -*- coding=utf-8 -*-

class UrlManager(object):
	def __init__(self):
		self.new_urls=set()
		self.crawled_urls=set()

	def new_urls_size(self):
		return len(self.new_urls)

	def crawled_urls_size(self):
		return len(self.crawled_urls)

	def has_new_url(self):
		return self.new_urls_size()!=0

	def get_new_url(self):
		new_url=self.new_urls.pop()
		self.crawled_urls.add(new_url)
		print 'get new url %s'%new_url
		return new_url

	def add_new_url(self,url):
		if url is None or len(url)==0:
			print u'add empty url.......'
			return 
		if url in self.new_urls or url in self.crawled_urls:
			print u'%s is a repeating url'%url
			return
		else:
			self.new_urls.add(url)
			print 'adding new url %s'%url
	def add_new_urls(self,urls):
		if urls is None or len(urls)==0:
			print u'add empty urls.......'
			return 
		for url in urls:
			self.add_new_url(url)
