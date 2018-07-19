# -*- coding=utf-8 -*-
import re 
from bs4 import BeautifulSoup
import urlparse
import sys
import chardet
class HtmlParser(object):
	def parser(self,content,page_url,base_url):
		if content is None or base_url is None:
			print "empty error nothing to parse"
			return None
		soup = BeautifulSoup(content,'lxml')

			
		try:
			new_urls = self.parse_urls(soup,page_url,base_url)
		except Exception,e:
			print 'parse_urls error',e
			return None

		try:
			new_data = self.parse_data(soup,page_url)
		except Exception,e:
			print 'parse_data error',e
			return None
		return new_urls,new_data

	def parse_urls(self,soup,page_url,base_url):

		new_urls =set()
		links = soup.find_all('a',href =re.compile(r'/item/.*?'))

		for link in links:
			link = link.get('href') 
			url = urlparse.urljoin(base_url,link)
			new_urls.add(url)
			print url
		return new_urls

	def parse_data(self,soup,page_url):

		data ={}
		title = soup.title
		description = soup.find('meta',attrs ={'name':"description"})
		print '++++++++++++++++'
		if not description:
			print title
			print 'parse_data:error happened'
			return None
		print '==============='
		data['title'] = title.get_text()
		print type(data['title'])
		data['description'] = description.get('content')
		print type(data['title'])
		data['url'] = page_url
		print type(data['title'])
		return data
