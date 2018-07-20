# -*- coding=utf-8 -*-

import requests
import chardet
class HtmlDownloader(object):
	def __init__(self):
		self.headers ={'User-Agent':'User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
	def download(self,url):
		if url is None or len(url)==0:
			print u'can not crawl an empty url.......'
			return None

		print self.headers
		r = requests.get(url,headers=self.headers)
		print r.status_code
		if r.status_code == 200:
			r.encoding = chardet.detect(r.content)['encoding']
			return r.text
		else:
			r.raise_for_status( )
			return None