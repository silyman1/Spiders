#-*-coding:utf-8-*-
import MySQLdb
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8') 
class DataStorer(object):
	'''
		数据库和表的编码方式都要设置好，INSERT INTO %s(title,url,description)VALUES ('%s','%s','%s')这里%s的引号不能漏
	'''
	def __init__(self):
		self.datas = []
		self.filepath='baike_%s.html'%(time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime()) )
		self.output_head()
		self.table_name = u'crawling_results'
		self.db,self.cur = self.db_init()
	def gettime(self):
		return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

	def store_data(self,data):
		if data is None:
			print 'nothing to store.....'
			return
		self.datas.append(data)
		if len(self.datas)>=10:
			self.output_html()
			self.output_db()
			self.datas = []
	def output_head(self):
		with open(self.filepath,'w+') as f:
			f.write("<html>")
			f.write("<head>")
			f.write('<meta charset="UTF-8">')
			f.write("<title>")
			f.write("展示结果页")
			f.write("</title>")
			f.write("</head>")
			f.write("<body>")
	def output_html(self):

		with open(self.filepath,'a') as f:

			for data in self.datas:
				f.write("<h3>")
				f.write("<td>%s</td>"%data['title'])
				f.write("</h3>")
				f.write("<p>&nbsp;&nbsp;&nbsp;&nbsp;%s</p>"%data['url'])
				f.write("<p>%s</p>"%data['description'])
				f.write("</tr>")

	def output_end(self):
		with open(self.filepath,'w+') as f:
			f.write("</body>")
			f.write("</html>")
		self.db.close()
	def db_init(self):
		print self.gettime(),'connecting to db...'
		try:
			db = MySQLdb.connect(host="localhost",user="root",passwd="pzc",db="test",charset='utf8')
			cursor = db.cursor()
		except MySQLdb.Error,e:
			print self.gettime(),'failed to connect database...reason:',e
		cursor.execute('DROP TABLE IF EXISTS %s'%self.table_name)
		return db,cursor
	def output_db(self):
		print self.gettime(),'creating table...'
		sql = """CREATE TABLE IF NOT EXISTS %s(
			`id` int PRIMARY KEY AUTO_INCREMENT,
			`title` TEXT,
			`url` VARCHAR(255),
			`description` TEXT)DEFAULT CHARSET=utf8;"""%self.table_name
		try:
			self.cur.execute(sql)
		except MySQLdb.Error,e:
			print self.gettime(),'creating table failed...reason:',e

		print self.gettime(),'insert data...'
		for data in self.datas:
			self.db.set_character_set('utf8')
 			title = MySQLdb.escape_string(data['title'])
 			url = MySQLdb.escape_string(data['url'])
 			description = MySQLdb.escape_string(data['description'])
			sql2 = "INSERT INTO %s(title,url,description)VALUES ('%s','%s','%s')"%(self.table_name,title,url,description)
			try:
				result = self.cur.execute(sql2)
				if result:
					print 'insert NO.%d data'%self.db.insert_id()
				else:
					print 'rolling back..................'
					self.db.rollback()
				self.db.commit()
				# if self.db.insert_id()>=200:
				# 	return 'stop'
				# else:
				# 	return 'continue'
			except MySQLdb.Error,e:
				print self.gettime(),'insert data error...reason:',e
		# f =open('test.log','w+')
		# sys.stdout = f
		# for i in range(1,2):
		# 	sql = "SELECT * FROM %s WHERE `id` = '%d'" %(table_name,i)

		# 	cursor.execute(sql)
		# 	results = cursor.fetchone()
		# 	print i,results[1]
		# 	print results[2]
		# 	print results[3]
		# 	print '======================================='
		
