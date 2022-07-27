import os
import random
import string
import time
import urllib.request
from proxy_checking import ProxyChecker

from smart_store_crawler import config
from smart_store_crawler import commonLib
from smart_store_crawler.config import LOGLEVEL

class proxyManager():
	def __init__(self, isLoadProxy):
		self.splited = []
		self.current_proxy = ''
		self.failed_count = 0
		self.isLoadProxy = isLoadProxy

		if isLoadProxy:
			self.loadProxy()

	def loadProxy(self):
		if os.path.exists(config.CFG_PROXY_FILENAME):
			del self.splited

			commonLib.print_log(LOGLEVEL.D, "proxyManager.loadProxy : " + config.CFG_PROXY_FILENAME)

			data = open(config.CFG_PROXY_FILENAME, "r").read()
			self.splited = data.split("\n")

	def getCheckedRandomProxy(self):
		while True:
			if self.isLoadProxy and self.failed_count > 20:
				self.loadProxy()

			proxy_server = random.choice(self.splited).strip()

			if self.isLiveProxy2(proxy_server):
				self.failed_count = 0
				break

			self.failed_count += 1
			commonLib.print_log(LOGLEVEL.D, "proxyManager.getCheckedRandomProxy Dead Proxy : " + proxy_server)

		#commonLib.print_log(LOGLEVEL.D, "proxyManager.getCheckedRandomProxy : " + proxy_server)

		return proxy_server
	def isLoadedProxy(self):
		return self.isLoadProxy and len(self.splited) > 0

	def changeProxy(self):
		if len(self.splited) == 0:
			self.current_proxy = None
			return

		self.current_proxy = {'http': 'http://' + self.getCheckedRandomProxy()}

	def getCurrentProxy(self):
		return self.current_proxy

	def isLiveProxy(self, proxy_server):
		if proxy_server == '':
			return False

		try:
			checker = ProxyChecker()
			res = checker.check_proxy(proxy_server)

			return res['status'] == True
		except:
			pass

		return False

	def isLiveProxy2(self, pip):
		try:
			proxy_handler = urllib.request.ProxyHandler({'http': pip})
			opener = urllib.request.build_opener(proxy_handler)
			opener.addheaders = [('User-agent', 'Mozilla/5.0')]
			urllib.request.install_opener(opener)		
			req=urllib.request.Request('http://www.google.com')
			sock=urllib.request.urlopen(req, timeout=3)
		except:
			return False

		return True
