import random
import time
import logging

import requests

def urljoin(*args):
	return '/'.join(args)


class EtcdException(Exception):
	def __init__(self, response):
		self.__dict__.update(response)


class Client(object):
	CONNECT_RETRY_INTERVAL = 5
	CONNECT_RETRY_RANDOM_FACTOR = 0.2

	def __init__(self, servers, log=None):
		"""Servers should be a list of connection specs like "http://localhost:4001"
		log can be a Logger or a name to create a Logger from, overriding the default logger.
		"""
		if not log:
			log = 'syncrony.etcd'
		if not isinstance(log, logging.Logger):
			log = logging.getLogger(log)
		self.servers = servers
		self.log = log

	def request(self, method, path, **kwargs):
		version = kwargs.pop('version', 2)
		version = 'v' + str(version).lstrip('v')
		prefix = kwargs.pop('prefix', 'keys')

		# etcd expects boolean values as 'true' and 'false', not 'True' and 'False'
		# as requests casts them.
		if 'params' in kwargs and isinstance(kwargs['params'], dict):
			params = kwargs['params'].copy()
			for k, v in params.items():
				if v in (True, False):
					params[k] = 'true' if v else 'false'
			kwargs['params'] = params

		while True:
			for server in self.servers:
				url = urljoin(server, version, prefix, path)
				self.log.debug("{method} {url} with args {kwargs}".format(
					method=method, url=url, kwargs=kwargs
				))
				try:
					response = requests.request(method, url, **kwargs)
				except requests.ConnectionError:
					self.log.warning("Connection error when making request", exc_info=True)
					continue
				response.raise_for_status()
				response = response.json()
				self.log.debug("Got response: %s", response)
				if 'errorCode' in response:
					raise EtcdException(response)
				return response
			# we tried all servers and can't contact any of them!
			# wait and try again
			# XXX proper backoff
			interval = self.CONNECT_RETRY_INTERVAL
			factor = self.CONNECT_RETRY_RANDOM_FACTOR
			interval += interval * (random.random() * factor*2 - factor) # +/- up to factor times interval
			self.log.warning("Could not contact any servers. Waiting for %.2f seconds", interval)
			time.sleep(interval)

	def get(self, path, recursive=False, sorted=False, wait=False, **kwargs):
		"""wait can be False, True or integer index."""
		params = dict(
			recursive = recursive,
			sorted = sorted,
			wait = wait,
		)
		if wait not in (True, False):
			params['waitIndex'] = int(wait)
			params['wait'] = True
		return self.request('GET', path, params=params, **kwargs)

	def set(self, path, value, prev_value=None, ttl=None, **kwargs):
		"""prev_value can be:
			False: require node to not exist
			True: require node to exist
			string: require node to have given value
			int: require node to have given integer index
		Note that you MUST cast to string to prevent a misinterpretation.
		If value is set to None, a delete is performed instead.
		"""
		params = {}
		if ttl is not None: params['ttl'] = ttl

		if prev_value in (True, False):
			params['prevExist'] = prev_value
		elif isinstance(prev_value, basestring):
			params['prevValue'] = prev_value
		elif prev_value is not None:
			params['prevIndex'] = prev_value

		if value is None:
			method = 'DELETE'
			if ttl is not None:
				raise TypeError("Cannot combine a delete with a ttl")
			if 'prevExist' in params:
				raise TypeError("Cannot combine a delete with prev_value = True or False")
		else:
			method = 'PUT'
			params['value'] = value

		return self.request(method, path, params=params, **kwargs)

	def in_order_create(self, path, value, **kwargs):
		params = {'value': value}
		return self.request('POST', path, params=params, **kwargs)

	def mkdir(self, path, ttl=None, **kwargs):
		params = {'dir': True}
		if ttl is not None: params['ttl'] = ttl
		return self.request('PUT', path, params=params, **kwargs)

	def rmdir(self, path, recursive=False, **kwargs):
		params = {'dir': True, 'recursive': recursive}
		return self.request('DELETE', path, params=params, **kwargs)

	def watch_iter(self, path):
		"""Returns an iterator that yields all events that occur to watched path.
		Note that if the path does not exist, it will block until it does.
		"""
		index = False # initial value means no wait on first get
		while True:
			result = self.get(path, wait=index)
			yield result
			index = result['node']['modifiedIndex'] + 1
