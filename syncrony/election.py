import uuid
import logging

import eventlet

import etcd


class Election(object):
	"""Elects a leader between a group of participants.
	To enter yourself as a candidate:
		election.run()
	To check if you are currently the leader:
		election.is_leader
	To block until you are the leader:
		election.wait_for_leader()
	To stop trying to be leader (or gracefully step down):
		election.cancel()
	"""

	servers = ['http://localhost:4001']
	ttl = 15
	interval = 5

	is_leader = False

	# runner runs in background and watches the node / refreshes the lock
	_runner = None
	# watchdog ensures that if something goes wrong, is_leader is set False after ttl
	_watchdog = None

	def __init__(self, path, servers=None, ttl=None, interval=None, identifier=None, log=None):
		"""Create a new Election at the given backend path.
		All participants must use the same path to participate in the same election.
		Options:
			servers: A list of servers to connect to.
			ttl: Time in seconds that this leader must be unresponsive before releasing lock. Default 15.
			interval: Time in seconds between updating lock. Should be less than ttl. Default 5.
			identifier: String identifying this node. Defaults to a randomized uuid.
			            There's no need to set this unless you want other nodes to be able to recognize who has leadership.
			log: Either a Logger object to log to, or a logger name to create a Logger from.
		"""

		if not log:
			log = 'syncrony.election'
		if not isinstance(log, logging.Logger):
			log = logging.getLogger(log)
		self.log = log

		self.path = path
		if servers is not None: self.servers = servers
		if ttl is not None: self.ttl = ttl
		if interval is not None: self.interval = interval
		self.identifier = identifier or str(uuid.uuid4())
		self.connect()

	def connect(self):
		self.conn = etcd.Client(self.servers)

	def run(self):
		if not self._runner:
			self._runner = eventlet.spawn(self._run)

	def _run(self):
		if self._write(False):
			self._set_leader(True)
		while True:
			if self.is_leader:
				eventlet.sleep(self.interval)
				self._renew()
			else:
				self._try_become_leader()

	def _on_watchdog(self):
		self.log.warning("Setting is_leader false after {} seconds - "
		                 "is something preventing renewal?")
		self.is_leader = False

	def _set_leader(self, value):
		if self._watchdog is not None:
			self._watchdog.cancel()
			self._watchdog = None
		self.is_leader = value
		if value:
			self._watchdog = eventlet.spawn_after(self.ttl, self._on_watchdog)
		else:
			self.log.warning("We are no longer the leader!")

	def _write(self, old):
		"""Returns True on successful write, False if old value did not match."""
		try:
			self.conn.set(self.path, self.identifier, prev_value=old, ttl=self.ttl)
		except etcd.EtcdException as ex:
			# XXX should make sure it's the right error code
			return False
		return True

	def _renew(self):
		success = self._write(self.identifier) # only write if we're still leader
		self._set_leader(success)

	def _try_become_leader(self):
		for event in self.conn.watch_iter(self.path):
			if event['action'] not in ('delete', 'expire'): continue
			success = self._write(False)
			if not success: continue
			self._set_leader(True)
			break
