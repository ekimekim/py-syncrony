from setuptools import setup

setup(
	name='syncrony',
	version='0.1',
	description='Distributed systems primitives using etcd',
	author='Michael Lang',
	author_email='mikelang3000@gmail.com',
	url='http://github.com/ekimekim/py-syncrony',
	packages=['syncrony'],
	install_requires=[
		'requests',
	]
)
