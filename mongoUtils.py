# -*- coding: utf-8 -*-
# @Author: uevol
# @Date:   2018-05-15 14:25:50
# @Last Modified by:   uevol
# @Last Modified time: 2018-05-20 19:48:13

from pymongo import MongoClient
import pprint

class Mcube(object):
	"""docstring for Mcube"""
	def __init__(self, user=None, password=None):
		self.__monitorUser = user
		self.__monitorUserPass = password

	def get_mongo_client(self, host=None, port=None):
		try:
			if self.__monitorUser and self.__monitorUserPass:
				client = MongoClient(host, port, username=self.__monitorUser, password=self.__monitorUserPass, serverSelectionTimeoutMS=5000)
			else:
				client = MongoClient(host, port, serverSelectionTimeoutMS=5000)
		except Exception as e:
			return e, None
		return None, client

	# standalone, mongos, primary
	def get_mongo_role(self, mongoClient):
		try:
			if mongoClient.is_mongos:
				mongo_role = 'mongos'
			elif mongoClient.primary:
				mongo_role = 'primary'
			else:
				mongo_role = 'standalone'
		except Exception as e:
			return e, None
		return None, mongo_role

	def get_cluster_info(self, mongoClient, clusterId):
		try:
			cluster_info = {'clusterId': clusterId}
			err, role = self.get_mongo_role(mongoClient)
			if role == 'standalone':
				cluster_info['type'] = 'standalone'
				ip, port = mongoClient.address
				cluster_info['members'] = {'ip': ip, 'port': port}
			elif role == 'primary':
				cluster_info['type'] = 'replication'
				cluster_info['members'] = self.get_replMembers(mongoClient)[1]
			elif role == 'mongos':

				cluster_info['type'] = 'shard'
				cluster_info['members'] = {'mongos': [], 'configdb': {}, 'shards': []}

				# get mongos
				mongoses = mongoClient.config.mongos.find()
				for mongos in mongoses:
					ip, port = mongos['_id'].split(':')
					cluster_info['members']['mongos'].append({'ip': ip, 'port': port})

				# get configdb
				configDbs = mongoClient.admin.command("getCmdLineOpts")['parsed']['sharding']['configDB'].split('/')
				ip, port = configDbs[1].split(',')[0].split(':')
				
				cluster_info['members']['configdb']['set'] = configDbs[0]
				client = self.get_mongo_client(ip, int(port))[1]
				cluster_info['members']['configdb']['members'] = self.get_replMembers(client)[1]
							
				# get shards 
				shards = self.get_shardMembers(mongoClient)[1]
				for shard in shards:
					repl = {}
					hosts = shard['host'].split('/')[1].split(',')
					ip, port = hosts[0].split(':')
					client = self.get_mongo_client(ip, int(port))[1]
					repl['set'] = shard['_id']
					repl['members'] = self.get_replMembers(client)[1]
					cluster_info['members']['shards'].append(repl)
			else:
				return err, None
		except Exception as e:
			raise e
			return e, None
		return None, cluster_info

	def get_serverStatus(self, mongoClient):
		try:
			serverStatus = mongoClient.admin.command("serverStatus")
			data = {}

			# base info
			data['host'] = serverStatus['host']
			data['version'] = serverStatus['version']
			data['uptime'] = serverStatus['uptime']
			data['localtime'] = serverStatus['localtime']

			# troughput info
			data['opcounters_query'] = serverStatus['opcounters']['query']
			data['opcounters_getmore'] = serverStatus['opcounters']['getmore']
			data['opcounters_insert'] = serverStatus['opcounters']['insert']
			data['opcounters_update'] = serverStatus['opcounters']['update']
			data['opcounters_delete'] = serverStatus['opcounters']['delete']

			if serverStatus.has_key('globalLock'):
				data['globalLock_activeClients_readers'] = serverStatus['globalLock']['activeClients']['readers']
				data['globalLock_activeClients_writers'] = serverStatus['globalLock']['activeClients']['writers']
				data['globalLock_currentQueue_readers'] = serverStatus['globalLock']['currentQueue']['readers']
				data['globalLock_currentQueue_writers'] = serverStatus['globalLock']['currentQueue']['writers']

			# Locking
			if serverStatus.has_key('wiredTiger'):
				data['wiredTiger_concurrentTransactions_read_out'] = serverStatus['wiredTiger']['concurrentTransactions']['read']['out']
				data['wiredTiger_concurrentTransactions_write_out'] = serverStatus['wiredTiger']['concurrentTransactions']['write']['out']
				data['wiredTiger_concurrentTransactions_read_available'] = serverStatus['wiredTiger']['concurrentTransactions']['read']['available']
				data['wiredTiger_concurrentTransactions_read_available'] = serverStatus['wiredTiger']['concurrentTransactions']['read']['available']

				# cahche
				data['wiredTiger_cache_bytes_currently_in_the_cache'] = serverStatus['wiredTiger']['cahche']['bytes currently in the cache']
				data['wiredTiger_cache_maximum_bytes_configured'] = serverStatus['wiredTiger']['cahche']['maximum bytes configured']
				data['wiredTiger_cache_tracked_dirty_bytes_in_the_cache'] = serverStatus['wiredTiger']['cahche']['tracked  dirty bytes in the cache']
				data['wiredTiger_cache_unmodified_pages_evicted'] = serverStatus['wiredTiger']['cahche']['unmodified pages evicted']
				data['wiredTiger_cache_modified_pages_evicted'] = serverStatus['wiredTiger']['cahche']['modified pages evicted']
				data['wiredTiger_cache_eviction_calls_to_get_a_page'] = serverStatus['wiredTiger']['cahche']['eviction calls to get a page']
				data['wiredTiger_cache_eviction_calls_to_get_a_page_found_queue_empty'] = serverStatus['wiredTiger']['cahche']['eviction calls to get a page found queue empty']
				
			# cursors
			data['metrics_cursor_open_total'] = serverStatus['metrics']['cursor']['open']['total']
			data['metrics_cursor_timedOut'] = serverStatus['metrics']['cursor']['timedOut']
			if serverStatus['metrics']['cursor']['open'].has_key('noTimeout'):
				data['metrics_cursor_open_noTimedOut'] =  serverStatus['metrics']['cursor']['open']['noTimedOut']

			# connections
			data['connections_current'] = serverStatus['connections']['current']
			data['connections_available'] = serverStatus['connections']['available']

			# memery
			if serverStatus.has_key('mem'):
				data['mem_virtual'] = serverStatus['mem']['virtual']
				data['mem_resident'] = serverStatus['mem']['resident']
				data['extra_info_page_faults'] = serverStatus['extra_info']['page_faults']

			# error
			data['asserts_msg'] = serverStatus['asserts']['msg']
			data['asserts_warning'] = serverStatus['asserts']['warning']
			data['asserts_regular'] = serverStatus['asserts']['regular']
			data['asserts_user'] = serverStatus['asserts']['user']

		except Exception as e:
			return e, None
		return None, data

	def get_dbStats(self, mongoClient):
		try:
			dbs = mongoClient.admin.command("listDatabases")
			dbs_info = {}
			for db in dbs:
				if db not in ['admin', 'local', 'config']:
					dbs_info[db] = mongoClient[db].command('dbStats')
			return None, dbs_info
		except Exception as e:
			return e, None
		finally:
			mongoClient.close()

	def get_replMembers(self, mongoClient):
		try:
			members = mongoClient.admin.command("replSetGetStatus")['members']
			data = []
			for member in members:
				member_info = {}
				ip, port = member['name'].split(':')
				member_info['ip'] = ip
				member_info['port'] = port
				member_info['stateStr'] = member['stateStr']
				data.append(member_info)
			return None, data
		except Exception as e:
			return e, None
		finally:
			mongoClient.close()

	def get_replStatus(self, mongoClient):
		try:
			replStatus = mongoClient.admin.command("replSetGetStatus")
			return None, replStatus
		except Exception as e:
			return e, None
		finally:
			mongoClient.close()
		
	def get_shardMembers(self, mongoClient):
		try:
			shards = mongoClient.admin.command('listShards')['shards']
			return None, shards
		except Exception as e:
			return e, None
		finally:
			mongoClient.close()

	def get_clusterStatus(self, clusterInfo):
		try:
			pass
		except Exception as e:
			raise e

if __name__ == '__main__':
	mcube_ins = Mcube()
	err, mongoClient = mcube_ins.get_mongo_client('localhost', 27017)
	if not err:
		cluster_info = mcube_ins.get_cluster_info(mongoClient, '123456789')[1]
		mongoClient.close()
		err, mongoClient = mcube_ins.get_mongo_client('localhost', 12345)
		if not err:
			mongoClient.mcube.cluster.replace_one({'clusterId': '123456789'}, cluster_info, upsert=True)
			mongoClient.close()
	else:
		print err