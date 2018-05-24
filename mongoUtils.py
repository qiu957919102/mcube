# -*- coding: utf-8 -*-
# @Author: uevol
# @Date:   2018-05-15 14:25:50
# @Last Modified by:   wy
# @Last Modified time: 2018-05-24 23:18:25

import pymongo
from pymongo import MongoClient

def get_mongo_client(host=None, port=None, user=None, password=None, sst=3000):
    try:
        if user and password:
            client = MongoClient(host, int(port), username=user, password=password, serverSelectionTimeoutMS=sst)
        else:
            client = MongoClient(host, int(port), serverSelectionTimeoutMS=sst)
        client.admin.command('ping')
    except Exception as e:
        # print repr(e), 'get_mongo_client'
        client = None
    finally:
        return client

# standalone, mongos, primary
def get_mongo_role(mongoClient):
    try:
        if mongoClient.is_mongos:
            role = 'mongos'
        elif 'oplog.rs' in mongoClient.local.collection_names():
            if mongoClient.is_primary:
                role = 'primary'
            else:
                role = 'secondary'
        else:
            role = 'standalone'
    except Exception as e:
        # print repr(e), 'get_mongo_role'
        # raise e
        role = None
    finally:
        return role

def get_replMembers(mongoClient):
    try:
        data = []
        members = mongoClient.admin.command("replSetGetStatus")['members']
        for member in members:
            member_info = {}
            ip, port = member['name'].split(':')
            member_info['ip'] = ip
            member_info['port'] = port
            member_info['stateStr'] = member['stateStr']
            data.append(member_info)
    except Exception as e:
        # print repr(e), 'get_replMembers'
        # raise e
        data = None
    finally:
        mongoClient.close()
        return data

def get_shardMembers(mongoClient):
        try:
            shards = mongoClient.admin.command('listShards')['shards']
        except Exception as e:
            # print repr(e), 'get_shardMembers'
            # raise e
            shards = None
        finally:
            mongoClient.close()
            return shards 

def get_cluster(mongoClient, clusterId):
    try:
        cluster_info = {'clusterId': clusterId}
        role = get_mongo_role(mongoClient)
        if role == 'standalone':
            cluster_info['type'] = 'standalone'
            ip, port = mongoClient.address
            cluster_info['members'] = {'ip': ip, 'port': port}
        elif role in ['primary','secondary']:
            cluster_info['type'] = 'replication'
            cluster_info['members'] = get_replMembers(mongoClient)
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
            client = get_mongo_client(ip, port)
            cluster_info['members']['configdb']['members'] = get_replMembers(client) if client else None
                        
            # get shards 
            shards = get_shardMembers(mongoClient)
            for shard in shards:
                repl = {}
                hosts = shard['host'].split('/')[1].split(',')
                ip, port = hosts[0].split(':')
                client = get_mongo_client(ip, port)
                repl['set'] = shard['_id']
                repl['members'] = get_replMembers(client) if client else None
                cluster_info['members']['shards'].append(repl)
        else:
            cluster_info = None
    except Exception as e:
        # print repr(e), 'get_cluster'
        # raise e
        cluster_info = None
    return cluster_info

def get_serverStatus(mongoClient):
    try:
        data = {}
        serverStatus = mongoClient.admin.command("serverStatus")

        # base info
        data['host'] = serverStatus['host']
        data['version'] = serverStatus['version']
        data['uptime'] = serverStatus['uptime']
        data['localTime'] = serverStatus['localTime']

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

            # cache
            data['wiredTiger_cache_bytes_currently_in_the_cache'] = serverStatus['wiredTiger']['cache']['bytes currently in the cache']
            data['wiredTiger_cache_maximum_bytes_configured'] = serverStatus['wiredTiger']['cache']['maximum bytes configured']
            data['wiredTiger_cache_tracked_dirty_bytes_in_the_cache'] = serverStatus['wiredTiger']['cache']['tracked dirty bytes in the cache']
            data['wiredTiger_cache_unmodified_pages_evicted'] = serverStatus['wiredTiger']['cache']['unmodified pages evicted']
            data['wiredTiger_cache_modified_pages_evicted'] = serverStatus['wiredTiger']['cache']['modified pages evicted']
            data['wiredTiger_cache_eviction_calls_to_get_a_page'] = serverStatus['wiredTiger']['cache']['eviction calls to get a page']
            data['wiredTiger_cache_eviction_calls_to_get_a_page_found_queue_empty'] = serverStatus['wiredTiger']['cache']['eviction calls to get a page found queue empty']
            
        # cursors
        data['metrics_cursor_open_total'] = serverStatus['metrics']['cursor']['open']['total']
        data['metrics_cursor_timedOut'] = serverStatus['metrics']['cursor']['timedOut']
        if serverStatus['metrics']['cursor']['open'].has_key('noTimeout'):
            data['metrics_cursor_open_noTimedout'] =  serverStatus['metrics']['cursor']['open']['noTimeout']

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
        # print repr(e), 'get_serverStatus'
        # raise e
        data = None
    finally:
        return data

def get_dbStats(mongoClient):
    try:
        dbs_info = {}
        dbs = mongoClient.database_names()
        for db in dbs:
            if db not in ['admin', 'local', 'config']:
                dbs_info[db] = mongoClient[db].command('dbStats')
    except Exception as e:
        # print repr(e), 'get_dbStats'
        # raise e
        dbs_info = None
    finally:
        mongoClient.close()
        return dbs_info

def get_replStatus(mongoClient):
    try:
        repl = {}
        oplog_collection = mongoClient.local["oplog.rs"]
        oplogrs_collstats = mongoClient.local.command("collstats", "oplog.rs")

        # oplog size
        repl['oplog_size'] = oplogrs_collstats['maxSize']/(1024*1024)

        # log length start to end
        oplog_tFirst =   oplog_collection.find({},{"ts":1}).sort('$natural',pymongo.ASCENDING).limit(1).next()
        oplog_tLast = oplog_collection.find({},{"ts":1}).sort('$natural',pymongo.DESCENDING).limit(1).next()
        log_length_start_to_end = oplog_tLast["ts"].time - oplog_tFirst["ts"].time
        repl['log_length_start_to_end'] = log_length_start_to_end
        repl['log_length_start_to_end_hour'] = round(log_length_start_to_end/3600.0, 2)

        # caculate replication lag
        master_optime = 0
        secondary_optime = 0
        members = mongoClient.admin.command("replSetGetStatus")['members']
        for member in members:
            if member['stateStr'] == 'PRIMARY':
                master_optime = member['optime']['ts'].time
            elif member.has_key('self') and member['stateStr'] == 'SECONDARY':
                secondary_optime = member['optime']['ts'].time
            else:
                pass
        repl['relication_lag'] = master_optime - secondary_optime   
    except Exception as e:
        # print repr(e), 'get_replStatus'
        # raise e
        repl = None
    finally:
        mongoClient.close()
        return repl

def get_clusterStatus(cluster, mongoClient):
    try:
        if cluster['type'] == 'standalone':
            client = get_mongo_client(cluster['members']['ip'], cluster['members']['port'])
            if client:
                serverStatus = get_serverStatus(client)
                serverStatus['dbStats'] = get_dbStats(client)
                mongoClient.mcube.monitorData.insert_one(serverStatus)
        elif cluster['type'] == 'replication':
            members = cluster['members']
            for member in members:
                client = get_mongo_client(member['ip'], member['port'])
                if client:
                    serverStatus = get_serverStatus(client)
                    serverStatus['dbStats'] = get_dbStats(client)
                    serverStatus['replSetStatus'] = get_replStatus(client)
                    mongoClient.mcube.monitorData.insert_one(serverStatus)
        elif cluster['type'] == 'shard':
            configdbs = cluster['members']['configdb']['members']
            mongoses = cluster['members']['mongos']
            shards = cluster['members']['shards']
            for member in configdbs:
                client = get_mongo_client(member['ip'], member['port'])
                if client:
                    serverStatus = get_serverStatus(client)
                    serverStatus['dbStats'] = get_dbStats(client)
                    serverStatus['replSetStatus'] = get_replStatus(client)
                    mongoClient.mcube.monitorData.insert_one(serverStatus)
            for mongos in mongoses:
                client = get_mongo_client(mongos['ip'], mongos['port'])
                if client:
                    serverStatus = get_serverStatus(client)
                    mongoClient.mcube.monitorData.insert_one(serverStatus)
            for shard in shards:
                members = shard['members']
                for member in members:
                    client = get_mongo_client(member['ip'], member['port'])
                    if client:
                        serverStatus = get_serverStatus(client)
                        serverStatus['dbStats'] = get_dbStats(client)
                        serverStatus['replSetStatus'] = get_replStatus(client)
                        mongoClient.mcube.monitorData.insert_one(serverStatus)                
    except Exception as e:
        # print repr(e), 'get_clusterStatus'
        pass


def mongo_ping(cluster, mongoClient):
    try:
        if cluster['type'] == 'standalone':
            client = get_mongo_client(cluster['members']['ip'], cluster['members']['port'], sst=1000)
            if client:
                mongoClient.mcube.aliveCheck.insert_one({'clusterId': cluster['clusterId'], 'ip': cluster['members']['ip'], 'port': cluster['members']['port'], 'alive': True})
            else:
                mongoClient.mcube.aliveCheck.insert_one({'clusterId': cluster['clusterId'], 'ip': cluster['members']['ip'], 'port': cluster['members']['port'], 'alive': False})
        elif cluster['type'] == 'replication':
            members = cluster['members']
            for member in members:
                client = get_mongo_client(member['ip'], member['port'], sst=1000)
                if client:
                    mongoClient.mcube.aliveCheck.insert_one({'clusterId': cluster['clusterId'], 'ip': member['ip'], 'port': member['port'], 'alive': True})
                else:
                    mongoClient.mcube.aliveCheck.insert_one({'clusterId': cluster['clusterId'], 'ip': member['ip'], 'port': member['port'], 'alive': False})
        elif cluster['type'] == 'shard':
            configdbs = cluster['members']['configdb']['members']
            mongoses = cluster['members']['mongos']
            shards = cluster['members']['shards']
            for member in configdbs:
                client = get_mongo_client(member['ip'], member['port'], sst=1000)
                if client:
                    mongoClient.mcube.aliveCheck.insert_one({'clusterId': cluster['clusterId'], 'ip': member['ip'], 'port': member['port'], 'alive': True})
                else:
                    mongoClient.mcube.aliveCheck.insert_one({'clusterId': cluster['clusterId'], 'ip': member['ip'], 'port': member['port'], 'alive': False})
            for mongos in mongoses:
                client = get_mongo_client(mongos['ip'], mongos['port'], sst=1000)
                if client:
                    mongoClient.mcube.aliveCheck.insert_one({'clusterId': cluster['clusterId'], 'ip': mongos['ip'], 'port': mongos['port'], 'alive': True})
                else:
                    mongoClient.mcube.aliveCheck.insert_one({'clusterId': cluster['clusterId'], 'ip': mongos['ip'], 'port': mongos['port'], 'alive': False})
            for shard in shards:
                members = shard['members']
                for member in members:
                    client = get_mongo_client(member['ip'], member['port'], sst=1000)
                    if client:
                        mongoClient.mcube.aliveCheck.insert_one({'clusterId': cluster['clusterId'], 'ip': member['ip'], 'port': member['port'], 'alive': True})
                    else:
                        mongoClient.mcube.aliveCheck.insert_one({'clusterId': cluster['clusterId'], 'ip': member['ip'], 'port': member['port'], 'alive': False})
    except Exception as e:
        pass

if __name__ == '__main__':
    # clusters = [{'id': 001, 'ip': 'localhost', 'port': 12345}, {'id': 002, 'ip': 'localhost', 'port': 27017}, {'id': 003, 'ip': 'localhost', 'port': 30000}]
    # for cluster in clusters:
    #     mongoClient = mcube_ins.get_mongo_client(cluster['ip'], cluster['port'])
    #     if mongoClient:
    #         cluster_info = mcube_ins.get_cluster(mongoClient, cluster['id'])
    #         mongoClient.close()
    #         mongoClient = mcube_ins.get_mongo_client('localhost', 12345)
    #         if mongoClient:
    #             mongoClient.mcube.cluster.replace_one({'clusterId': cluster['id']}, cluster_info, upsert=True)
    #             mongoClient.close()
    pass









