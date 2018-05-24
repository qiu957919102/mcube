# -*- coding: utf-8 -*-
# @Author: wy
# @Date:   2018-05-23 22:47:52
# @Last Modified by:   wy
# @Last Modified time: 2018-05-24 23:18:46

import threading
from multiprocessing import Process
import time
from mongoUtils import *
from config import configs


def aliveCheck(host, port, user, password, interval):
	mongoClient = get_mongo_client(host, port, user, password)
	if mongoClient:
		while 1:
			clusters = mongoClient.mcube.cluster.find()
			if clusters:
				for cluster in clusters:
					t = threading.Thread(target=mongo_ping, args=(cluster, mongoClient))
					t.start()
			time.sleep(int(interval))
	else:
		print "mongoClient is None, please check mongo config"


def monitor_db(host, port, user, password, interval):
	mongoClient = get_mongo_client(host, port, user, password)
	if mongoClient:
		while 1:
			clusters = mongoClient.mcube.cluster.find()
			if clusters:
				for cluster in clusters:
					t = threading.Thread(target=get_clusterStatus, args=(cluster, mongoClient))
					t.start()
			time.sleep(int(interval))
	else:
		print "mongoClient is None, please check mongo config"	

                    
if __name__ == "__main__":

	local_db_host = configs['local_db']['host']
	local_db_port = configs['local_db']['port']
	local_user = configs['local_db']['user']
	local_pass  = configs['local_db']['pass']
	aliveCheckInterval = configs['interval']['aliveCheck']
	monitorDataInterval = configs['interval']['getData']

	p1 = Process(target=aliveCheck, args=(local_db_host, local_db_port, local_user, local_pass, aliveCheckInterval))
	p1.start()
	p2 = Process(target=monitor_db, args=(local_db_host, local_db_port, local_user, local_pass, monitorDataInterval))
	p2.start()





















