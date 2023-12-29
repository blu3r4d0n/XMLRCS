#!/usr/bin/python
import sys
import pprint
import logging
import json
import redis
import os
from sseclient import SSEClient as EventSource
from xml.sax.saxutils import quoteattr

url = 'https://stream.wikimedia.org/v2/stream/recentchange'
rs = redis.Redis('localhost')

# Store pid of this daemon so that we can easily kill it in case we start
# hitting https://phabricator.wikimedia.org/T179986
rs.set("es2r.pid", int(os.getpid()))

def insert_to_redis(wiki, xml):
    result = wiki + "|" + xml
    rs.rpush('rc', result);
    return True;
while True:
    try:
        for event in EventSource(url):
            if event.event == 'message':
                try:
                    change = json.loads(event.data)
                except ValueError:
                    pass
                else:
                    if(bool(change)):
                        rev_id = ''
                        patrolled=False
                        length_n=0
                        length_o=0
                        minor=False
                        old = ''
                        if ('revision' in change):
                            rev_id = 'revid="' + str(change['revision']['new']) + '" '
                            if ('old' in change['revision']):
                                old = 'oldid="' + str(change['revision']['old']) + '" '
                        if ('patrolled' in change):
                            patrolled = change['patrolled']
                        if ('minor' in change):
                            minor = change['minor']
                        if ('length' in change):
                            length_n = change['length']['new']
                            if ('old' in change['length']):
                                length_o = change['length']['old']
                        if ('wiki' in change):
                            result = '<edit wiki="' + change['wiki'] + '" '
                        #else: 
                            #print(change)
                        if ('server_name' in change): 
                            result += 'server_name="' + change['server_name'] + '" '
                        result += rev_id + old
                        if ('comment' in change):
                            result += 'summary=' + quoteattr(change['comment']) + ' '
                        if('title' in change):
                            result += 'title=' +  quoteattr(change['title']) + ' '
                        if('namespace' in change):
                            result += 'namespace="' + str(change['namespace']) + '" '
                        if('user' in change):
                            result += 'user=' + quoteattr(change['user']) + ' '
                        if('bot' in change):
                            result += 'bot="' + str(change['bot']) + '" '
                    result += 'patrolled="' + str(patrolled) + '" '
                    result += 'minor="' + str(minor) + '" '
                    if('type' in change):
                        result += 'type=' + quoteattr(change['type']) + ' '
                        if (change['type'] == 'log'):
                            if ('log_id' in change):
                                result += 'log_id="' + str(change['log_id']) + '" '
                    result += 'length_new="' + str(length_n) + '" '
                    result += 'length_old="' + str(length_o) + '" '
                    if ('log_type' in change):
                        result += 'log_type=' + quoteattr(change['log_type']) + ' '
                    if ('log_action' in change):
                        result += 'log_action=' + quoteattr(change['log_action']) + ' '
                    if ('log_action_comment' in change):
                        result += 'log_action_comment=' + quoteattr(change['log_action_comment']) + ' '
                    if('timestamp' in change):
                        result += 'timestamp="' + str(change['timestamp']) + '">'
                    result += '</edit>'
                    if('server_name' in change):
                        insert_to_redis(change['server_name'], result)
    except Exception as e:
       print(f"An error occurred: {e}")
       continue
            
