#!/usr/bin/env python

from mqtt import *

from twisted.internet.protocol import Factory
from twisted.internet import reactor

online_users = []

CONNACK_ACCEPTED = b'\x00'
GRANTED_QOS = 1
""" QoS 1 is used in this implementation """
PORT = 1982

class MQHandler (MQTTProtocol):
    """MQTT broker connection handler.
    """

    def connectReceived(self, clientID, keepalive, willTopic, willMessage,
                                     willQoS, willRetain, cleanStart):
        logging.debug('Connect received, client name is: %s' %clientID)
        for user in online_users:
		    if clientID is user.clientID:
			    logging.info('User %s is already online' %clientID)
			    return
		 
    	self.connack(CONNACK_ACCEPTED)
    	self.clientID = clientID
    	self.keepalive = keepalive
    	self.willTopic = willTopic
    	self.willMessage = willMessage
    	self.willQoS = willQoS
    	self.willRetain = willRetain
    	self.cleanStart = cleanStart
    	if self not in online_users:
	    	online_users.append(self)
        logging.info('User %s added to online_users' %self.clientID)
    
    def disconnectReceived(self):
        logging.debug('Disconnect received from %s' %(self.clientID))
    	online_users.remove(self)
    	logging.info('Removed %s from online_users' %(self.clientID))

    def connectionLost(self, reason):
        logging.debug('TCP teardown with %s' %(self.clientID))
        if self in online_users:
            online_users.remove(self)
            logging.info('Removed %s from online_users' %(self.clientID))


    def subscribeReceived(self, topics, messageId):
        newtopics = [i[0] for i in topics[::2]]
        # topics are like {'topic','QoS'}
        logging.debug('subscribeReceived on topics: %s'%newtopics)
        for topic in newtopics:
	    	if topic in self.topics:
		    	logging.warning("User %s sent dublicate subscribe on topic %s" %(self.clientID, topic))
	    	else:
		    	self.topics.append(topic)
    	self.suback(len(topics), messageId)
        logging.debug('Sent suback for topics: %s' %self.topics)
    	logging.debug('Now user %s is subscribed to topics %s' %(self.clientID, self.topics))


    def publishReceived(self, topic, message, qos,
                        dup, retain, messageId):
        logging.debug('Publish received (%s bytes) from %s on topic %s ' %(len(message),
                                                            self.clientID, topic))
        self.puback(messageId)
        for user in online_users:
		    logging.debug('user %s topics are: %s' %(user.clientID,user.topics))
		    if topic in user.topics:
			    logging.debug('Found topic %s in %s' %(topic, user.topics))
			    logging.debug('Sending publish (%s bytes) to %s' %(len(message), user.clientID))
			    user.publish(topic, message, qosLevel=GRANTED_QOS, retain=False, dup=False, messageId=None)

    def pingreqReceived(self):
        logging.debug("Pingreq received from %s" %(self.clientID))
        self.pingresp()
        logging.debug("Pingresp send back to %s" %(self.clientID))


class ChatFactory (Factory):
    def buildProtocol (self, addr):
        return MQHandler()

if __name__ == '__main__':
    reactor.listenTCP(PORT, ChatFactory())
    print 'Started server at: *:%s' %PORT
    reactor.run()

