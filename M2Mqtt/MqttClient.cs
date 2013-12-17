using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using uPLibrary.Networking.M2Mqtt.Exceptions;
using uPLibrary.Networking.M2Mqtt.Messages;
// if .Net Micro Framework
#if (MF_FRAMEWORK_VERSION_V4_2 || MF_FRAMEWORK_VERSION_V4_3)
using Microsoft.SPOT;
#if SSL
using Microsoft.SPOT.Net.Security;
#endif
// else other frameworks (.Net, .Net Compact or Mono)
#else
using System.Collections.Generic;
#if SSL
using System.Security.Authentication;
using System.Net.Security;
#endif
#endif

using System.Security.Cryptography.X509Certificates;
using System.Collections;

namespace uPLibrary.Networking.M2Mqtt
{
    /// <summary>
    /// MQTT Client
    /// </summary>
    public class MqttClient
    {
        /// <summary>
        /// Delagate that defines event handler for PUBLISH message received from broker
        /// </summary>
        public delegate void MqttMsgPublishEventHandler(object sender, MqttMsgPublishEventArgs e);

        /// <summary>
        /// Delagate that defines event handler for published message
        /// </summary>
        public delegate void MqttMsgPublishedEventHandler(object sender, MqttMsgPublishedEventArgs e);

        /// <summary>
        /// Delagate that defines event handler for subscribed topic
        /// </summary>
        public delegate void MqttMsgSubscribedEventHandler(object sender, MqttMsgSubscribedEventArgs e);

        /// <summary>
        /// Delagate that defines event handler for unsubscribed topic
        /// </summary>
        public delegate void MqttMsgUnsubscribedEventHandler(object sender, MqttMsgUnsubscribedEventArgs e);

        // default port for MQTT protocol
        public const int MQTT_BROKER_DEFAULT_PORT = 1883;
        public const int MQTT_BROKER_DEFAULT_SSL_PORT = 8883;
        // default timeout on receiving from broker
        public const int MQTT_DEFAULT_TIMEOUT = 5000;
        // max publish, subscribe and unsubscribe retry for QoS Level 1 or 2
        private const int MQTT_ATTEMPTS_RETRY = 3;
        // delay for retry publish, subscribe and unsubscribe for QoS Level 1 or 2
        private const int MQTT_DELAY_RETRY = 10000;

        // CA certificate
        private X509Certificate caCert;

        // broker hostname, ip address and port
        private string brokerHostName;
        private IPAddress brokerIpAddress;
        private int brokerPort;
        // using SSL
        private bool secure;

        // thread for receiving incoming message from broker
        Thread receiveThread;
        bool isRunning;

        // event for signaling receive end from broker
        AutoResetEvent endReceiving;
        // message received from broker
        MqttMsgBase msgReceived;

        // exeption thrown during receiving from broker
        Exception exReceiving;

        // keep alive period (in ms)
        int keepAlivePeriod;
        // thread for sending keep alive message
        Thread keepAliveThread;
        AutoResetEvent keepAliveEvent;
        // keep alive timeout expired
        bool isKeepAliveTimeout;
        // last message sent ticks
        long lastSend;

        // event for PUBLISH messahe received from broker
        public event MqttMsgPublishEventHandler MqttMsgPublishReceived;
        // event for published message
        public event MqttMsgPublishedEventHandler MqttMsgPublished;
        // event for subscribed topic
        public event MqttMsgSubscribedEventHandler MqttMsgSubscribed;
        // event for unsubscribed topic
        public event MqttMsgUnsubscribedEventHandler MqttMsgUnsubscribed;

        /// <summary>
        /// Connection status between client and broker
        /// </summary>
        public bool IsConnected { get; private set; }
        
        // channel to communicate over the network
        private IMqttNetworkChannel channel;

        // current message identifier generated
        private ushort messageIdCounter = 0;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="brokerIpAddress">Broker IP address</param>
        public MqttClient(IPAddress brokerIpAddress) :
            this(brokerIpAddress, MQTT_BROKER_DEFAULT_PORT, false, null)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="brokerIpAddress">Broker IP address</param>
        /// <param name="brokerPort">Broker port</param>
        /// <param name="secure">Using secure connection</param>
        /// <param name="caCert">CA certificate for secure connection</param>
        public MqttClient(IPAddress brokerIpAddress, int brokerPort, bool secure, X509Certificate caCert)
        {
            this.Init(null, brokerIpAddress, brokerPort, secure, caCert);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="brokerHostName">Broker Host Name</param>
        public MqttClient(string brokerHostName) :
            this(brokerHostName, MQTT_BROKER_DEFAULT_PORT, false, null)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="brokerHostName">Broker Host Name</param>
        /// <param name="brokerPort">Broker port</param>
        /// <param name="secure">Using secure connection</param>
        /// <param name="caCert">CA certificate for secure connection</param>
        public MqttClient(string brokerHostName, int brokerPort, bool secure, X509Certificate caCert)
        {
            // throw exceptions to the caller
            IPHostEntry hostEntry = Dns.GetHostEntry(brokerHostName);

            if ((hostEntry != null) && (hostEntry.AddressList.Length > 0))
            {
                // check for the first address not null
                // it seems that with .Net Micro Framework, the IPV6 addresses aren't supported and return "null"
                int i = 0;
                while (hostEntry.AddressList[i] == null) i++;
                this.Init(brokerHostName, hostEntry.AddressList[i], brokerPort, secure, caCert);
            }
            else
                throw new ApplicationException("No address found for the broker");
        }

        /// <summary>
        /// MqttClient initialization
        /// </summary>
        /// <param name="brokerHostName">Broker host name</param>
        /// <param name="brokerIpAddress">Broker IP address</param>
        /// <param name="brokerPort">Broker port</param>
        /// <param name="secure">>Using secure connection</param>
        /// <param name="caCert">CA certificate for secure connection</param>
        private void Init(string brokerHostName, IPAddress brokerIpAddress, int brokerPort, bool secure, X509Certificate caCert)
        {
#if SSL
            // check security parameters
            if ((secure) && (caCert == null))
                throw new ArgumentException("Secure requested but CA certificate is null !");
#else
            if (secure)
                throw new ArgumentException("Library compiled without SSL support");
#endif

            this.brokerHostName = brokerHostName;
            // if broker hostname is null, set ip address
            if (this.brokerHostName == null)
                this.brokerHostName = brokerIpAddress.ToString();

            this.brokerIpAddress = brokerIpAddress;
            this.brokerPort = brokerPort;
            this.secure = secure;
            
#if SSL
            // if secure, load CA certificate
            if (this.secure)
            {
                this.caCert = caCert;
            }
#endif

            this.endReceiving = new AutoResetEvent(false);
            this.keepAliveEvent = new AutoResetEvent(false);
        }

        /// <summary>
        /// Connect to broker
        /// </summary>
        /// <param name="clientId">Client identifier</param>
        /// <returns>Return code of CONNACK message from broker</returns>
        public byte Connect(string clientId)
        {
            return this.Connect(clientId, null, null, false, MqttMsgConnect.QOS_LEVEL_AT_LEAST_ONCE, false, null, null, true, MqttMsgConnect.KEEP_ALIVE_PERIOD_DEFAULT);
        }
        
        /// <summary>
        /// Connect to broker
        /// </summary>
        /// <param name="clientId">Client identifier</param>
        /// <param name="username">Username</param>
        /// <param name="password">Password</param>
        /// <param name="willRetain">Will retain flag</param>
        /// <param name="willQosLevel">Will QOS level</param>
        /// <param name="willFlag">Will flag</param>
        /// <param name="willTopic">Will topic</param>
        /// <param name="willMessage">Will message</param>
        /// <param name="cleanSession">Clean sessione flag</param>
        /// <param name="keepAlivePeriod">Keep alive period</param>
        /// <returns>Return code of CONNACK message from broker</returns>
        public byte Connect(string clientId, 
            string username,
            string password,
            bool willRetain,
            byte willQosLevel,
            bool willFlag,
            string willTopic,
            string willMessage,
            bool cleanSession,
            ushort keepAlivePeriod)
        {
            // create CONNECT message
            MqttMsgConnect connect = new MqttMsgConnect(clientId,
                username,
                password,
                willRetain,
                willQosLevel,
                willFlag,
                willTopic,
                willMessage,
                cleanSession,
                keepAlivePeriod);

            try
            {
                // create network channel and connect to broker
                this.channel = new MqttNetworkChannel(this.brokerHostName, this.brokerIpAddress, this.brokerPort, this.secure, this.caCert);
                this.channel.Connect();                
            }
            catch (Exception ex)
            {
                throw new MqttConnectionException("Exception connecting to the broker", ex);
            }

            this.lastSend = 0;
            this.isRunning = true;
            // start thread for receiving messages from broker
            this.receiveThread = new Thread(this.ReceiveThread);
            this.receiveThread.Start();
            
            MqttMsgConnack connack = (MqttMsgConnack)this.SendReceive(connect.GetBytes());
            // if connection accepted, start keep alive timer
            if (connack.ReturnCode == MqttMsgConnack.CONN_ACCEPTED)
            {
                this.keepAlivePeriod = keepAlivePeriod * 1000; // convert in ms
                
                // start thread for sending keep alive message to the broker
                this.keepAliveThread = new Thread(this.KeepAliveThread);
                this.keepAliveThread.Start();

                this.IsConnected = true;
            }
            return connack.ReturnCode;
        }

        /// <summary>
        /// Disconnect from broker
        /// </summary>
        public void Disconnect()
        {
            MqttMsgDisconnect disconnect = new MqttMsgDisconnect();
            this.Send(disconnect.GetBytes());

            // close client
            this.Close();
        }

        /// <summary>
        /// Close client
        /// </summary>
        private void Close()
        {
            // stop receiving thread and keep alive thread
            this.isRunning = false;

            // wait end receive thread
            if (this.receiveThread != null)
                this.receiveThread.Join();

            // avoid dedalock if keep alive timeout expired
            if (!this.isKeepAliveTimeout)
            {
                // unlock keep alive thread and wait
                this.keepAliveEvent.Set();

                if (this.keepAliveThread != null)
                    this.keepAliveThread.Join();
            }

            // close network channel
            this.channel.Close();

            // keep alive thread will set it gracefully
            if (!this.isKeepAliveTimeout)
                this.IsConnected = false;
        }

        /// <summary>
        /// Execute ping to broker for keep alive
        /// </summary>
        /// <returns>PINGRESP message from broker</returns>
        private MqttMsgPingResp Ping()
        {
            MqttMsgPingReq pingreq = new MqttMsgPingReq();
            try
            {
                // broker must send PINGRESP within timeout equal to keep alive period
                return (MqttMsgPingResp)this.SendReceive(pingreq.GetBytes(), this.keepAlivePeriod);
            }
            catch (Exception)
            {
                this.isKeepAliveTimeout = true;
                // client must close connection
                this.Close();
                return null;
            }
        }

        /// <summary>
        /// Subscribe for message topics
        /// </summary>
        /// <param name="topics">List of topics to subscribe</param>
        /// <param name="qosLevels">QOS levels related to topics</param>
        /// <returns>Granted QoS Levels in SUBACK message from broker</returns>
        public byte[] Subscribe(string[] topics, byte[] qosLevels)
        {
            int attempts = 0;
            bool acknowledged = false;

            MqttMsgSubscribe subscribe =
                new MqttMsgSubscribe(topics, qosLevels);
            subscribe.MessageId = this.GetMessageId();

            MqttMsgSuback suback = null;
            do
            {
                try
                {
                    // try subscribe
                    suback = (MqttMsgSuback)this.SendReceive(subscribe.GetBytes());
                    acknowledged = true;
                }
                catch (MqttTimeoutException)
                {
                    // no SUBACK message received in time, retry with duplicate flag
                    attempts++;
                    subscribe.DupFlag = true;
                    // delay before retry
                    if (attempts < MQTT_ATTEMPTS_RETRY)
                        Thread.Sleep(MQTT_DELAY_RETRY);
                }
            } while ((attempts < MQTT_ATTEMPTS_RETRY) && !acknowledged);

            // return granted QoS Levels or null
            return acknowledged ? suback.GrantedQoSLevels : null;
        }

        /// <summary>
        /// Unsubscribe for message topics
        /// </summary>
        /// <param name="topics">List of topics to unsubscribe</param>
        /// <returns>Message Id in UNSUBACK message from broker</returns>
        public ushort Unsubscribe(string[] topics)
        {
            int attempts = 0;
            bool acknowledged = false;

            MqttMsgUnsubscribe unsubscribe =
                new MqttMsgUnsubscribe(topics);
            unsubscribe.MessageId = this.GetMessageId();

            MqttMsgUnsuback unsuback = null;
            do
            {
                try
                {
                    // try unsubscribe
                    unsuback = (MqttMsgUnsuback)this.SendReceive(unsubscribe.GetBytes());
                    acknowledged = true;
                }
                catch (MqttTimeoutException)
                {
                    // no UNSUBACK message received in time, retry with duplicate flag
                    attempts++;
                    unsubscribe.DupFlag = true;

                    // delay before retry
                    if (attempts < MQTT_ATTEMPTS_RETRY)
                        Thread.Sleep(MQTT_DELAY_RETRY);
                }

            } while ((attempts < MQTT_ATTEMPTS_RETRY) && !acknowledged);

            // return message id from SUBACK or zero (no message id)
            return acknowledged ? unsuback.MessageId : (ushort)0;
        }

        /// <summary>
        /// Publish a message to the broker
        /// </summary>
        /// <param name="topic">Message topic</param>
        /// <param name="message">Message data (payload)</param>
        /// <returns>Message Id related to PUBLISH message</returns>
        public ushort Publish(string topic, byte[] message)
        {
            return this.Publish(topic, message, MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE, false);
        }

        /// <summary>
        /// Publish a message to the broker
        /// </summary>
        /// <param name="topic">Message topic</param>
        /// <param name="message">Message data (payload)</param>
        /// <param name="qosLevel">QoS Level</param>
        /// <param name="retain">Retain flag</param>
        /// <returns>Message Id related to PUBLISH message</returns>
        public ushort Publish(string topic, byte[] message, 
            byte qosLevel, 
            bool retain)
        {
            ushort messageId = 0;
            int attempts = 0;
            bool acknowledged = false;

            MqttMsgPublish publish = 
                new MqttMsgPublish(topic, message, false, qosLevel, retain);
            publish.MessageId = this.GetMessageId();

            // based on QoS level, the messages flow between client and broker changes
            switch (qosLevel)
            {
                // QoS Level 0, no answer from broker
                case MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE:
                                        
                    this.Send(publish.GetBytes());
                    break;

                // QoS Level 1, waiting for PUBACK message from broker
                case MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE:

                    attempts = 0;
                    acknowledged = false;
                    
                    do
                    {
                        MqttMsgPuback puback = null;
                        try
                        {
                            // try publish
                            puback = (MqttMsgPuback)this.SendReceive(publish.GetBytes());
                            acknowledged = true;
                        }
                        catch (MqttTimeoutException)
                        {
                            // no PUBACK message received in time, retry with duplicate flag
                            attempts++;
                            publish.DupFlag = true;

                            // delay before retry
                            if (attempts < MQTT_ATTEMPTS_RETRY)
                                Thread.Sleep(MQTT_DELAY_RETRY);
                        }
                    } while ((attempts < MQTT_ATTEMPTS_RETRY) && !acknowledged);

                    if (acknowledged)
                        messageId = publish.MessageId;

                    break;

                // QoS Level 2, waiting for PUBREC message from broker,
                // send PUBREL message and waiting for PUBCOMP message from broker
                case MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE:

                    attempts = 0;
                    acknowledged = false;

                    do
                    {
                        MqttMsgPubrec pubrec = null;
                        try
                        {
                            // try publish
                            pubrec = (MqttMsgPubrec)this.SendReceive(publish.GetBytes());
                            acknowledged = true;
                        }
                        catch (MqttTimeoutException)
                        {
                            // no PUBREC message received in time, retry with duplicate flag
                            attempts++;
                            publish.DupFlag = true;

                            // delay before retry
                            if (attempts < MQTT_ATTEMPTS_RETRY)
                                Thread.Sleep(MQTT_DELAY_RETRY);
                        }
                    } while ((attempts < MQTT_ATTEMPTS_RETRY) && !acknowledged);

                    // first phase ok
                    if (acknowledged)
                    {
                        attempts = 0;
                        acknowledged = false;

                        do
                        {
                            // set publish message identifier into PUBREL message
                            MqttMsgPubrel pubrel = new MqttMsgPubrel();
                            pubrel.MessageId = publish.MessageId;

                            MqttMsgPubcomp pubcomp = null;
                            try
                            {
                                // try send PUBREL message
                                pubcomp = (MqttMsgPubcomp)this.SendReceive(pubrel.GetBytes());
                                acknowledged = true;
                            }
                            catch (MqttTimeoutException)
                            {
                                // no PUBCOMP message received in time, retry with duplicate flag
                                attempts++;
                                pubrel.DupFlag = true;

                                // delay before retry
                                if (attempts < MQTT_ATTEMPTS_RETRY)
                                    Thread.Sleep(MQTT_DELAY_RETRY);
                            }
                        } while ((attempts < MQTT_ATTEMPTS_RETRY) && !acknowledged);

                        if (acknowledged)
                            messageId = publish.MessageId;
                    }
                    break;

                default:
                    throw new MqttClientException(MqttClientErrorCode.QosNotAllowed);
            }

            return messageId;
        }

        /// <summary>
        /// Wrapper method for raising PUBLISH message received event
        /// </summary>
        /// <param name="publish">PUBLISH message received</param>
        private void OnMqttMsgPublishReceived(MqttMsgPublish publish)
        {
            if (this.MqttMsgPublishReceived != null)
            {
                Thread threadEvent = 
                    new Thread(() => this.MqttMsgPublishReceived(this, 
                        new MqttMsgPublishEventArgs(publish.Topic, publish.Message, publish.QosLevel, publish.Retain)));

                threadEvent.Start();
            }
        }

        /// <summary>
        /// Wrapper method for raising published message event
        /// </summary>
        /// <param name="messageId">Message identifier for published message</param>
        private void OnMqttMsgPublished(ushort messageId)
        {
            if (this.MqttMsgPublished != null)
            {
                Thread threadEvent =  
                    new Thread(() => this.MqttMsgPublished(this, 
                        new MqttMsgPublishedEventArgs(messageId)));

                threadEvent.Start();
            }
        }

        /// <summary>
        /// Wrapper method for raising subscribed topic event (SUBACK message)
        /// </summary>
        /// <param name="suback">SUBACK message received</param>
        private void OnMqttMsgSubscribed(MqttMsgSuback suback)
        {
            if (this.MqttMsgSubscribed != null)
            {
                Thread threadEvent = 
                    new Thread(() => this.MqttMsgSubscribed(this, 
                        new MqttMsgSubscribedEventArgs(suback.MessageId, suback.GrantedQoSLevels)));
                
                threadEvent.Start();
            }
        }

        /// <summary>
        /// Wrapper method for raising unsubscribed topic event
        /// </summary>
        /// <param name="messageId">Message identifier for unsubscribed topic</param>
        private void OnMqttMsgUnsubscribed(ushort messageId)
        {
            if (this.MqttMsgUnsubscribed != null)
            {
                Thread threadEvent =
                    new Thread(() => this.MqttMsgUnsubscribed(this, 
                        new MqttMsgUnsubscribedEventArgs(messageId)));

                threadEvent.Start();
            }
        }
        
        /// <summary>
        /// Send a message to the broker
        /// </summary>
        /// <param name="msgBytes">Message bytes</param>
        private void Send(byte[] msgBytes)
        {
            try
            {
                // send message
                this.channel.Send(msgBytes);

                // update last message sent ticks
                this.lastSend = DateTime.Now.Ticks;
            }
            catch
            {
                throw new MqttCommunicationException();
            }
        }

        /// <summary>
        /// Send a message to the broker and wait answer
        /// </summary>
        /// <param name="msgBytes">Message bytes</param>
        /// <returns>MQTT message response</returns>
        private MqttMsgBase SendReceive(byte[] msgBytes)
        {
            return this.SendReceive(msgBytes, MQTT_DEFAULT_TIMEOUT);
        }

        /// <summary>
        /// Send a message to the broker and wait answer
        /// </summary>
        /// <param name="msgBytes">Message bytes</param>
        /// <param name="timeout">Timeout for receiving answer</param>
        /// <returns>MQTT message response</returns>
        private MqttMsgBase SendReceive(byte[] msgBytes, int timeout)
        {
            // reset handle before sending
            this.endReceiving.Reset();
            try
            {
                // send message
                this.channel.Send(msgBytes);

                // update last message sent ticks
                this.lastSend = DateTime.Now.Ticks;
            }
            catch (SocketException e)
            {
#if (!MF_FRAMEWORK_VERSION_V4_2 && !MF_FRAMEWORK_VERSION_V4_3 && !COMPACT_FRAMEWORK)
                // connection reset by broker
                if (e.SocketErrorCode == SocketError.ConnectionReset)
                    this.IsConnected = false;
#endif

                throw new MqttCommunicationException();
            }

#if (MF_FRAMEWORK_VERSION_V4_2 || MF_FRAMEWORK_VERSION_V4_3 || COMPACT_FRAMEWORK)
            // wait for answer from broker
            if (this.endReceiving.WaitOne(timeout, false))
#else
            // wait for answer from broker
            if (this.endReceiving.WaitOne(timeout))
#endif
            {
                // message received without exception
                if (this.exReceiving == null)
                    return this.msgReceived;
                // receiving thread catched exception
                else
                    throw this.exReceiving;
            }
            else
            {
                // throw timeout exception
                //throw new MqttTimeoutException();
                throw new MqttCommunicationException();
            }
        }

        /// <summary>
        /// Thread for receiving messages from broker
        /// </summary>
        private void ReceiveThread()
        {
            int readBytes = 0;
            byte[] fixedHeaderFirstByte = new byte[1];
            byte msgType;
            
            while (this.isRunning)
            {
                try
                {
                    if (this.channel.DataAvailable)
                        // read first byte (fixed header)
                        readBytes = this.channel.Receive(fixedHeaderFirstByte);
                    else
                    {
                        // no bytes available, sleep before retry
                        readBytes = 0;
                        Thread.Sleep(10);
                    }

                    if (readBytes > 0)
                    {
                        // extract message type from received byte
                        msgType = (byte)((fixedHeaderFirstByte[0] & MqttMsgBase.MSG_TYPE_MASK) >> MqttMsgBase.MSG_TYPE_OFFSET);

                        switch (msgType)
                        {
                            // impossible, broker can't send CONNECT message
                            case MqttMsgBase.MQTT_MSG_CONNECT_TYPE:

                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
                                
                            // CONNACK message received from broker
                            case MqttMsgBase.MQTT_MSG_CONNACK_TYPE:

                                this.msgReceived = MqttMsgConnack.Parse(fixedHeaderFirstByte[0], this.channel);
                                this.endReceiving.Set();
                                break;

                            // impossible, broker can't send PINGREQ message
                            case MqttMsgBase.MQTT_MSG_PINGREQ_TYPE:

                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);

                            // CONNACK message received from broker
                            case MqttMsgBase.MQTT_MSG_PINGRESP_TYPE:

                                this.msgReceived = MqttMsgPingResp.Parse(fixedHeaderFirstByte[0], this.channel);
                                this.endReceiving.Set();
                                break;

                            // impossible, broker can't send SUBSCRIBE message
                            case MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE:

                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);

                            // SUBACK message received from broker
                            case MqttMsgBase.MQTT_MSG_SUBACK_TYPE:

                                this.msgReceived = MqttMsgSuback.Parse(fixedHeaderFirstByte[0], this.channel);
                                this.endReceiving.Set();

                                // raise subscribed topic event (SUBACK message received)
                                this.OnMqttMsgSubscribed((MqttMsgSuback)this.msgReceived);

                                break;

                            // PUBLISH message received from broker
                            case MqttMsgBase.MQTT_MSG_PUBLISH_TYPE:

                                MqttMsgPublish msgReceived = MqttMsgPublish.Parse(fixedHeaderFirstByte[0], this.channel);
                                
                                // for QoS Level 1 and 2, client sends PUBACK message to broker
                                if ((msgReceived.QosLevel == MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE) ||
                                    (msgReceived.QosLevel == MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE))
                                {
                                    MqttMsgPuback puback = new MqttMsgPuback();
                                    puback.MessageId = msgReceived.MessageId;
                                    this.Send(puback.GetBytes());
                                }

                                // raise PUBLISH message received event 
                                this.OnMqttMsgPublishReceived(msgReceived);
                                                                
                                break;

                            // PUBACK message received from broker
                            case MqttMsgBase.MQTT_MSG_PUBACK_TYPE:

                                this.msgReceived = MqttMsgPuback.Parse(fixedHeaderFirstByte[0], this.channel);
                                this.endReceiving.Set();

                                // raise published message event
                                // (PUBACK received for QoS Level 1)
                                this.OnMqttMsgPublished(((MqttMsgPuback)this.msgReceived).MessageId);

                                break;

                            // PUBREC message received from broker
                            case MqttMsgBase.MQTT_MSG_PUBREC_TYPE:

                                this.msgReceived = MqttMsgPubrec.Parse(fixedHeaderFirstByte[0], this.channel);
                                this.endReceiving.Set();
                                break;

                            // impossible, broker can't send PUBREL message
                            case MqttMsgBase.MQTT_MSG_PUBREL_TYPE:

                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
                                
                            // PUBCOMP message received from broker
                            case MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE:

                                this.msgReceived = MqttMsgPubcomp.Parse(fixedHeaderFirstByte[0], this.channel);
                                this.endReceiving.Set();

                                // raise published message event
                                // (PUBCOMP received for QoS Level 2)
                                this.OnMqttMsgPublished(((MqttMsgPubcomp)this.msgReceived).MessageId);

                                break;

                            // impossible, broker can't send UNSUBSCRIBE message
                            case MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE:

                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);

                            // UNSUBACK message received from broker
                            case MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE:

                                this.msgReceived = MqttMsgUnsuback.Parse(fixedHeaderFirstByte[0], this.channel);
                                this.endReceiving.Set();

                                // raise unsubscribed topic event
                                this.OnMqttMsgUnsubscribed(((MqttMsgUnsuback)this.msgReceived).MessageId);

                                break;

                            default:

                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
                        }

                        this.exReceiving = null;
                    }

                }
                catch (Exception)
                {
                    this.exReceiving = new MqttCommunicationException();
                }
            }
        }

        /// <summary>
        /// Thread for sending keep alive message to broker
        /// </summary>
        private void KeepAliveThread()
        {
            long now = 0;
            int wait = this.keepAlivePeriod;
            this.isKeepAliveTimeout = false;

            while (this.isRunning)
            {
#if (MF_FRAMEWORK_VERSION_V4_2 || MF_FRAMEWORK_VERSION_V4_3 || COMPACT_FRAMEWORK)
                // waiting...
                this.keepAliveEvent.WaitOne(wait, false);
#else
                // waiting...
                this.keepAliveEvent.WaitOne(wait);
#endif

                if (this.isRunning)
                {
                    now = DateTime.Now.Ticks;
                    
                    // if timeout exceeded ... (keep alive period converted in ticks)
                    if ((now - this.lastSend) >= (this.keepAlivePeriod * TimeSpan.TicksPerMillisecond)) {
						// ... send keep alive
						this.Ping();
						wait = this.keepAlivePeriod;
					} else {
						// update waiting time (convert ticks in milliseconds)
						wait = (int)(this.keepAlivePeriod - (now - this.lastSend) / TimeSpan.TicksPerMillisecond);
					}
                }
            }

            if (this.isKeepAliveTimeout)
                this.IsConnected = false;
        }

        /// <summary>
        /// Generate the next message identifier
        /// </summary>
        /// <returns>Message identifier</returns>
        private ushort GetMessageId()
        {
            if (this.messageIdCounter == 0)
                this.messageIdCounter++;
            else
                this.messageIdCounter = ((this.messageIdCounter % UInt16.MaxValue) != 0) ? (ushort)(this.messageIdCounter + 1) : (ushort)0;
            return this.messageIdCounter;
        }
    }
}
