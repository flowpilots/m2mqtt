using System;
using System.Net.Sockets;

namespace uPLibrary.Networking.M2Mqtt.Messages
{
    /// <summary>
    /// Class for CONNACK message from broker to client
    /// </summary>
    public class MqttMsgConnack : MqttMsgBase
    {
        #region Constants...

        // return codes for CONNACK message
        public const byte CONN_ACCEPTED = 0x00;
        public const byte CONN_REFUSED_PROT_VERS = 0x01;
        public const byte CONN_REFUSED_IDENT_REJECTED = 0x02;
        public const byte CONN_REFUSED_SERVER_UNAVAILABLE = 0x03;
        public const byte CONN_REFUSED_USERNAME_PASSWORD = 0x04;
        public const byte CONN_REFUSED_NOT_AUTHORIZED = 0x05;

        private const byte TOPIC_NAME_COMP_RESP_BYTE_OFFSET = 0;
        private const byte CONN_RETURN_CODE_BYTE_OFFSET = 1;

        #endregion

        #region Properties...

        /// <summary>
        /// Return Code
        /// </summary>
        public byte ReturnCode
        {
            get { return this.returnCode; }
            set { this.returnCode = value; }
        }

        #endregion

        // return code for CONNACK message
        private byte returnCode;

        /// <summary>
        /// Constructor
        /// </summary>
        public MqttMsgConnack()
        {
            this.type = MQTT_MSG_CONNACK_TYPE;
        }

        /// <summary>
        /// Parse bytes for a CONNACK message
        /// </summary>
        /// <param name="fixedHeaderFirstByte">First fixed header byte</param>
        /// <param name="socket">Socket connected to the broker</param>
        /// <returns>CONNACK message instance</returns>
        public static MqttMsgConnack Parse(byte fixedHeaderFirstByte, Socket socket)
        {
            byte[] buffer;
            MqttMsgConnack msg = new MqttMsgConnack();

            // get remaining length and allocate buffer
            int remainingLength = MqttMsgBase.decodeRemainingLength(socket);
            buffer = new byte[remainingLength];

            // read bytes from socket...
            socket.Receive(buffer);
            // ...and set return code from broker
            msg.returnCode = buffer[CONN_RETURN_CODE_BYTE_OFFSET];

            return msg;
        }

        public override byte[] GetBytes()
        {
            throw new NotImplementedException();
        }
    }
}
