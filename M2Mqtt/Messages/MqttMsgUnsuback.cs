using System;
using System.Net.Sockets;

namespace uPLibrary.Networking.M2Mqtt.Messages
{
    /// <summary>
    /// Class for UNSUBACK message from broker to client
    /// </summary>
    public class MqttMsgUnsuback : MqttMsgBase
    {
        #region Properties...

        /// <summary>
        /// Message identifier for the unsubscribe message
        /// that is acknowledged
        /// </summary>
        public ushort MessageId
        {
            get { return this.messageId; }
            set { this.messageId = value; }
        }

        #endregion

        // message identifier
        private ushort messageId;
        
        /// <summary>
        /// Constructor
        /// </summary>
        public MqttMsgUnsuback()
        {
            this.type = MQTT_MSG_UNSUBACK_TYPE;
        }

        /// <summary>
        /// Parse bytes for a UNSUBACK message
        /// </summary>
        /// <param name="fixedHeaderFirstByte">First fixed header byte</param>
        /// <param name="socket">Socket connected to the broker</param>
        /// <returns>UNSUBACK message instance</returns>
        public static MqttMsgUnsuback Parse(byte fixedHeaderFirstByte, Socket socket)
        {
            byte[] buffer;
            int index = 0;
            MqttMsgUnsuback msg = new MqttMsgUnsuback();

            // get remaining length and allocate buffer
            int remainingLength = MqttMsgBase.decodeRemainingLength(socket);
            buffer = new byte[remainingLength];

            // read bytes from socket...
            socket.Receive(buffer);

            // message id
            msg.messageId = (ushort)((buffer[index++] << 8) & 0xFF00);
            msg.messageId |= (buffer[index++]);

            return msg;
        }

        public override byte[] GetBytes()
        {
            throw new NotImplementedException();
        }
    }
}
