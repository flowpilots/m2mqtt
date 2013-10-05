using System;
using System.Net.Sockets;

namespace uPLibrary.Networking.M2Mqtt.Messages
{
    /// <summary>
    /// Class for SUBACK message from broker to client
    /// </summary>
    public class MqttMsgSuback : MqttMsgBase
    {
        #region Properties...

        /// <summary>
        /// Message identifier for the subscribe message
        /// that is acknowledged
        /// </summary>
        public ushort MessageId
        {
            get { return this.messageId; }
            set { this.messageId = value; }
        }

        /// <summary>
        /// List of granted QOS Levels
        /// </summary>
        public byte[] GrantedQoSLevels
        {
            get { return this.grantedQosLevels; }
            set { this.grantedQosLevels = value; }
        }

        #endregion

        // message identifier
        private ushort messageId;
        // granted QOS levels
        byte[] grantedQosLevels;

        /// <summary>
        /// Constructor
        /// </summary>
        public MqttMsgSuback()
        {
            this.type = MQTT_MSG_SUBACK_TYPE;
        }

        /// <summary>
        /// Parse bytes for a SUBACK message
        /// </summary>
        /// <param name="fixedHeaderFirstByte">First fixed header byte</param>
        /// <param name="channel">Channel connected to the broker</param>
        /// <returns>SUBACK message instance</returns>
        public static MqttMsgSuback Parse(byte fixedHeaderFirstByte, MqttNetworkChannel channel)
        {
            byte[] buffer;
            int index = 0;
            MqttMsgSuback msg = new MqttMsgSuback();

            // get remaining length and allocate buffer
            int remainingLength = MqttMsgBase.decodeRemainingLength(channel);
            buffer = new byte[remainingLength];

            // read bytes from socket...
            channel.Receive(buffer);

            // message id
            msg.messageId = (ushort)((buffer[index++] << 8) & 0xFF00);
            msg.messageId |= (buffer[index++]);

            // payload contains QoS levels granted
            msg.grantedQosLevels = new byte[remainingLength - MESSAGE_ID_SIZE];
            int qosIdx = 0;
            do
            {
                msg.grantedQosLevels[qosIdx++] = buffer[index++];
            } while (index < remainingLength);

            return msg;
        }

        public override byte[] GetBytes()
        {
            throw new NotImplementedException();
        }
    }
}
