using System;

namespace uPLibrary.Networking.M2Mqtt.Exceptions
{
    /// <summary>
    /// Exception due to error communication with broker on socket
    /// </summary>
    public class MqttCommunicationException : ApplicationException
    {
        /// <summary>
        /// Default constructor
        /// </summary>
        public MqttCommunicationException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="e">Inner Exception</param>
        public MqttCommunicationException(Exception e)
            : base(String.Empty, e)
        {
        }
    }
}
