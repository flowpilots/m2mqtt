using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace uPLibrary.Networking.M2Mqtt
{
    public class NetMqttPlugin : IMqttPlugin
    {
        public IMqttNetworkChannel Create(string remoteHostName, int remotePort, bool secure)
        {
            return new MqttNetworkChannel(remoteHostName, remotePort);
        }
    }
}
