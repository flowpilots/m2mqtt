using System;

namespace uPLibrary.Networking.M2Mqtt
{
    public class WindowsMqttPlugin : IMqttPlugin
    {
        public IMqttNetworkChannel Create(string remoteHostName, int remotePort, bool secure)
        {
            return new MqttNetworkChannel(remoteHostName, remotePort, secure);
        }
    }
}
