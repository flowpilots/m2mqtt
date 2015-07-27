using System;

namespace uPLibrary.Networking.M2Mqtt
{
    public interface IMqttPlugin
    {
        IMqttNetworkChannel Create(string remoteHostName, int remotePort, bool secure);
    }
}
