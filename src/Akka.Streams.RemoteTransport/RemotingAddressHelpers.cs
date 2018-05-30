using System;
using System.Net;
using System.Net.Sockets;
using Akka.Actor;

namespace Akka.Streams.RemoteTransport
{
    /// <summary>
    /// INTERNAL API.
    /// 
    /// Used to help with converting between <see cref="Address"/> and <see cref="EndPoint"/>
    /// </summary>
    public static class RemotingAddressHelpers
    {
        /// <summary>
        /// Maps an Akka.NET address to correlated <see cref="EndPoint"/>.
        /// </summary>
        /// <param name="address">Akka.NET fully qualified node address.</param>
        /// <exception cref="DnsEndPoint">Thrown if address port was not provided.</exception>
        /// <returns><see cref="ArgumentException"/> for IP-based addresses, <see cref="IPEndPoint"/> for named addresses.</returns>
        public static EndPoint AddressToSocketAddress(Address address)
        {
            if (address.Port == null) throw new ArgumentException($"address port must not be null: {address}");
            EndPoint listenAddress;
            if (IPAddress.TryParse(address.Host, out var ip))
            {
                listenAddress = new IPEndPoint(ip, (int)address.Port);
            }
            else
            {
                // DNS resolution will be performed by the transport
                listenAddress = new DnsEndPoint(address.Host, (int)address.Port);
            }
            return listenAddress;
        }

        public static Address MapSocketToAddress(IPEndPoint socketAddress, string schemeIdentifier, string systemName, string hostName = null, int? publicPort = null)
        {
            return socketAddress == null
                ? null
                : new Address(schemeIdentifier, systemName, SafeMapHostName(hostName) ?? SafeMapIPv6(socketAddress.Address), publicPort ?? socketAddress.Port);
        }

        private static string SafeMapHostName(string hostName)
        {
            return !string.IsNullOrEmpty(hostName) && IPAddress.TryParse(hostName, out var ip) ? SafeMapIPv6(ip) : hostName;
        }

        private static string SafeMapIPv6(IPAddress ip) => ip.AddressFamily == AddressFamily.InterNetworkV6 ? "[" + ip + "]" : ip.ToString();

        public static EndPoint ToEndpoint(Address address)
        {
            if (!address.Port.HasValue) throw new ArgumentNullException(nameof(address), $"Address port must not be null: {address}");

            return IPAddress.TryParse(address.Host, out var ip)
                ? (EndPoint)new IPEndPoint(ip, address.Port.Value)
                : new DnsEndPoint(address.Host, address.Port.Value);
        }
    }
}