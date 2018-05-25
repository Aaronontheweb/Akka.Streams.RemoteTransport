using System;
using System.Net;
using Akka.Actor;
using Akka.Configuration;
using Akka.IO;
using Akka.Util;

namespace Akka.Streams.RemoteTransport
{
    /// <summary>
    /// INTERNAL API.
    /// 
    /// Defines the settings for the <see cref="StreamsTransport"/>.
    /// </summary>
    public sealed class StreamsTransportSettings
    {
        public static StreamsTransportSettings Create(ActorSystem system)
        {
            return Create(system.Settings.Config.GetConfig("akka.remote.dot-netty.tcp"));
        }

        public static StreamsTransportSettings Create(Config config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config), "DotNetty HOCON config was not found (default path: `akka.remote.dot-netty.diagnostic.tcp`)");

            var transportMode = config.GetString("transport-protocol", "tcp").ToLower();
            var host = config.GetString("hostname");
            if (string.IsNullOrEmpty(host)) host = IPAddress.Any.ToString();
            var publicHost = config.GetString("public-hostname", null);
            var publicPort = config.GetInt("public-port", 0);

            var order = ByteOrder.LittleEndian;
            var byteOrderString = config.GetString("byte-order", "little-endian").ToLowerInvariant();
            switch (byteOrderString)
            {
                case "little-endian": order = ByteOrder.LittleEndian; break;
                case "big-endian": order = ByteOrder.BigEndian; break;
                default: throw new ArgumentException($"Unknown byte-order option [{byteOrderString}]. Supported options are: big-endian, little-endian.");
            }

            return new StreamsTransportSettings(
                transportMode: transportMode == "tcp" ? TransportMode.Tcp : TransportMode.Udp,
                enableSsl: config.GetBoolean("enable-ssl", false),
                connectTimeout: config.GetTimeSpan("connection-timeout", TimeSpan.FromSeconds(15)),
                hostname: host,
                publicHostname: !string.IsNullOrEmpty(publicHost) ? publicHost : host,
                port: config.GetInt("port", 2552),
                publicPort: publicPort > 0 ? publicPort : (int?)null,
                maxFrameSize: ToNullableInt(config.GetByteSize("maximum-frame-size")) ?? 128000,
                ssl: config.HasPath("ssl") ? SslSettings.Create(config.GetConfig("ssl")) : SslSettings.Empty,
                dnsUseIpv6: config.GetBoolean("dns-use-ipv6", false),
                tcpReuseAddr: config.GetBoolean("tcp-reuse-addr", true),
                tcpKeepAlive: config.GetBoolean("tcp-keepalive", true),
                tcpNoDelay: config.GetBoolean("tcp-nodelay", true),
                backlog: config.GetInt("backlog", 4096),
                enforceIpFamily: RuntimeDetector.IsMono || config.GetBoolean("enforce-ip-family", false),
                receiveBufferSize: ToNullableInt(config.GetByteSize("receive-buffer-size") ?? 256000),
                sendBufferSize: ToNullableInt(config.GetByteSize("send-buffer-size") ?? 256000),
                writeBufferHighWaterMark: ToNullableInt(config.GetByteSize("write-buffer-high-water-mark")),
                writeBufferLowWaterMark: ToNullableInt(config.GetByteSize("write-buffer-low-water-mark")),
                logTransport: config.HasPath("log-transport") && config.GetBoolean("log-transport"),
                byteOrder: order, bufferedMessages: config.GetInt("msg-buffer-size", 25000));
        }

        private static int? ToNullableInt(long? value) => value.HasValue && value.Value > 0 ? (int?)value.Value : null;

        /// <summary>
        /// Transport mode used by underlying socket channel. 
        /// Currently only TCP is supported.
        /// </summary>
        public readonly TransportMode TransportMode;

        /// <summary>
        /// If set to true, a Secure Socket Layer will be established
        /// between remote endpoints. They need to share a X509 certificate
        /// which path is specified in `akka.remote.dot-netty.tcp.ssl.certificate.path`
        /// </summary>
        public readonly bool EnableSsl;

        /// <summary>
        /// Sets a connection timeout for all outbound connections 
        /// i.e. how long a connect may take until it is timed out.
        /// </summary>
        public readonly TimeSpan ConnectTimeout;

        /// <summary>
        /// The hostname or IP to bind the remoting to.
        /// </summary>
        public readonly string Hostname;

        /// <summary>
        /// If this value is set, this becomes the public address for the actor system on this
        /// transport, which might be different than the physical ip address (hostname)
        /// this is designed to make it easy to support private / public addressing schemes
        /// </summary>
        public readonly string PublicHostname;

        /// <summary>
        /// The default remote server port clients should connect to.
        /// Default is 2552 (AKKA), use 0 if you want a random available port
        /// This port needs to be unique for each actor system on the same machine.
        /// </summary>
        public readonly int Port;

        /// <summary>
        /// If this value is set, this becomes the public port for the actor system on this
        /// transport, which might be different than the physical port
        /// this is designed to make it easy to support private / public addressing schemes
        /// </summary>
        public readonly int? PublicPort;

        public readonly int MaxFrameSize;
        public readonly SslSettings Ssl;

        /// <summary>
        /// If set to true, we will use IPv6 addresses upon DNS resolution for 
        /// host names. Otherwise IPv4 will be used.
        /// </summary>
        public readonly bool DnsUseIpv6;

        /// <summary>
        /// Enables SO_REUSEADDR, which determines when an ActorSystem can open
        /// the specified listen port (the meaning differs between *nix and Windows).
        /// </summary>
        public readonly bool TcpReuseAddr;

        /// <summary>
        /// Enables TCP Keepalive, subject to the O/S kernel's configuration.
        /// </summary>
        public readonly bool TcpKeepAlive;

        /// <summary>
        /// Enables the TCP_NODELAY flag, i.e. disables Nagle's algorithm
        /// </summary>
        public readonly bool TcpNoDelay;

        /// <summary>
        /// If set to true, we will enforce usage of IPv4 or IPv6 addresses upon DNS 
        /// resolution for host names. If true, we will use IPv6 enforcement. Otherwise, 
        /// we will use IPv4.
        /// </summary>
        public readonly bool EnforceIpFamily;

        /// <summary>
        /// Sets the size of the connection backlog.
        /// </summary>
        public readonly int Backlog;

        /// <summary>
        /// Sets the default receive buffer size of the Sockets.
        /// </summary>
        public readonly int? ReceiveBufferSize;

        /// <summary>
        /// Sets the default send buffer size of the Sockets.
        /// </summary>
        public readonly int? SendBufferSize;
        public readonly int? WriteBufferHighWaterMark;
        public readonly int? WriteBufferLowWaterMark;

        /// <summary>
        /// The number of messages that will be buffered by Akka.Streams
        /// prior to being written to the socket. Measured by number of messages,
        /// not their byte-size.
        /// </summary>
        /// <remarks>
        /// Defaults to 25,000 messages per connection if not set.
        /// </remarks>
        public readonly int BufferedMessages;

        /// <summary>
        /// When set to true, it will enable logging of DotNetty user events 
        /// and message frames.
        /// </summary>
        public readonly bool LogTransport;

        /// <summary>
        /// Byte order used by DotNetty, either big or little endian.
        /// By default a little endian is used to achieve compatibility with Helios.
        /// </summary>
        public readonly ByteOrder ByteOrder;

        public StreamsTransportSettings(TransportMode transportMode, bool enableSsl, TimeSpan connectTimeout, string hostname, string publicHostname,
            int port, int? publicPort, int maxFrameSize, SslSettings ssl,
            bool dnsUseIpv6, bool tcpReuseAddr, bool tcpKeepAlive, bool tcpNoDelay, int backlog, bool enforceIpFamily,
            int? receiveBufferSize, int? sendBufferSize, int? writeBufferHighWaterMark, int? writeBufferLowWaterMark, bool logTransport, ByteOrder byteOrder, int bufferedMessages)
        {
            if (maxFrameSize < 32000) throw new ArgumentException("maximum-frame-size must be at least 32000 bytes", nameof(maxFrameSize));

            TransportMode = transportMode;
            EnableSsl = enableSsl;
            ConnectTimeout = connectTimeout;
            Hostname = hostname;
            PublicHostname = publicHostname;
            Port = port;
            PublicPort = publicPort;
            MaxFrameSize = maxFrameSize;
            Ssl = ssl;
            DnsUseIpv6 = dnsUseIpv6;
            TcpReuseAddr = tcpReuseAddr;
            TcpKeepAlive = tcpKeepAlive;
            TcpNoDelay = tcpNoDelay;
            Backlog = backlog;
            EnforceIpFamily = enforceIpFamily;
            ReceiveBufferSize = receiveBufferSize;
            SendBufferSize = sendBufferSize;
            WriteBufferHighWaterMark = writeBufferHighWaterMark;
            WriteBufferLowWaterMark = writeBufferLowWaterMark;
            LogTransport = logTransport;
            ByteOrder = byteOrder;
            BufferedMessages = bufferedMessages;
        }
    }
}