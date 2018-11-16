using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Dispatch;
using Akka.Event;
using Akka.Remote.Transport;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Internal;
using DotNetty.Common;

namespace Akka.Streams.RemoteTransport
{
    public abstract class StreamsTransport : Transport
    {
        public StreamsTransportSettings Settings { get; }
        protected readonly ILoggingAdapter Log;
        internal readonly IMaterializer StreamMaterializer;
        internal readonly IActorRef StreamSupervisor;

        /// <summary>
        /// Handles to all of the current streams
        /// </summary>
        internal readonly ConcurrentSet<IActorRef> StreamRefs = new ConcurrentSet<IActorRef>();

        protected StreamsTransport(ActorSystem system, Config config)
        {
            System = system;
            Config = config;

            Settings = StreamsTransportSettings.Create(Config);
            Log = Logging.GetLogger(System, GetType());

            SchemeIdentifier = (Settings.EnableSsl ? "ssl." : string.Empty) + Settings.TransportMode.ToString().ToLowerInvariant();
            StreamSupervisor = system.AsInstanceOf<ExtendedActorSystem>().SystemActorOf(
                Props.Create(() => new StreamsTransportSupervisor()), Uri.EscapeUriString(SchemeIdentifier)+"-supervisor");

            // block here and get access to the materializer used for creating
            // future stream actors
            StreamMaterializer = StreamSupervisor
                .Ask<IMaterializer>(StreamsTransportSupervisor.GetMaterializer.Instance, Settings.ConnectTimeout)
                .Result;
        }

        public sealed override string SchemeIdentifier { get; protected set; }
        public override long MaximumPayloadBytes => Settings.MaxFrameSize;
        private TransportMode InternalTransport => Settings.TransportMode;

        public sealed override bool IsResponsibleFor(Address remote) => true;

        public override Task<Tuple<Address, TaskCompletionSource<IAssociationEventListener>>> Listen()
        {
            throw new NotImplementedException();
        }

        protected async Task<IPEndPoint> DnsToIpEndpoint(DnsEndPoint dns)
        {
            var addressFamily = Settings.DnsUseIpv6 ? AddressFamily.InterNetworkV6 : AddressFamily.InterNetwork;
            var endpoint = await ResolveNameAsync(dns, addressFamily).ConfigureAwait(false);
            return endpoint;
        }

        private async Task<IPEndPoint> ResolveNameAsync(DnsEndPoint address, AddressFamily addressFamily)
        {
            var resolved = await Dns.GetHostEntryAsync(address.Host).ConfigureAwait(false);
            var found = resolved.AddressList.LastOrDefault(a => a.AddressFamily == addressFamily);
            if (found == null)
            {
                throw new KeyNotFoundException($"Couldn't resolve IP endpoint from provided DNS name '{address}' with address family of '{addressFamily}'");
            }

            return new IPEndPoint(found, address.Port);
        }

        public override Task<AssociationHandle> Associate(Address remoteAddress)
        {
            throw new NotImplementedException();
        }

        public override Task<bool> Shutdown()
        {
            throw new NotImplementedException();
        }

        private async Task<AssociationHandle> StartClient(Address remoteAddress)
        {
            if (InternalTransport != TransportMode.Tcp)
                throw new NotSupportedException("Currently Akka.Streams server supports only TCP transport mode.");

            var addressFamily = Settings.DnsUseIpv6 ? AddressFamily.InterNetworkV6 : AddressFamily.InterNetwork;

           
            var socketAddress = RemotingAddressHelpers.AddressToSocketAddress(remoteAddress);
            socketAddress = await MapEndpointAsync(socketAddress).ConfigureAwait(false);

            // TODO: socket options
            var clientSource = System.TcpStream().OutgoingConnection(socketAddress, connectionTimeout:Settings.ConnectTimeout)
                .AddAttributes(ActorAttributes.CreateDispatcher(System.Settings.Config.GetString("akka.remote.use-dispatcher")));

            var joined = clientSource.JoinMaterialized(
                    StreamTransportFlows.OutboundConnectionHandler(this, remoteAddress, socketAddress), (connectTask, associate) => (connectTask, associate))
                .Join(Flow.Create<Google.Protobuf.ByteString>().Where(_ => true))
                .Run(StreamMaterializer);

            await joined.connectTask.ConfigureAwait(false);
            return await joined.associate.ConfigureAwait(false);
        }

        private void StartServer()
        {
            if (InternalTransport != TransportMode.Tcp)
                throw new NotSupportedException("Currently Akka.Streams server supports only TCP transport mode.");

            var addressFamily = Settings.DnsUseIpv6 ? AddressFamily.InterNetworkV6 : AddressFamily.InterNetwork;

            var serverSource = System.TcpStream().Bind(Settings.Hostname, Settings.Port, Settings.Backlog)
                .AddAttributes(ActorAttributes.CreateDispatcher(System.Settings.Config.GetString("akka.remote.use-dispatcher")));

            serverSource.RunForeach(connection =>
            {
                connection.Flow.Join(StreamTransportFlows.OutboundConnectionHandler(Settings, connection.));
            });
        }

        private async Task<IPEndPoint> MapEndpointAsync(EndPoint socketAddress)
        {
            IPEndPoint ipEndPoint;

            if (socketAddress is DnsEndPoint dns)
                ipEndPoint = await DnsToIpEndpoint(dns).ConfigureAwait(false);
            else
                ipEndPoint = (IPEndPoint)socketAddress;

            if (ipEndPoint.Address.Equals(IPAddress.Any) || ipEndPoint.Address.Equals(IPAddress.IPv6Any))
            {
                // client hack
                return ipEndPoint.AddressFamily == AddressFamily.InterNetworkV6
                    ? new IPEndPoint(IPAddress.IPv6Loopback, ipEndPoint.Port)
                    : new IPEndPoint(IPAddress.Loopback, ipEndPoint.Port);
            }
            return ipEndPoint;
        }
    }

    public enum TransportMode
    {
        Tcp,
        Udp
    }

    public sealed class SslSettings
    {
        public static readonly SslSettings Empty = new SslSettings();
        public static SslSettings Create(Config config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config), "DotNetty SSL HOCON config was not found (default path: `akka.remote.dot-netty.Ssl`)");

            var flagsRaw = config.GetStringList("certificate.flags");
            var flags = flagsRaw.Aggregate(X509KeyStorageFlags.DefaultKeySet, (flag, str) => flag | ParseKeyStorageFlag(str));

            return new SslSettings(
                certificatePath: config.GetString("certificate.path"),
                certificatePassword: config.GetString("certificate.password"),
                flags: flags,
                suppressValidation: config.GetBoolean("suppress-validation", false));
        }

        private static X509KeyStorageFlags ParseKeyStorageFlag(string str)
        {
            switch (str)
            {
                case "default-key-set": return X509KeyStorageFlags.DefaultKeySet;
                case "exportable": return X509KeyStorageFlags.Exportable;
                case "machine-key-set": return X509KeyStorageFlags.MachineKeySet;
                case "persist-key-set": return X509KeyStorageFlags.PersistKeySet;
                case "user-key-set": return X509KeyStorageFlags.UserKeySet;
                case "user-protected": return X509KeyStorageFlags.UserProtected;
                default: throw new ArgumentException($"Unrecognized flag in X509 certificate config [{str}]. Available flags: default-key-set | exportable | machine-key-set | persist-key-set | user-key-set | user-protected");
            }
        }

        /// <summary>
        /// X509 certificate used to establish Secure Socket Layer (SSL) between two remote endpoints.
        /// </summary>
        public readonly X509Certificate2 Certificate;

        /// <summary>
        /// Flag used to suppress certificate validation - use true only, when on dev machine or for testing.
        /// </summary>
        public readonly bool SuppressValidation;

        public SslSettings()
        {
            Certificate = null;
            SuppressValidation = false;
        }

        public SslSettings(string certificatePath, string certificatePassword, X509KeyStorageFlags flags, bool suppressValidation)
        {
            if (string.IsNullOrEmpty(certificatePath))
                throw new ArgumentNullException(nameof(certificatePath), "Path to SSL certificate was not found (by default it can be found under `akka.remote.dot-netty.tcp.ssl.certificate.path`)");

            Certificate = new X509Certificate2(certificatePath, certificatePassword, flags);
            SuppressValidation = suppressValidation;
        }
    }
}
