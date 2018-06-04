using System.Threading;
using Akka.Actor;
using Akka.Remote.Transport;
using Google.Protobuf;

namespace Akka.Streams.RemoteTransport
{
    /// <inheritdoc />
    /// <summary>
    /// INTERNAL API
    /// </summary>
    internal sealed class StreamsAssociationHandle : AssociationHandle
    {
        private readonly IActorRef _streamsRef;
        private readonly StreamsTransport _transport;

        public StreamsAssociationHandle(Address localAddress, Address remoteAddress, IActorRef streamsRef, StreamsTransport transport) : base(localAddress, remoteAddress)
        {
            _streamsRef = streamsRef;
            _transport = transport;
        }

        public override bool Write(ByteString payload)
        {
            // TODO: add mechanism for signaling backoff externally
            _streamsRef.Tell(payload);
            return true;
        }

        public override void Disassociate()
        {
            _transport.System.Stop(_streamsRef); // terminate the stream
            SpinWait.SpinUntil(() => _transport.StreamRefs.TryRemove(_streamsRef)); // remove from shutdown list
        }
    }
}