using System;
using System.Threading.Tasks;
using Akka.IO;
using Akka.Remote.Transport;
using Akka.Streams.Stage;

namespace Akka.Streams.RemoteTransport
{
    /// <inheritdoc />
    /// <summary>
    /// Used to form outbound Akka.Remote associations
    /// </summary>
    public sealed class RemoteOutboundAssociationSink : GraphStageWithMaterializedValue<SinkShape<ByteString>, Task<AssociationHandle>>
    {
        public override ILogicAndMaterializedValue<Task<AssociationHandle>> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            throw new NotImplementedException();
        }

        public Inlet<ByteString> In { get; } = new Inlet<ByteString>("Akka.Remote.Outbound.In");

        public override SinkShape<ByteString> Shape { get; }
    }
}