using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.IO;
using Akka.Remote.Transport;
using Akka.Streams.Stage;

namespace Akka.Streams.RemoteTransport
{
    /// <inheritdoc />
    /// <summary>
    /// Used to form outbound Akka.Remote associations
    /// </summary>
    public sealed class RemoteOutboundAssociationSink : GraphStageWithMaterializedValue<SinkShape<Google.Protobuf.ByteString>, Task<AssociationHandle>>
    {
        private readonly EndPoint _remoteSocketAddress;
        private readonly Address _remoteAddress;
        private readonly StreamsTransport _transport;

        public RemoteOutboundAssociationSink(StreamsTransport transport, Address remoteAddress, EndPoint remoteSocketAddress)
        {
            _transport = transport;
            _remoteAddress = remoteAddress;
            _remoteSocketAddress = remoteSocketAddress;
            Shape = new SinkShape<Google.Protobuf.ByteString>(In);
        }

        public override ILogicAndMaterializedValue<Task<AssociationHandle>> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var tcs = new TaskCompletionSource<AssociationHandle>();
            return new LogicAndMaterializedValue<Task<AssociationHandle>>(new RemoteOutboundAssociationLogic(this, tcs), tcs.Task);
        }

        public Inlet<Google.Protobuf.ByteString> In { get; } = new Inlet<Google.Protobuf.ByteString>("Akka.Remote.Outbound.In");

        public override SinkShape<Google.Protobuf.ByteString> Shape { get; }

        private class RemoteOutboundAssociationLogic : GraphStageLogic
        {
            private readonly TaskCompletionSource<AssociationHandle> _associationCompletion;
            private readonly RemoteOutboundAssociationSink _sink;
            private IHandleEventListener _listener;
            private Queue<Google.Protobuf.ByteString> _pendingMessages = new Queue<Google.Protobuf.ByteString>();
            private bool _cleanShutdown;

            public RemoteOutboundAssociationLogic(RemoteOutboundAssociationSink sink, TaskCompletionSource<AssociationHandle> associationCompletion)
                : base(sink.Shape)
            {
                _associationCompletion = associationCompletion;
                _sink = sink;

                // start off by buffering pending messages until the association process completes
                SetHandler(_sink.In, () => Buffer(Grab(_sink.In)), Finish, OnUpstreamFailure);
            }

            public override void PreStart()
            {
                // keep going even if the upstream is finished
                // so we can process the queued elements
                SetKeepGoing(true);

                // Request the first element
                Pull(_sink.In);

                var cb = GetAsyncCallback<IHandleEventListener>(SetListener);
                //todo: create handle, invoke callback
            }

            private void SetListener(IHandleEventListener handleEventListener)
            {
                _listener = handleEventListener;
                foreach (var msg in _pendingMessages)
                {
                    _listener.Notify(new InboundPayload(msg));
                }
                _pendingMessages.Clear();
                _pendingMessages = null; // GC the collection right away

                SetHandler(_sink.In, () => Recv(Grab(_sink.In)), Finish, OnUpstreamFailure);
            }

            private void OnUpstreamFailure(Exception exception)
            {
                _listener?.Notify(new UnderlyingTransportError(exception, "Failure in Akka.Streams processing pipeline."));
                FailStage(exception);
            }

            private void Recv(Google.Protobuf.ByteString msg)
            {
                _listener.Notify(new InboundPayload(msg));
                PullOrComplete();
            }

            private void Buffer(Google.Protobuf.ByteString msg)
            {
                _pendingMessages.Enqueue(msg);
                PullOrComplete();
            }

            private void PullOrComplete()
            {
                if (IsClosed(_sink.In))
                    Finish();
                else
                    Pull(_sink.In);
            }

            private void Finish()
            {
                _cleanShutdown = true;
                CompleteStage();
            }

            public override void PostStop()
            {
                _listener?.Notify(_cleanShutdown
                    ? new Disassociated(DisassociateInfo.Shutdown)
                    : new Disassociated(DisassociateInfo.Unknown));
            }
        }
    }
}