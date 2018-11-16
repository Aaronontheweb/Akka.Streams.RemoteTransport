using System;
using System.Net;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.IO;
using Akka.Remote.Transport;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Streams.RemoteTransport
{
    public static class StreamTransportFlows
    {
        public const int DefaultMaxFrameLength = 128000;

        /// <summary>
        ///     Toggles the endianness of the specified 64-bit long integer.
        /// </summary>
        public static long SwapLong(long value)
        {
            return ((SwapInt((int)value) & 0xFFFFFFFF) << 32)
                   | (SwapInt((int)(value >> 32)) & 0xFFFFFFFF);
        }

        /// <summary>
        ///     Toggles the endianness of the specified 32-bit integer.
        /// </summary>
        public static int SwapInt(int value)
        {
            return ((SwapShort((short)value) & 0xFFFF) << 16)
                   | (SwapShort((short)(value >> 16)) & 0xFFFF);
        }

        /// <summary>
        ///     Toggles the endianness of the specified 16-bit integer.
        /// </summary>
        public static short SwapShort(short value)
        {
            return (short)(((value & 0xFF) << 8) | (value >> 8) & 0xFF);
        }

        public static byte[] MyInt(this byte[] target, int x, int offset = 0, ByteOrder order = ByteOrder.BigEndian)
        {
            uint u = (uint)x;
            if (order == ByteOrder.BigEndian)
            {
                target[offset + 0] = (byte)(u >> 24);
                target[offset + 1] = (byte)(u >> 16);
                target[offset + 2] = (byte)(u >> 8);
                target[offset + 3] = (byte)(u >> 0);
            }
            else
            {
                target[offset + 0] = (byte)(u >> 0);
                target[offset + 1] = (byte)(u >> 8);
                target[offset + 2] = (byte)(u >> 16);
                target[offset + 3] = (byte)(u >> 24);
            }

            return target;
        }


        public static Flow<ByteString, ByteString, NotUsed> Decode(int frameLengthBytes = 4,
            int maxFrameLength = DefaultMaxFrameLength, ByteOrder byteOrder = ByteOrder.LittleEndian)
        {
            return Framing.LengthField(frameLengthBytes, maxFrameLength, 0, byteOrder)
                .Select(m => m.Slice(frameLengthBytes, m.Count - frameLengthBytes)); // strip the header
        }

        public static Flow<ByteString, ByteString, NotUsed> Encode(int frameLengthBytes = 4, int maxFrameLength = DefaultMaxFrameLength,
            ByteOrder byteOrder = ByteOrder.LittleEndian)
        {
            if (frameLengthBytes < 1 || frameLengthBytes > 4)
                throw new ArgumentException("Length field length must be 1,2,3 or 4", nameof(frameLengthBytes));

            return Flow.Create<ByteString>().Select(x =>
            {
                // todo: add validation that the message length can fit into frameLengthBytes
                // todo: buffer pooling (maybe)
                var header = new byte[frameLengthBytes].PutInt(x.Count, 0, order: byteOrder);
                return ByteString.FromBytes(header) + x;
            });
        }

        public static Flow<Google.Protobuf.ByteString, ByteString, NotUsed> FromProtobuf()
        {
            // TODO: avoid copying
            return Flow.Create<Google.Protobuf.ByteString>()
                .Select(x => ByteString.CopyFrom(x.ToByteArray()));
        }

        public static Flow<ByteString, Google.Protobuf.ByteString, NotUsed> ToProtobuf()
        {
            // TODO: avoid copying
            return Flow.Create<ByteString>()
                .Select(x => Google.Protobuf.ByteString.CopyFrom(x.ToArray()));
        }

        public static BidiFlow<ByteString, Google.Protobuf.ByteString, Google.Protobuf.ByteString, ByteString, (IActorRef sourceRef, Task<AssociationHandle> associateTask)>
            OutboundConnectionHandler(StreamsTransport transport, Address remoteAddress, EndPoint remoteSocketAddr)
        {
            var settings = transport.Settings;
            var transmissionHandle =
                Source.ActorRef<Google.Protobuf.ByteString>(settings.BufferedMessages, OverflowStrategy.Backpressure);
            var fromLocalActors = FromProtobuf().Via(Encode(4, settings.MaxFrameSize, settings.ByteOrder));


            var fromRemoteActors = Decode(4, settings.MaxFrameSize, settings.ByteOrder).Via(ToProtobuf());

            var transmissionRef = transmissionHandle.PreMaterialize(transport.StreamMaterializer);

            var finalOutput = new RemoteOutboundAssociationSink(transport, remoteAddress, remoteSocketAddr, transmissionRef.Item1);

            var dsl = GraphDsl.Create(transmissionRef.Item2, finalOutput, (@ref, task) => (sourceRef:@ref, associateTask:task),
                (builder, localInput, remoteOutput) =>
                {
                    // local stages
                    var compilerCeremony = builder.Add(Flow.Create<Google.Protobuf.ByteString>());
                    var local = builder.Add(fromLocalActors);
                    var merge = builder.Add(new Merge<Google.Protobuf.ByteString, Google.Protobuf.ByteString>(2, false));
                    builder.From(localInput.Outlet).To(merge.In(0));
                    builder.From(compilerCeremony.Outlet).To(merge.In(1));
                    builder.From(merge.Out).To(local.Inlet);

                    // remote stages
                    var remote = builder.Add(fromRemoteActors);
                    builder.From(remote.Outlet).To(remoteOutput.Inlet);

                    return new BidiShape<ByteString, Google.Protobuf.ByteString, Google.Protobuf.ByteString, ByteString>(remote.Inlet, remote.Outlet, compilerCeremony.Inlet, local.Outlet);
                });



            return BidiFlow.FromGraph(dsl);
        }
    }
}