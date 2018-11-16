using System;
using System.Collections.Generic;
using System.Text;
using Akka.Actor;

namespace Akka.Streams.RemoteTransport
{
    /// <summary>
    /// INTERNAL API.
    ///
    /// Actor responsible for creating and supervising the actors
    /// used for each individual Akka.Streams transport instance.
    /// </summary>
    internal sealed class StreamsTransportSupervisor : ReceiveActor
    {
        /// <summary>
        /// Command used to retrieve the <see cref="IMaterializer"/> that runs as part of this actor
        /// </summary>
        public sealed class GetMaterializer
        {
            public static readonly GetMaterializer Instance = new GetMaterializer();
            private GetMaterializer() { }
        }

        public StreamsTransportSupervisor()
        {
            Receive<GetMaterializer>(_ =>
            {
                Sender.Tell(Context.Materializer(namePrefix:"remotestreams"));
            });
        }
    }
}
