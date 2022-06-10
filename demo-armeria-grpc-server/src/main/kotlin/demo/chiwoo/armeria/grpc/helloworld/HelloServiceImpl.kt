package demo.chiwoo.armeria.grpc.helloworld

import com.linecorp.armeria.server.ServiceRequestContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import java.util.concurrent.TimeUnit

class HelloServiceImpl : HelloServiceGrpcKt.HelloServiceCoroutineImplBase() {

    /**
     * Sends a [HelloReply] immediately when receiving a request.
     */
    override suspend fun sayHello(request: HelloRequest): HelloReply {
        // Make sure that current thread is request context aware
        ServiceRequestContext.current()
        return buildReply(toMessage(request.name))
    }

    override suspend fun sayHelloLazy(request: HelloRequest): HelloReply {
        delay(3000L)
        ServiceRequestContext.current()
        return buildReply(toMessage(request.name))
    }

    /**
     * Sends a [HelloReply] using `blockingTaskExecutor`.
     * You can remove the [[withArmeriaBlockingContext]] block when your gRPC service is built with
     * [[com.linecorp.armeria.server.grpc.GrpcServiceBuilder.useBlockingTaskExecutor]].
     *
     * @see [Blocking service implementation](https://armeria.dev/docs/server-grpc#blocking-service-implementation)
     */
    override suspend fun sayHelloBlocking(request: HelloRequest): HelloReply = withArmeriaBlockingContext {
        try { // Simulate a blocking API call.
            TimeUnit.SECONDS.sleep(3)
        } catch (ignored: Exception) { // Do nothing.
        }
        // Make sure that current thread is request context aware
        ServiceRequestContext.current()
        buildReply(toMessage(request.name))
    }

    /**
     * Sends 5 [HelloReply] responses when receiving a request.
     *
     * @see sayHelloStreamReply(HelloRequest, StreamObserver)
     */
    override fun sayHelloStreamReply(request: HelloRequest): Flow<HelloReply> {
        return flow {
            for (i in 1..5) {
                // Check context between delay and emit
                ServiceRequestContext.current()
                delay(1000)
                ServiceRequestContext.current()
                emit(buildReply("Hello, ${request.name}! (sequence: $i)")) // emit next value
                ServiceRequestContext.current()
            }
        }
    }

    /**
     * Sends a [HelloReply] when a request has been completed with multiple [HelloRequest]s.
     */
    override suspend fun sayHelloStreamRequest(requests: Flow<HelloRequest>): HelloReply {
        val names = mutableListOf<String>()
        requests.map { it.name }.toList(names)
        // Make sure that current thread is request context aware
        ServiceRequestContext.current()
        return buildReply(toMessage(names.joinToString()))
    }

    /**
     * Sends a [HelloReply] when each [HelloRequest] is received. The response will be completed
     * when the request is completed.
     */
    override fun sayHelloStreamBid(requests: Flow<HelloRequest>): Flow<HelloReply> = flow {
        requests.collect { request ->
            ServiceRequestContext.current()
            emit(buildReply(toMessage(request.name)))
        }
    }

    companion object {

        suspend fun <T> withArmeriaBlockingContext(block: suspend CoroutineScope.() -> T): T =
            withContext(ServiceRequestContext.current().blockingTaskExecutor().asCoroutineDispatcher(), block)

        private fun buildReply(message: String): HelloReply = HelloReply.newBuilder().setMessage(message).build()

        private fun toMessage(message: String): String = "Hello, $message!"
    }
}
