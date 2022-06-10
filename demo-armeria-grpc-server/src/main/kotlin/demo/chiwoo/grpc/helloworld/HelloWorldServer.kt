package demo.chiwoo.grpc.helloworld

import demo.chiwoo.armeria.grpc.helloworld.HelloReply
import demo.chiwoo.armeria.grpc.helloworld.HelloRequest
import demo.chiwoo.armeria.grpc.helloworld.HelloServiceGrpcKt
import io.grpc.Server
import io.grpc.ServerBuilder

class HelloWorldServer(private val port: Int) {
    val server: Server = ServerBuilder
        .forPort(port)
        .addService(HelloWorldService())
        .build()

    fun start() {
        server.start()
        println("Server started, listening on $port")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                println("*** shutting down gRPC server since JVM is shutting down")
                this.stop()
                // this@HelloWorldServer.stop()
                println("*** server shut down")
            }
        )
    }

    private fun stop() {
        server.shutdown()
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }

    internal class HelloWorldService : HelloServiceGrpcKt.HelloServiceCoroutineImplBase() {
        override suspend fun sayHello(request: HelloRequest): HelloReply =
            HelloReply.newBuilder().setMessage("Hello ${request.name}").build()
    }
}

fun main() {
    val port = System.getenv("PORT")?.toInt() ?: 26565
    val server = HelloWorldServer(port)
    server.start()
    server.blockUntilShutdown()
}
