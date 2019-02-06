package org.sravan.rsocket.channel;

import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class PingPong {
	static String reply(String message) {
		if (message.equalsIgnoreCase("ping")) {
			return "pong";
		} else if (message.equalsIgnoreCase("pong")) {
			return "ping";
		} else {
			throw new IllegalArgumentException("input must either 'ping' or 'pong'");
		}
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(PingPong.class, args);
	}

}

@Component
class Pong implements Ordered, ApplicationListener<ApplicationReadyEvent> {

	private static final Logger log = LogManager.getLogger(Pong.class);

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {

		final SocketAcceptor socketAcceptor = (connectionSetupPayload, sendingSocket) -> {

			final AbstractRSocket abstractRSocket = new AbstractRSocket() {
				@Override
				public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
					return Flux.from(payloads).map(Payload::getDataUtf8)
							.doOnNext(str -> log.info("received " + str + " in " + this.getClass().getName()))
							.map(PingPong::reply).map(DefaultPayload::create);

				}
			};
			return Mono.just(abstractRSocket);
		};
		final TcpServerTransport transport = TcpServerTransport.create(7000);
		RSocketFactory.receive().acceptor(socketAcceptor).transport(transport).start().block();
	}
}

@Component
class Ping implements Ordered, ApplicationListener<ApplicationReadyEvent> {

	private static final Logger log = LogManager.getLogger(Ping.class);

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		log.info("started " + this.getClass().getName());
		final TcpClientTransport transport = TcpClientTransport.create(7000);
		RSocketFactory.connect().transport(transport).start()
				.flatMapMany(socket -> socket
						.requestChannel(Flux.interval(Duration.ofSeconds(2)).map(l -> DefaultPayload.create("ping")))
						.map(payload -> payload.getDataUtf8())
						.doOnNext(string -> log.info("Received " + string + " in class " + this.getClass().getName()))
						.take(10).doFinally(signal -> socket.dispose()))
				.then().subscribe(result -> log.info(" consumed new result " + result));
	}
}