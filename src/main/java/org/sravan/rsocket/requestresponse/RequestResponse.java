package org.sravan.rsocket.requestresponse;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
public class RequestResponse {
	public static void main(String[] args) throws Exception {
		SpringApplication.run(RequestResponse.class, args);
	}

}

@Component
class Producer implements Ordered, ApplicationListener<ApplicationReadyEvent> {

	private static final Logger log = LogManager.getLogger(Producer.class);

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}

	Flux<String> notifications(String name) {
		return Flux.fromStream(Stream.generate(() -> "Hello " + name + "@" + Instant.now().toString()))
				.delayElements(Duration.ofSeconds(2));
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		final SocketAcceptor socketAcceptor = (connectionSetupPayload, sendingSocket) -> {

			final AbstractRSocket abstractRSocket = new AbstractRSocket() {
				@Override
				public Flux<Payload> requestStream(Payload payload) {
					final String name = payload.getDataUtf8();
					log.info("got request from consumer with payload: " + name);
					return notifications(name).map(DefaultPayload::create);
				}
			};
			return Mono.just(abstractRSocket);
		};
		final TcpServerTransport transport = TcpServerTransport.create(7000);
		RSocketFactory.receive().acceptor(socketAcceptor).transport(transport).start().block();
	}
}

@Component
class Consumer implements Ordered, ApplicationListener<ApplicationReadyEvent> {

	private static final Logger log = LogManager.getLogger(Consumer.class);

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		final TcpClientTransport transport = TcpClientTransport.create(7000);
		RSocketFactory.connect().transport(transport).start()
				.flatMapMany(sender -> sender.requestStream(DefaultPayload.create("sravan"))).map(Payload::getDataUtf8)
				.subscribe(result -> log.info(" consumed new result " + result));
	}
}