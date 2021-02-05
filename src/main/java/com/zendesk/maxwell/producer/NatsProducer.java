package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.util.InterpolatedStringsHandler;
import io.nats.client.Connection;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import io.nats.client.Nats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class NatsProducer extends AbstractProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(NatsProducer.class);
	private final Connection natsConnection;
	private final String natsSubjectTemplate;

	public NatsProducer(MaxwellContext context) {
		super(context);
		try {
			this.natsConnection = Nats.connect(context.getConfig().natsUrl);
			this.natsSubjectTemplate = context.getConfig().natsSubject;

		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getNextPosition());

			return;
		}

		String value = r.toJSON(outputConfig);
		String natsSubject = new InterpolatedStringsHandler(this.natsSubjectTemplate).generateFromRowMap(r);

		natsConnection.publish(natsSubject, value.getBytes(StandardCharsets.UTF_8));
		if ( r.isTXCommit() ) {
			context.setPosition(r.getNextPosition());
		}
		if ( LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  nats subject:" + natsSubject + ", message:" + value);
		}
	}
}
