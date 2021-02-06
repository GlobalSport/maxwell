package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.util.InterpolatedStringsHandler;
import io.nats.client.Connection;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NatsProducer extends AbstractProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(NatsProducer.class);
	private final Connection natsConnection;
	private final String natsSubjectTemplate;

	public NatsProducer(MaxwellContext context) {
		super(context);
		try {
			List<String> urls = Arrays.asList(context.getConfig().natsUrl.split("\\s*,\\s*"));
			Options.Builder optionBuilder = new Options.Builder();
			for (String url : urls) {
				optionBuilder = optionBuilder.server(url);
			}
			Options option= optionBuilder.build();
			//this.natsConnection = Nats.connect(option);
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
		String natsSubject = new InterpolatedStringsHandler(this.natsSubjectTemplate).generateFromRowMapAndTrimAllWhitesSpaces(r);

		long maxPayloadSize = natsConnection.getMaxPayload();
		byte[] messageBytes = value.getBytes(StandardCharsets.UTF_8);

		if (messageBytes.length > maxPayloadSize) {
			LOGGER.error("->  nats message size (" + messageBytes.length + ") > max payload size (" + maxPayloadSize + ")");
			return;
		}

		natsConnection.publish(natsSubject, messageBytes);
		if ( r.isTXCommit() ) {
			context.setPosition(r.getNextPosition());
		}
		if ( LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  nats subject:" + natsSubject + ", message:" + value);
		}
	}
}
