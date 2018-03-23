package descent.causalbroadcast.routingbispray;

import descent.causalbroadcast.messages.IMControlMessage;
import peersim.core.Node;

/**
 * Ask a process to connect to another process using the sender as mediator.
 */
public class MConnectTo implements IMControlMessage {

	public final Node from;
	public final Node to;
	public final Node mediator;

	public MConnectTo(Node from, Node mediator, Node to) {
		assert (from != to);
		assert (mediator != from);
		assert (mediator != to);

		this.from = from;
		this.to = to;
		this.mediator = mediator;
	}

	public Node getMediator() {
		return this.mediator;
	}
	
	public boolean isDirect() {
		return this.mediator == null;
	}

	public Node getFrom() {
		return this.from;
	}

	public Node getTo() {
		return this.to;
	}

	public Node getSender() {
		return this.mediator;
	}

	public Node getReceiver() {
		return this.from;
	}

}
