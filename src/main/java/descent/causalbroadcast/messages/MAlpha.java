package descent.causalbroadcast.messages;

import peersim.core.Node;

/**
 * When Process A adds a link to Process B, this is the first control message
 * that notifies Process B. On receipt, Process B starts buffering.
 */
public class MAlpha implements IMControlMessage {

	public final Node from;
	public final Node to;
	public final Node mediator;

	public MAlpha(Node from, Node mediator, Node to) {
		this.from = from;
		this.to = to;
		this.mediator = mediator;
	}

	public Node getFrom() {
		return this.from;
	}

	public Node getTo() {
		return this.to;
	}

	public Node getSender() {
		return this.from;
	}

	public Node getReceiver() {
		return this.to;
	}
}
