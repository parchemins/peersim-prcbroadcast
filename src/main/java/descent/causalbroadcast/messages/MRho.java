package descent.causalbroadcast.messages;

import peersim.core.Node;

/**
 * When Process A adds a link to Process B, this is the fourth control message.
 * On receipt, Process A can safely send its buffer to Process B and start using
 * the new link for causal broadcast.
 */
public class MRho implements IMControlMessage {

	public final Node from;
	public final Node to;

	public MRho(Node from, Node to) {
		this.from = from;
		this.to = to;
	}

	public Node getFrom() {
		return this.from;
	}

	public Node getTo() {
		return this.to;
	}

	public Node getSender() {
		return this.to;
	}

	public Node getReceiver() {
		return this.from;
	}
}
