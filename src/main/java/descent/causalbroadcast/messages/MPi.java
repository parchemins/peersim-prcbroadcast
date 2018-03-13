package descent.causalbroadcast.messages;

import peersim.core.Node;

/**
 * When Process A adds a link to Process B, this is the third control message
 * that must reach Process B. On receipt, Process B stops the first buffer and
 * starts another one.
 */
public class MPi implements IMControlMessage {

	public final Node from;
	public final Node to;

	public MPi(Node from, Node to) {
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
		return this.from;
	}

	public Node getReceiver() {
		return this.to;
	}
}
