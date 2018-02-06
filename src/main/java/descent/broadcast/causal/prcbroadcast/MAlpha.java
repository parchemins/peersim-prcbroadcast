package descent.broadcast.causal.prcbroadcast;

import peersim.core.Node;

/**
 * When Process A adds a link to Process B, this is the first control message
 * that notifies Process B. On receipt, Process B starts buffering.
 */
public class MAlpha implements IMControlMessage {

	public final Node from;
	public final Node to;

	public MAlpha(Node from, Node to) {
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
