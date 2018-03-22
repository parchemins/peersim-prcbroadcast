package descent.causalbroadcast.routingbispray;

import peersim.core.Node;

/**
 * Ask a process to connect to another process using the sender as mediator.
 */
public class MConnectTo {

	public final Node from;
	public final Node to;
	public final Node mediator;

	public MConnectTo(Node from, Node to, Node mediator) {
		assert(from != to);
		this.from = from;
		this.to = to;
		this.mediator = mediator;
	}

	public boolean isDirect() {
		return this.mediator == null;
	}

}
