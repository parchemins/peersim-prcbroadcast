package descent.broadcast.causal.prcbroadcast.routing;

import peersim.core.Node;

/**
 * Inform a process that another process will add it as neighbor.
 */
public class MConnectFrom {

	public final Node from;
	public final Node to;
	public final Node mediator;

	public MConnectFrom(Node from, Node to, Node mediator) {
		this.from = from;
		this.to = to;
		this.mediator = mediator;
	}
}
