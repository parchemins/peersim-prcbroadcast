package descent.broadcast.causal.prcbroadcast.routing;

import peersim.core.Node;

/**
 * Entry of a map to get a mediator node.
 */
public class FromTo {

	public final Node from;
	public final Node to;

	public FromTo(Node from, Node to) {
		this.from = from;
		this.to = to;
	}
}
