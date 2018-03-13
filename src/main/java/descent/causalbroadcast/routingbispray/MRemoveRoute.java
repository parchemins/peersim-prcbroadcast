package descent.causalbroadcast.routingbispray;

import peersim.core.Node;

// Notify mediator to remove an occurrence of a route.
public class MRemoveRoute {

	public final Node from;
	public final Node mediator;
	public final Node to;

	public MRemoveRoute(Node from, Node mediator, Node to) {
		this.from = from;
		this.mediator = mediator;
		this.to = to;
	}

}
