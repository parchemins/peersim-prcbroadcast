package descent.causalbroadcast.routingbispray;

import peersim.core.CommonState;
import peersim.core.Node;

/**
 * Route that disappears over time if unused;
 */
public class Route {

	public final Integer timestamp;

	public final Node mediator;
	public final Node dest;

	public Route(Node mediator, Node dest) {
		this.mediator = mediator;
		this.dest = dest;
		this.timestamp = CommonState.getIntTime();
	}

	public boolean isUsingMediator() {
		return this.mediator != null;
	}

}
