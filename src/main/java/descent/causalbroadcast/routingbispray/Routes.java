package descent.causalbroadcast.routingbispray;

import java.util.HashMap;

import peersim.core.Node;

/**
 * Data structure that register routes.
 */
public class Routes {

	public HashMap<Node, Node> toVia;

	public Routes() {
		this.toVia = new HashMap<Node, Node>();
	}

	public void addRoute(Node to, Node mediator) {
		this.toVia.put(to, mediator);
	}

	public void removeRoute(Node to) {
		this.toVia.remove(to);
	}

	public Node getRoute(Node to) {
		if (this.hasRoute(to)) {
			return this.toVia.get(to);
		} else {
			return null;
		}
	}

	public boolean hasRoute(Node to) {
		return this.toVia.containsKey(to);
	}

}
