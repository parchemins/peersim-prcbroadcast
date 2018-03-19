package descent.causalbroadcast.routingbispray;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import peersim.core.CommonState;
import peersim.core.Node;

/**
 * Data structure that register routes. It removes them over time if unused.
 */
public class Routes {

	public HashMap<Node, Route> routes;

	public Node node;

	public Integer retainingTime = 10000; // (TODO configurable)

	public Routes() {
		this.routes = new HashMap<Node, Route>();
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public void addRoute(Node from, Node mediator, Node to) {
		if (this.node == mediator) {
			this.routes.put(from, new Route(null, from));
			this.routes.put(to, new Route(null, to));
		} else if (this.node == from) {
			this.routes.put(mediator, new Route(mediator, to));
		} else if (this.node == to) {
			this.routes.put(mediator, new Route(mediator, from));
		}
		upKeep();
	}

	private void upKeep() {
		for (Node n : this.routes.keySet()) {
			Route r = this.routes.get(n);
			if (r.timestamp < CommonState.getIntTime() - this.retainingTime) {
				this.routes.remove(n);
			}
		}
	}

	public Node getRoute(Node to) {
		assert (this.hasRoute(to));
		Route r = this.routes.get(to);
		if (r.isUsingMediator()) {
			return r.mediator; // forward
		} else {
			return to; // direct
		}
	}

	public Set<Node> inUse() {
		return new HashSet<Node>(this.routes.keySet());
	}

	public boolean hasRoute(Node to) {
		this.upKeep();
		return this.routes.containsKey(to);
	}

}
