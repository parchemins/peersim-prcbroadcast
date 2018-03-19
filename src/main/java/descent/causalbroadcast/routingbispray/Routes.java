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

	public Node node;

	public Integer retainingTime = 10000; // (TODO configurable)

	public HashMap<Node, Route> routes;

	public Routes() {
		this.routes = new HashMap<Node, Route>();
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public void addRoute(Node from, Node mediator, Node to) {
		this.upKeep();

		if (mediator == null && this.node == from) {
			this.routes.put(to, new Route(null, to));
		} else if (mediator == null && this.node == to) {
			this.routes.put(from, new Route(null, from));
		} else if (this.node == mediator) {
			this.routes.put(from, new Route(null, from));
			this.routes.put(to, new Route(null, to));
		} else if (this.node == from) {
			this.routes.put(to, new Route(mediator, to));
		} else if (this.node == to) {
			this.routes.put(from, new Route(mediator, from));
		}
	}

	private void upKeep() {
		for (Node n : new HashSet<Node>(routes.keySet())) {
			Route r = this.routes.get(n);
			if (r.timestamp < CommonState.getIntTime() - this.retainingTime) {
				this.routes.remove(n);
			}
		}
	}

	public Node getRoute(Node to) {
		this.upKeep();

		assert (this.hasRoute(to));

		Route r = this.routes.get(to);
		if (r.isUsingMediator()) {
			return r.mediator; // forward
		} else {
			return to; // direct
		}
	}

	public Set<Node> inUse() {
		this.upKeep();
		HashSet<Node> result = new HashSet<Node>();
		for (Node n : this.routes.keySet()) {
			if (this.routes.get(n).mediator != null) {
				result.add(this.routes.get(n).mediator);
			} else {
				result.add(n);
			}
		}
		return result;
	}

	public boolean hasRoute(Node to) {
		this.upKeep();
		return this.routes.containsKey(to);
	}

}
