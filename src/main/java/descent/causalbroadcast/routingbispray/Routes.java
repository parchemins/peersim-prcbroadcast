package descent.causalbroadcast.routingbispray;

import java.util.HashMap;

import org.apache.commons.collections4.bag.HashBag;

import peersim.core.Node;

/**
 * Data structure that register routes.
 */
public class Routes {

	public HashMap<Node, HashBag<Node>> toVia;

	public Routes() {
		this.toVia = new HashMap<Node, HashBag<Node>>();
	}

	public void addRoute(Node to, Node mediator) {
		if (!this.toVia.containsKey(to)) {
			this.toVia.put(to, new HashBag<Node>());
		}
		this.toVia.get(to).add(mediator);
	}

	public void removeRoute(Node to, Node via) {
		assert (this.toVia.containsKey(to));
		assert (this.toVia.get(to).contains(via));

		this.toVia.get(to).remove(via, 1);
		if (this.toVia.get(to).isEmpty()) {
			this.toVia.remove(to);
		}
	}

	public Node getRoute(Node to) {
		assert (this.toVia.containsKey(to));
		assert (!this.toVia.get(to).isEmpty());

		return this.toVia.get(to).iterator().next();
	}

	public boolean hasRoute(Node to) {
		return this.toVia.containsKey(to) && !this.toVia.get(to).isEmpty();
	}

}
