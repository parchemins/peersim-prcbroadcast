package descent.causalbroadcast.routingbispray;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import descent.causalbroadcast.PRCBcast;
import descent.causalbroadcast.WholePRCcast;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.transport.Transport;

/**
 * Data structure that register routes. It removes them over time if unused.
 */
public class Routes {

	public Node node;

	// worst-case scenario, we have to keep routes for 8 times latency.
	public Integer multiplicativeFactorForRetainingRoute = 8;

	public HashMap<Node, Route> routes;

	public Routes() {
		this.routes = new HashMap<Node, Route>();
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public void addRoute(Node from, Node mediator, Node to) {
		PRCBcast prcb = ((WholePRCcast) this.node.getProtocol(WholePRCcast.PID)).prcb;

		if (mediator == null && this.node == from) {
			assert (this.routes.containsKey(to) || prcb.isSafe(to));
			this.routes.put(to, new Route(null, to));

		} else if (mediator == null && this.node == to) {
			assert (this.routes.containsKey(from) || prcb.isSafe(from));
			this.routes.put(from, new Route(null, from));

		} else if (this.node == mediator) {
			assert (this.routes.containsKey(from) || prcb.isSafe(from));
			assert (this.routes.containsKey(to) || prcb.isSafe(to));
			this.routes.put(from, new Route(null, from));
			this.routes.put(to, new Route(null, to));

		} else if (this.node == from) {
			SprayWithRouting other = ((WholePRCcast) mediator.getProtocol(WholePRCcast.PID)).swr;

			assert (other.routes.routes.containsKey(this.node) || this.routes.containsKey(mediator)
					|| prcb.isSafe(mediator));
			this.routes.put(to, new Route(mediator, to));

		} else if (this.node == to) {
			SprayWithRouting other = ((WholePRCcast) mediator.getProtocol(WholePRCcast.PID)).swr;
			assert (other.routes.routes.containsKey(this.node) || this.routes.containsKey(mediator)
					|| prcb.isSafe(mediator));
			this.routes.put(from, new Route(mediator, from));
		}
		this.upKeep();
	}

	private void upKeep() {
		// Integer retainingTime = this.retainingTime;
		Integer retainingTime = ((int) (((Transport) this.node.getProtocol(FastConfig.getTransport(WholePRCcast.PID)))
				.getLatency(null, null)) * this.multiplicativeFactorForRetainingRoute);

		for (Node n : new HashSet<Node>(routes.keySet())) {
			Route r = this.routes.get(n);
			if (r.timestamp < CommonState.getIntTime() - retainingTime) {
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
			if (this.routes.get(n).isUsingMediator()) {
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
