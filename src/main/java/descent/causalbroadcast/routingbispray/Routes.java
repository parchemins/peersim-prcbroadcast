package descent.causalbroadcast.routingbispray;

import java.util.ArrayList;
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

	public HashMap<Node, ArrayList<Route>> routes;

	public Routes() {
		this.routes = new HashMap<Node, ArrayList<Route>>();
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public void addRoute(Node from, Node mediator, Node to) {
		PRCBcast prcb = ((WholePRCcast) this.node.getProtocol(WholePRCcast.PID)).prcb;

		if (mediator == null && this.node == from) {
			// #A this --> to
			assert (this.routes.containsKey(to) || prcb.isSafe(to));
			if (!this.routes.containsKey(to))
				this.routes.put(to, new ArrayList<Route>());

			this.routes.get(to).add(new Route(null, to));

		} else if (mediator == null && this.node == to) {
			// #B from --> this
			if (!(this.routes.containsKey(from) || prcb.isSafe(from)))
				System.out.println("@"+ this.node.getID() + ";;; from "+ from.getID());
			assert (this.routes.containsKey(from) || prcb.isSafe(from));
			if (!this.routes.containsKey(from))
				this.routes.put(from, new ArrayList<Route>());

			this.routes.get(from).add(new Route(null, from));

		} else if (this.node == mediator) {
			// #C from --> this --> to
			assert (this.routes.containsKey(from) || prcb.isSafe(from));
			assert (this.routes.containsKey(to) || prcb.isSafe(to));
			if (!this.routes.containsKey(from))
				this.routes.put(from, new ArrayList<Route>());
			if (!this.routes.containsKey(to))
				this.routes.put(to, new ArrayList<Route>());

			this.routes.get(from).add(new Route(null, from));
			this.routes.get(to).add(new Route(null, to));

		} else if (this.node == from && mediator != null) {
			// #D this -> mediator -> to
			SprayWithRouting other = ((WholePRCcast) mediator.getProtocol(WholePRCcast.PID)).swr;
			assert (other.routes.routes.containsKey(this.node) || this.routes.containsKey(mediator)
					|| prcb.isSafe(mediator));
			if (!this.routes.containsKey(to))
				this.routes.put(to, new ArrayList<Route>());

			this.routes.get(to).add(new Route(mediator, to));

		} else if (this.node == to && mediator != null) {
			// #E from -> mediator -> this
			SprayWithRouting other = ((WholePRCcast) mediator.getProtocol(WholePRCcast.PID)).swr;
			assert (other.routes.routes.containsKey(this.node) || this.routes.containsKey(mediator)
					|| prcb.isSafe(mediator));
			if (!this.routes.containsKey(from))
				this.routes.put(from, new ArrayList<Route>());

			this.routes.get(from).add(new Route(mediator, from));
		} else {
			assert (false); // ugly ! !! !! :3
		}
		this.upKeep();
	}

	private void upKeep() {
		// Integer retainingTime = this.retainingTime;
		Integer retainingTime = 
				((int) (((Transport) this.node.getProtocol(FastConfig.getTransport(WholePRCcast.PID)))
						.getLatency(null, null)) * this.multiplicativeFactorForRetainingRoute);

		for (Node n : new HashSet<Node>(this.routes.keySet())) {
			for (Route r : new ArrayList<Route>(this.routes.get(n))) {
				if (r.timestamp < CommonState.getIntTime() - retainingTime) {
					this.routes.get(n).remove(r);
				}
			}
			if (this.routes.get(n).isEmpty())
				this.routes.remove(n);
		}
	}

	public Node getRoute(Node to) {
		this.upKeep();

		assert (this.hasRoute(to));

		for (Route r : this.routes.get(to)) {
			if (!r.isUsingMediator()) {
				return to; // direct
			}
		}

		for (Route r : this.routes.get(to)) {
			if (r.isUsingMediator()) {
				return r.mediator; // forward
			}
		}

		return null;
	}

	public Set<Node> inUse() {
		this.upKeep();
		HashSet<Node> result = new HashSet<Node>();
		for (Node n : this.routes.keySet()) {
			for (Route r : this.routes.get(n)) {
				if (r.isUsingMediator()) {
					result.add(r.mediator);
				} else {
					result.add(n);
				}
			}
		}
		return result;
	}

	public boolean hasRoute(Node to) {
		this.upKeep();
		return this.routes.containsKey(to);
	}

}
