package descent.causalbroadcast.routingbispray;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.collections4.bag.HashBag;

import descent.causalbroadcast.PreventiveReliableCausalBroadcast;
import descent.causalbroadcast.WholePRCcast;
import descent.causalbroadcast.messages.MAlpha;
import descent.causalbroadcast.messages.MBeta;
import descent.causalbroadcast.messages.MBuffer;
import descent.causalbroadcast.messages.MPi;
import descent.causalbroadcast.messages.MReliableBroadcast;
import descent.causalbroadcast.messages.MRho;
import descent.rps.APeerSampling;
import descent.rps.IMessage;
import descent.rps.IPeerSampling;
import descent.spray.SprayPartialView;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.transport.Transport;

/**
 * Spray with a light form of routing capabilities : the mediator knows both
 * peers and routes control messages back and forth until new links become safe.
 */
public class SprayWithRouting extends APeerSampling implements IRoutingService {

	public PreventiveReliableCausalBroadcast prcb;

	public Routes routes;

	public SprayPartialView outview; // In charge of this range of links
	public HashSet<Node> inview; // Other used links but cannot modify

	public HashBag<Node> inUse; // outview -> # control messages before removal

	////////////////////////////////////////////////////////////////////////////

	public SprayWithRouting(PreventiveReliableCausalBroadcast prcb) {
		this.prcb = prcb;

		this.routes = new Routes();

		this.outview = new SprayPartialView();
		this.inview = new HashSet<Node>();

		this.inUse = new HashBag<Node>();
	}

	// PEER-SAMPLING:

	public void periodicCall() {
		// #1 select a neighbor to exchange with
		Node q = this._getOldest();
		if (q != null) {
			// #2 prepare a sample
			HashBag<Node> sample = this._getSample(q);
			if (!sample.isEmpty()) {
				this.outview.removeNeighbor(q);
				this.inview.add(q);
				this.sendMConnectTo(q, this.node, new MConnectTo(q, this.node, null));
				// #A lock links for routing purpose
				for (Node neighbor : sample) {
					if (neighbor != this.node) {
						this.inUse.add(neighbor);
						this.inUse.add(q);
						this.outview.removeNeighbor(neighbor);
						this.sendMConnectTo(q, neighbor, new MConnectTo(q, neighbor, this.node));
					} else {
						this.outview.removeNeighbor(q);
						this.inview.add(q);
						this.sendMConnectTo(q, this.node, new MConnectTo(q, this.node, null));
					}
				}

			}
		}
	}

	public Node _getOldest() {
		Integer age = 0;
		ArrayList<Node> possibleOldest = new ArrayList<Node>();
		for (Node neighbor : this.outview.getPeers()) {
			if (!this.inUse.contains(neighbor) && this.isSafe(neighbor)) {
				if (age < this.outview.ages.get(neighbor)) {
					age = this.outview.ages.get(neighbor);
					possibleOldest = new ArrayList<Node>();
				}
				if (age == this.outview.ages.get(neighbor)) {
					possibleOldest.add(neighbor);
				}
			}
		}
		if (possibleOldest.size() > 0) {
			return possibleOldest.get(CommonState.r.nextInt(possibleOldest.size()));
		}
		return null;
	}

	public HashBag<Node> _getSample(Node q) {
		// #1 filter
		HashBag<Node> clone = new HashBag<Node>(this.outview.partialView);
		if (q != null) {
			clone.remove(q, 1);
		}
		for (Node neighbor : this.outview.partialView.uniqueSet()) {
			if (this.inUse.contains(neighbor) || this.isSafe(neighbor)) {
				clone.remove(neighbor);
			}
		}
		// #2 random and possibly replace
		Integer sampleSize = (int) Math.ceil(clone.size());
		HashBag<Node> sample = new HashBag<Node>();
		while (sample.size() < sampleSize && clone.size() > 0) {
			ArrayList<Node> ugly = new ArrayList<Node>(clone.uniqueSet());
			Node neighbor = ugly.get(CommonState.r.nextInt(ugly.size()));
			if (neighbor == q) {
				sample.add(this.node);
			} else {
				sample.add(q);
			}
			clone.remove(neighbor, 1);
		}
		return sample;
	}

	public IMessage onPeriodicCall(Node origin, IMessage message) {
		// TODO Auto-generated method stub
		return null;
	}

	public void join(Node joiner, Node contact) {
		this._clear();
		this._setNode(joiner);
		if (contact != null) {
			SprayWithRouting swr = ((WholePRCcast) contact.getProtocol(WholePRCcast.PID)).swr;
			// #1 the very first connection is safe
			this.outview.addNeighbor(contact);
			swr.inview.add(this.node);
			// #2 subsequent ones might not be
			swr.onSubscription(joiner);
		}
		this.isUp = true;
	}

	public void onSubscription(Node origin) {
		HashBag<Node> safeNeighbors = new HashBag<Node>();
		for (Node neighbor : this.outview.getPeers()) {
			if (this.isSafe(neighbor)) {
				safeNeighbors.add(neighbor);
			}
		}
		if (safeNeighbors.isEmpty()) {
			// #1 keep the subscription for ourself
			SprayWithRouting swr = ((WholePRCcast) origin.getProtocol(WholePRCcast.PID)).swr;
			this.outview.addNeighbor(origin);
			swr.inview.add(this.node);
		} else {
			// #2 share the subscription to neighbors
			for (Node neighbor : safeNeighbors) {
				this.sendMConnectTo(neighbor, origin, new MConnectTo(neighbor, origin, this.node));
			}
		}
	}

	public void leave() {
		// (TODO) (XXX)
		// #0 Goes down.
		// this.isUp = false;
		// #1 Immediately remove in the in-views.
		/*
		 * for (Node neighbor : this.outview.getPeers()) { SprayWithRouting swr
		 * = ((WholePRCcast) this.node.getProtocol(WholePRCcast.PID)).swr;
		 * swr._closeI(this.node);
		 * 
		 * }
		 */
	}

	/**
	 * A neighbor in our in-view just left. Remove the occurrences in our local
	 * structure.
	 * 
	 * @param leaver
	 *            The leaver identity.
	 */
	private void _closeI(Node leaver) {
		// (TODO) (XXX)
		this.inview.remove(leaver);
		this.prcb.closeI(leaver);
		this._removeAllRoutes(leaver); // (TODO)
	}

	@Override
	public boolean addNeighbor(Node peer) {
		// TODO Auto-generated method stub
		return false;
	}

	// ROUTING:

	/**
	 * Remove all routes coming from a node
	 * 
	 * @param toRemove
	 *            The node to remove from routing tables.
	 */
	private void _removeAllRoutes(Node toRemove) {
		// (TODO)
	}

	public void addRoute(Node mediator, Node to) {
		this.routes.addRoute(to, mediator);
	}

	public void removeRouteAsMediator(Node from, Node to) {
		assert(this.inUse.remove(from, 1));
		assert(this.inUse.remove(to, 1));
	}

	public void removeRouteAsEndProcess(Node mediator, Node to) {
		this.routes.removeRoute(to, mediator);
	}

	// CONTROL MESSAGES:

	public void sendMConnectTo(Node from, Node to, MConnectTo m) {
		// #1 mark nodes as currently used
		// if (to != this.node) {
		this.inUse.add(from);
		this.inUse.add(to);
		// }
		// #2 send the message
		this._sendControlMessage(from, new MConnectTo(from, to, this.node), "connect to");
	}

	// from: process A; to: process B; A -> alpha -> B
	public void sendAlpha(Node from, Node to) {
		Node mediator = null;
		if (this.node != from && this.node != to) {
			mediator = this.node; // ugly
		}
		this._sendControlMessage(to, new MAlpha(from, to, mediator), "alpha");
	}

	// from: process A; to: process B; B -> beta -> A
	public void sendBeta(Node from, Node to) {
		this._sendControlMessage(from, new MBeta(from, to), "beta");
	}

	// from: process A; to: process B; A -> pi -> B
	public void sendPi(Node from, Node to) {
		this._sendControlMessage(to, new MPi(from, to), "pi");
	}

	// from: process A; to: process B; B -> rho -> A
	public void sendRho(Node from, Node to) {
		this._sendControlMessage(from, new MRho(from, to), "rho");
	}

	private void _sendControlMessage(Node target, Object m, String info) {
		assert (this.routes.hasRoute(target)
				|| (this.inview.contains(target) || this.outview.contains(target) || this.inUse.contains(target))
						&& this.isSafe(target)); // no route nor forward

		if (this.routes.hasRoute(target)) {
			this._send(this.routes.getRoute(target), m); // route

		} else if ((this.inview.contains(target) || this.outview.contains(target) || this.inUse.contains(target))
				&& this.isSafe(target)) {
			this._send(target, m); // forward
			if (m instanceof MRho || m instanceof MPi) {
				this.inUse.remove(target, 1);
			}
		}
	}

	public void sendBuffer(Node dest, Node from, Node to, ArrayList<MReliableBroadcast> buffer) {
		// #0 bidirectional, Process "to" also sends a buffer
		Node receiver = this._getReceiver(from, to);
		Node sender = this._getSender(from, to);
		if (receiver != dest) { // ugly fix for bidirect
			Node temp = receiver;
			receiver = sender;
			sender = temp;
		}

		// #1 check if there is an issue with algo
		assert (!this.isSafe(receiver)); // was safe already

		// #2 send the buffer
		this._sendUnsafe(receiver, new MBuffer(from, to, sender, receiver, buffer));
	}

	/**
	 * Send a message using safe channels and processed routes.
	 * 
	 * @param to
	 *            The target ultimately.
	 */
	private void _send(Node to, Object m) {
		assert (((this.outview.contains(to) || this.inview.contains(to) || this.inUse.contains(to))
				&& this.isSafe(to)));

		((Transport) this.node.getProtocol(FastConfig.getTransport(WholePRCcast.PID))).send(this.node, to, m,
				WholePRCcast.PID);

	}

	/**
	 * Send a message using possibly unsafe channels and processed routes.
	 * 
	 * @param to
	 *            The target ultimately.
	 */
	private void _sendUnsafe(Node to, Object m) {
		assert (this.outview.contains(to) || this.inview.contains(to) || this.inUse.contains(to));

		((Transport) this.node.getProtocol(FastConfig.getTransport(WholePRCcast.PID))).send(this.node, to, m,
				WholePRCcast.PID);
	}

	public void sendToOutview(MReliableBroadcast m) {
		for (Node n : this.getOutview()) {
			this._send(n, m);
		}
	}

	public Node _getReceiver(Node from, Node to) {
		return this.node == to ? from : to;
	}

	public Node _getSender(Node from, Node to) {
		return this.node == to ? to : from;
	}

	public HashSet<Node> getOutview() {
		// since bidirectionnal, outview includes inview
		HashSet<Node> result = new HashSet<Node>();
		for (Node n : this.outview.getPeers()) {
			if (this.isSafe(n))
				result.add(n);
		}
		for (Node n : this.inview) {
			if (this.isSafe(n))
				result.add(n);
		}
		for (Node n : this.inUse) {
			assert (this.isSafe(n)); // inuse should not have unsafe links
			result.add(n);
		}
		return result;
	}

	// PEER-SAMPLING BASICS:

	public Iterable<Node> getPeers(int k) {
		// (TODO) inview ?
		return this.outview.getPeers(k);
	}

	public Iterable<Node> getPeers() {
		// (TODO) inview ?
		return this.outview.getPeers();
	}

	@Override
	public Iterable<Node> getAliveNeighbors() {
		HashSet<Node> result = new HashSet<Node>();
		for (Node n : this.outview.getPeers())
			if (n.isUp() && this.isSafe(n))
				result.add(n);

		for (Node n : this.inview)
			if (n.isUp())
				result.add(n);
		// System.out.println(this.outview.size() + " -- " +
		// this.inview.size());
		return result;
	}

	@Override
	public IPeerSampling clone() {
		return new SprayWithRouting(this.prcb);
	}

	@Override
	protected boolean pFail(List<Node> path) {
		return false;
	}

	public boolean isSafe(Node process) {
		return (!this.prcb.isUnsafe(process) && this.outview.contains(process)) || this.inview.contains(process);
	}

	private void _clear() {
		this.routes = new Routes();
		this.outview = new SprayPartialView();
		this.inview = new HashSet<Node>();
	}

	public void addToInView(Node neighbor) {
		assert (((WholePRCcast) neighbor.getProtocol(WholePRCcast.PID)).swr.outview.contains(this.node));
		this.inview.add(neighbor);
	}

	public boolean addToOutView(Node neighbor) {
		boolean notAlreadyExists = !this.outview.contains(neighbor);
		this.outview.addNeighbor(neighbor);
		return notAlreadyExists;

	}
}
