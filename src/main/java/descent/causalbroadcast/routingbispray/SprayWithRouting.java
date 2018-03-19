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

	////////////////////////////////////////////////////////////////////////////

	public SprayWithRouting(PreventiveReliableCausalBroadcast prcb) {
		this.prcb = prcb;

		this.routes = new Routes();

		this.outview = new SprayPartialView();
		this.inview = new HashSet<Node>();
	}

	// PEER-SAMPLING:

	public void periodicCall() {
		// #1 select a neighbor to exchange with
		Node q = this._getOldest();
		if (q != null) {

			System.out.println("OLDEST " + q.getID());

			// #2 prepare a sample
			HashBag<Node> sample = this._getSample(q);

			assert (!sample.isEmpty()); // contains at least q

			// #3 lock links for routing purpose and #4 send connection messages
			for (Node neighbor : sample) {
				System.out.println("@" + this.node.getID() + " orders " + q.getID() + " -> " + neighbor.getID());

				SprayWithRouting other = ((WholePRCcast) q.getProtocol(WholePRCcast.PID)).swr;

				if (neighbor != this.node) {
					this.sendMConnectTo(q, neighbor, new MConnectTo(q, neighbor, this.node));
					this.outview.removeNeighbor(neighbor);
					if (!this.outview.contains(neighbor)) {
						other.inview.remove(this.node);
					}
				} else {
					this.outview.removeNeighbor(q);
					this.inview.add(q);
					other.outview.addNeighbor(this.node);
					if (!this.outview.contains(neighbor)) {
						other.inview.remove(this.node);
					}
					// this.sendMConnectTo(q, this.node, new MConnectTo(q,
					// this.node, null));
				}

			}
		}
	}

	public Node _getOldest() {
		Integer age = 0;
		ArrayList<Node> possibleOldest = new ArrayList<Node>();
		for (Node neighbor : this.outview.getPeers()) {
			if (!this.routes.inUse().contains(neighbor) && this.isSafe(neighbor)
					&& !this.prcb.buffersAlpha.containsKey(neighbor)) {
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
		HashBag<Node> sample = new HashBag<Node>();
		// #A add an occurrence of ourself to the sample
		if (q != null) {
			clone.remove(q, 1);
			sample.add(this.node);
		}
		// #B discard currently used links and unsafe links
		for (Node neighbor : this.outview.partialView.uniqueSet()) {
			if (this.routes.inUse().contains(neighbor) || !this.isSafe(neighbor)
					|| this.prcb.buffersAlpha.containsKey(neighbor)) {
				clone.remove(neighbor);
			}
		}

		// #2 random and possibly replace
		Integer sampleSize = (int) Math.ceil(clone.size() / 2.);
		while (sample.size() < sampleSize && clone.size() > 0) {
			ArrayList<Node> ugly = new ArrayList<Node>(clone.uniqueSet());
			Node neighbor = ugly.get(CommonState.r.nextInt(ugly.size()));
			if (neighbor == q) {
				sample.add(this.node);
			} else {
				sample.add(neighbor);
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
			System.out.println("JOIN @ " + joiner.getID() + " -> " + contact.getID());

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
				System.out.println("FWD @" + this.node.getID() + " -> " + neighbor.getID());
				this.sendMConnectTo(neighbor, origin, new MConnectTo(neighbor, origin, this.node));
			}
		}
	}

	public void leave() {
		// (TODO)
	}

	@Override
	public boolean addNeighbor(Node peer) {
		boolean alreadyContained = this.outview.contains(peer);
		SprayWithRouting other = ((WholePRCcast) peer.getProtocol(WholePRCcast.PID)).swr;

		// #1 quick consistency check
		assert ((alreadyContained && other.inview.contains(this.node))
				|| (!alreadyContained && !other.inview.contains(this.node)));

		// #2 add in both directions
		this.outview.addNeighbor(peer);
		other.inview.add(this.node);

		return alreadyContained;
	}

	public boolean removeNeighbor(Node peer) {
		boolean contained = this.outview.contains(peer);
		SprayWithRouting other = ((WholePRCcast) peer.getProtocol(WholePRCcast.PID)).swr;

		// #1 quick check
		assert (contained);
		assert ((contained && other.inview.contains(this.node)) || (!contained && !other.inview.contains(this.node)));
		// #2 remove from outview and corresponding inview if need be
		this.outview.removeNeighbor(peer);
		if (!this.outview.contains(peer)) {
			other.inview.remove(this.node);
		}
		return contained;
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

	public void addRoute(Node from, Node mediator, Node to) {
		this.routes.addRoute(from, mediator, to);
	}

	// CONTROL MESSAGES:

	public void sendMConnectTo(Node from, Node to, MConnectTo m) {
		/*
		 * System.out.println("===="); System.out.println("m.from " +
		 * from.getID() + "; "); if (m.mediator != null) { System.out.println(
		 * "m.media " + m.mediator.getID()); } System.out.println("m.to " +
		 * m.to.getID()); System.out.println("====");
		 */
		// #1 mark nodes as currently used
		this.addRoute(from, this.node, to);
		// #2 send the message
		this._sendControlMessage(from, m, "connect to");
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
		boolean route = this.routes.hasRoute(target) && this.isSafe(this.routes.getRoute(target));
		boolean direct = this.isSafe(target);

		assert (route || direct); // no route nor forward

		if (route) {
			this._send(this.routes.getRoute(target), m); // route
		} else if (direct) {
			this._send(target, m); // forward
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
		assert (this.isSafe(to));

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
		// ugly assert, still assert
		SprayWithRouting futureNeighbor = ((WholePRCcast) to.getProtocol(WholePRCcast.PID)).swr;
		assert (this.outview.contains(to) || this.inview.contains(to) || this.routes.hasRoute(to)
				|| futureNeighbor.outview.contains(this.node));

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
		for (Node n : this.routes.inUse()) {
			// inuse should not have unsafe links
			assert (!this.prcb.isUnsafe(n));
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
			if (n.isUp() && this.isSafe(n)) // (TODO) inUse included?
				result.add(n);

		for (Node n : this.inview)
			if (n.isUp())
				result.add(n);

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
		return (this.outview.contains(process) && !this.prcb.unsafe.contains(process)) || this.inview.contains(process)
				|| this.routes.inUse().contains(process);
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
