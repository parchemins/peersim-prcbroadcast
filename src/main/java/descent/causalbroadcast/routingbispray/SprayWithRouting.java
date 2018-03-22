package descent.causalbroadcast.routingbispray;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.collections4.bag.HashBag;

import descent.causalbroadcast.IPRCB;
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

	public IPRCB prcb;

	public Routes routes;

	public SprayPartialView outview; // In charge of this range of links
	public HashSet<Node> inview; // Other used links but cannot modify

	////////////////////////////////////////////////////////////////////////////

	public SprayWithRouting(IPRCB prcb) {
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

			System.out.println("PERIODIC @" + this.node.getID() + "; " + q.getID());
			// #2 prepare a sample
			HashBag<Node> sample = this._getSample(q);

			assert (!sample.isEmpty()); // contains at least q

			// #3 lock links for routing purpose and #4 send connection messages
			Integer qCounter = 0;
			for (Node neighbor : sample) {
				if (neighbor != this.node) {
					this.sendMConnectTo(q, neighbor, new MConnectTo(q, neighbor, this.node));
					this.removeNeighbor(neighbor);
				} else {
					++qCounter;
				}
			}
			this.sendMExchangeWith(q, qCounter);
			// The other is in charge to remove the link if need be
			// for (int i = 0; i < qCounter; ++i) {
			// this.removeNeighbor(q);
			// }
		}
	}

	public Node _getOldest() {
		Integer age = 0;
		ArrayList<Node> possibleOldest = new ArrayList<Node>();
		for (Node neighbor : this.outview.getPeers()) {
			WholePRCcast other = (WholePRCcast) neighbor.getProtocol(WholePRCcast.PID);
			// no currently used as route && from <- safe -> to
			if (!this.routes.inUse().contains(neighbor) && !this.prcb.isStillChecking(neighbor)
					&& this.prcb.isSafe(neighbor)) {
				if (age < this.outview.ages.get(neighbor)) {
					age = this.outview.ages.get(neighbor);
					possibleOldest = new ArrayList<Node>();
				}
				if (age == this.outview.ages.get(neighbor)) {
					possibleOldest.add(neighbor);
				}
			}
		}
		if (possibleOldest.size() > 0)
			return possibleOldest.get(CommonState.r.nextInt(possibleOldest.size()));
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
			// same condition in getoldest, we filter candidates
			WholePRCcast other = (WholePRCcast) neighbor.getProtocol(WholePRCcast.PID);
			if (this.routes.inUse().contains(neighbor) || this.prcb.isStillChecking(neighbor)
					|| !this.prcb.isSafe(neighbor)) {
				clone.remove(neighbor);
			}
		}

		// #2 random and possibly replace
		Integer sampleSize = (int) Math.floor(clone.size() / 2.);
		if (q != null) {
			sampleSize = (int) Math.ceil(clone.size() / 2.);
		}
		while (sample.size() < sampleSize && clone.size() > 0) {
			ArrayList<Node> ugly = new ArrayList<Node>(clone.uniqueSet());
			Node neighbor = ugly.get(CommonState.r.nextInt(ugly.size()));
			if (neighbor == q) {
				sample.add(this.node);
			} else {
				sample.add(neighbor);
				WholePRCcast other = (WholePRCcast) neighbor.getProtocol(WholePRCcast.PID);
				// check
				assert (this.prcb.isSafe(neighbor));
				assert (other.prcb.isSafe(this.node));
			}
			clone.remove(neighbor, 1);
		}

		return sample;
	}

	public IMessage onPeriodicCall(Node origin, IMessage message) {
		Integer nbReferences = (Integer) message.getPayload();

		System.out.println("ON PERIODIC @" + this.node.getID() + ";; " + origin.getID() + " x" + nbReferences);
		SprayWithRouting other = ((WholePRCcast) origin.getProtocol(WholePRCcast.PID)).swr;

		for (int i = 0; i < nbReferences; ++i)
			other.removeNeighbor(this.node);
		for (int i = 0; i < nbReferences; ++i)
			this.addNeighborTrySafeButIfNotFallbackToUnsafe(origin);

		HashBag<Node> sample = this._getSample(null);

		for (Node neighbor : sample) {
			if (neighbor != origin) {
				this.sendMConnectTo(origin, neighbor, new MConnectTo(origin, neighbor, this.node));
				this.removeNeighbor(neighbor);
			}
		}

		return null;
	}

	public void join(Node joiner, Node contact) {
		this._setNode(joiner);
		this.routes.setNode(joiner);

		if (contact != null) {
			SprayWithRouting swr = ((WholePRCcast) contact.getProtocol(WholePRCcast.PID)).swr;
			// #1 the very first connection is safe
			this.addNeighborSafe(contact);
			// #2 subsequent ones might not be
			swr.onSubscription(joiner);
		}
		this.isUp = true;
	}

	public void onSubscription(Node origin) {
		HashBag<Node> safeNeighbors = new HashBag<Node>();
		for (Node neighbor : this.outview.getPeers()) {
			if (this.prcb.isSafe(neighbor)) {
				safeNeighbors.add(neighbor);
			}
		}
		if (safeNeighbors.isEmpty()) {
			// #1 keep the subscription for ourself
			this.addNeighbor(origin);
		} else {
			// #2 share the subscription to neighbors
			for (Node neighbor : safeNeighbors) {
				this.sendMConnectTo(neighbor, origin, new MConnectTo(neighbor, origin, this.node));
			}
		}
	}

	public void leave() {
		// (TODO)
	}

	@Override
	public boolean addNeighbor(Node peer) {
		assert (this.node != peer);
		boolean alreadyContained = this.outview.contains(peer);
		SprayWithRouting other = ((WholePRCcast) peer.getProtocol(WholePRCcast.PID)).swr;

		// #1 quick consistency check
		assert ((alreadyContained && other.inview.contains(this.node))
				|| (!alreadyContained && !other.inview.contains(this.node)));

		// #2 add in both directions
		this.outview.addNeighbor(peer);
		other.inview.add(this.node);

		return !alreadyContained;
	}

	public boolean addNeighborSafe(Node peer) {
		WholePRCcast other = (WholePRCcast) peer.getProtocol(WholePRCcast.PID);
		boolean isNew = this.addNeighbor(peer);
		assert (isNew);
		this.prcb.open(peer, true); // from -- safe -> to
		other.prcb.open(this.node, true); // to -- safe -> from
		return isNew; // (TODO) maybe more meaningful return value
	}

	public boolean addNeighborUnsafe(Node peer) {
		boolean isNew = this.addNeighbor(peer);
		// last part of condition is a cheat to ensure that only one
		// safety checking run at a time. Without it, the protocol is more
		// complex to handle concurrent adds.
		if (isNew && !this.prcb.isStillChecking(peer) && !this.prcb.isSafe(peer)) {
			this.prcb.open(peer, false);
		}
		return isNew; // (TODO) maybe more meaningful return value
	}

	public boolean addNeighborTrySafeButIfNotFallbackToUnsafe(Node peer) {
		WholePRCcast other = (WholePRCcast) peer.getProtocol(WholePRCcast.PID);
		boolean isNew = this.addNeighbor(peer);
		boolean isSafe = false;
		if (isNew && !this.prcb.isStillChecking(peer) && !this.prcb.isSafe(peer)) {
			// ensure that no concurrent adds are performed
			this.prcb.open(peer, true); // from -- safe -> to
			other.prcb.open(this.node, true); // to -- safe -> from
			isSafe = true;
		} else {
			// let the safety check run
			isSafe = false;
		}
		return isSafe; // (TODO) maybe more meaningful return value
	}

	public boolean removeNeighbor(Node peer) {
		if (this.node.getID() == 706) {
			System.out.println("REM " + peer.getID());
		}
		boolean contained = this.outview.contains(peer);
		SprayWithRouting other = ((WholePRCcast) peer.getProtocol(WholePRCcast.PID)).swr;

		// #1 quick check
		assert (contained);
		assert ((contained && other.inview.contains(this.node)) || (!contained && !other.inview.contains(this.node)));
		// #2 remove from outview and corresponding inview if need be
		this.outview.removeNeighbor(peer);
		if (!this.outview.contains(peer)) {
			other.inview.remove(this.node);
			if (!this.inview.contains(peer)) {
				assert (!other.outview.contains(this.node));
				this.prcb.close(peer);
				other.prcb.close(this.node);
			}

		}
		return contained;
	}

	// CONTROL MESSAGES:

	public void sendMConnectTo(Node from, Node to, MConnectTo m) {
		// #1 mark nodes as currently used
		this.addRoute(from, this.node, to);
		// #2 send the message
		this._sendControlMessage(from, m, "connect to");
	}

	public void receiveMConnectTo(Node from, Node mediator, Node to) {
		this.addRoute(from, mediator, to);

		if (mediator == null) {
			this.addNeighborTrySafeButIfNotFallbackToUnsafe(to);
		} else {
			this.addNeighborUnsafe(to);
		}
	}

	public void sendMExchangeWith(Node dest, Integer qCounter) {
		SprayWithRouting other = ((WholePRCcast) dest.getProtocol(WholePRCcast.PID)).swr;
		assert (this.outview.contains(dest) && other.inview.contains(this.node));
		this.addRoute(dest, null, this.node);
		this._sendControlMessage(dest, new MExchangeWith(dest, this.node, qCounter), "exchange with");
	}

	public void receiveMExchangeWith(Node origin, MExchangeWith message) {
		SprayWithRouting other = ((WholePRCcast) origin.getProtocol(WholePRCcast.PID)).swr;
		if (!this.inview.contains(origin)) {
			System.out.println("@" + this.node.getID() + " ;; " + origin.getID());
		}
		assert (this.inview.contains(origin));// ||
												// other.routes.hasRoute(this.node));
		this.addRoute(this.node, null, origin);
		this.onPeriodicCall(origin, message);
	}

	public void addRoute(Node from, Node mediator, Node to) {
		this.routes.addRoute(from, mediator, to);
	}

	// (receive alpha)
	public void receiveMConnectFrom(Node from, Node mediator, Node to) {
		this.routes.addRoute(from, mediator, to);
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
		boolean route = this.routes.hasRoute(target);
		boolean direct = this.prcb.isSafe(target);

		assert (route || direct); // no route nor forward

		if (direct) {
			this._send(target, m); // forward
		} else if (route) {
			this._send(this.routes.getRoute(target), m); // route
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
		assert (!this.prcb.isSafe(receiver)); // was safe already

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
		assert (this.prcb.isSafe(to) || this.routes.inUse().contains(to));

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
			if (this.prcb.isSafe(n))
				result.add(n);
		}
		for (Node n : this.inview) {
			if (this.prcb.isSafe(n))
				result.add(n);
		}
		for (Node n : this.routes.inUse()) {
			// inuse should not have unsafe links
			assert (this.prcb.isSafe(n));
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
			if (n.isUp() && this.prcb.isSafe(n)) // (TODO) inUse included?
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

}
