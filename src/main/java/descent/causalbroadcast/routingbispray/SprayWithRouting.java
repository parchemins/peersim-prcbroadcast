package descent.causalbroadcast.routingbispray;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.collections4.bag.HashBag;

import descent.causalbroadcast.IPRCB;
import descent.causalbroadcast.WholePRCcast;
import descent.causalbroadcast.messages.IMControlMessage;
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

	private Integer numberOfControlMessagesSentSinceLastCheck = 0;

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

			// System.out.println("PERIODIC @" + this.node.getID() + "; " +
			// q.getID());
			// #2 prepare a sample
			HashBag<Node> sample = this._getSample(q);

			assert (!sample.isEmpty()); // contains at least q

			// #3 lock links for routing purpose and #4 send connection messages
			Integer qCounter = 0;
			for (Node neighbor : sample) {
				if (neighbor != this.node) {
					// System.out.println("from " + q.getID() + " -> " +
					// neighbor.getID());
					this.sendMConnectTo(q, neighbor);
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
			// no currently used as route && from <- safe -> to
			if (!this.routes.inUse().contains(neighbor) && !this.prcb.isStillChecking(neighbor)
					&& this.prcb.canSend(neighbor)) {
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
			if (this.routes.inUse().contains(neighbor) || this.prcb.isStillChecking(neighbor)
					|| !this.prcb.canSend(neighbor)) {
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
			}
			clone.remove(neighbor, 1);
		}

		return sample;
	}

	public IMessage onPeriodicCall(Node origin, IMessage message) {
		MExchangeWith m = (MExchangeWith) message;

		// System.out.println("ON PERIODIC @" + this.node.getID() + ";; " +
		// origin.getID() + " x" + m.nbReferences);
		SprayWithRouting other = ((WholePRCcast) origin.getProtocol(WholePRCcast.PID)).swr;

		for (int i = 0; i < m.nbReferences; ++i)
			other.removeNeighbor(this.node);
		for (int i = 0; i < m.nbReferences; ++i)
			this.addNeighborTrySafeButIfNotFallbackToUnsafe(m);

		HashBag<Node> sample = this._getSample(null);

		for (Node neighbor : sample) {
			if (neighbor != origin) {
				this.sendMConnectTo(origin, neighbor);
				this.removeNeighbor(neighbor);
			}
		}

		return null;
	}

	public void join(Node joiner, Node contact) {
		this._setNode(joiner);
		this.routes.setNode(joiner);
		this.prcb.setNode(joiner);

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
		for (Node neighbor : this.outview.partialView) {
			if (this.prcb.canSend(neighbor)) {
				safeNeighbors.add(neighbor);
			}
		}
		if (safeNeighbors.isEmpty()) {
			// #1 keep the subscription for ourself
			this.addNeighbor(origin); // already safe by design
		} else {
			// #2 share the subscription to neighbors
			for (Node neighbor : safeNeighbors) {
				this.sendMConnectTo(neighbor, origin);
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
		assert (((alreadyContained || this.routes.inUse().contains(peer)) && other.inview.contains(this.node))
				|| (!alreadyContained && !this.routes.inUse().contains(peer) && !other.inview.contains(this.node)));

		// #2 add in both directions
		this.outview.addNeighbor(peer);
		// System.out.println("@" +other.node.getID() + " ADD INVIEW " +
		// this.node.getID());
		other.inview.add(this.node);

		return !alreadyContained;
	}

	public boolean addNeighborSafe(Node peer) {
		WholePRCcast other = (WholePRCcast) peer.getProtocol(WholePRCcast.PID);
		boolean isNew = this.addNeighbor(peer);
		assert (isNew);
		// from -- safe -> to
		this.prcb.open(new MConnectTo(this.node, null, peer), true);
		// to -- safe -> from
		other.prcb.open(new MConnectTo(this.node, null, peer), true);
		return isNew; // (TODO) maybe more meaningful return value
	}

	public boolean addNeighborUnsafe(MConnectTo m) {
		boolean isNew = this.addNeighbor(m.to);
		// last part of condition is a cheat to ensure that only one
		// safety checking run at a time. Without it, the protocol is more
		// complex to handle concurrent adds.
		if (isNew && !this.prcb.isStillChecking(m.to) && !this.prcb.canSend(m.to)) {
			this.prcb.open(m, false);
		}
		return isNew; // (TODO) maybe more meaningful return value
	}

	public boolean addNeighborTrySafeButIfNotFallbackToUnsafe(MExchangeWith m) {
		WholePRCcast other = (WholePRCcast) m.to.getProtocol(WholePRCcast.PID);
		boolean isNew = this.addNeighbor(m.to);
		boolean isSafe = false;
		if (isNew && !this.prcb.isStillChecking(m.to) && !this.prcb.canSend(m.to)) {
			// ensure that no concurrent adds are performed
			// from -- safe -> to
			this.prcb.open(new MConnectTo(m.from, null, m.to), true);
			// to -- safe -> from
			other.prcb.open(new MConnectTo(m.from, null, m.to), true);
			isSafe = true;
		} else {
			// let the safety check run
			isSafe = false;
		}
		return isSafe; // (TODO) maybe more meaningful return value
	}

	public boolean removeNeighbor(Node peer) {
		boolean contained = this.outview.contains(peer);
		SprayWithRouting other = ((WholePRCcast) peer.getProtocol(WholePRCcast.PID)).swr;

		// #1 quick check
		assert (contained);
		assert ((contained && other.inview.contains(this.node)) || (!contained && !other.inview.contains(this.node)));
		// #2 remove from outview and corresponding inview if need be
		this.outview.removeNeighbor(peer);
		if (!this.outview.contains(peer) && !this.routes.inUse().contains(peer)) {
			// (TODO) this code is similar as the one in remove route, factorize

			// System.out.println("@"+other.node.getID()+ " REMOVE FROM INVIEW "
			// + this.node.getID());
			other.inview.remove(this.node);
			if (!this.inview.contains(peer)) {
				assert (!other.outview.contains(this.node));
				assert (!other.routes.inUse().contains(this.node));
				this.prcb.close(peer);
				other.prcb.close(this.node);
			}

		}
		return contained;
	}

	public void removedRoute(Route r) {
		Node peer = (r.isUsingMediator()) ? r.mediator : r.dest;

		SprayWithRouting other = ((WholePRCcast) peer.getProtocol(WholePRCcast.PID)).swr;
		assert (other.inview.contains(this.node) || other.outview.contains(this.node));

		if (!this.outview.contains(peer) && !this.routes.inUse().contains(peer)) {
			// System.out.println("@"+other.node.getID()+ " REMOVE FROM INVIEW "
			// + this.node.getID());
			other.inview.remove(this.node);
			if (!this.inview.contains(peer)) {
				assert (!other.outview.contains(this.node));
				assert (!other.routes.inUse().contains(this.node));
				this.prcb.close(peer);
				other.prcb.close(this.node);
			}

		}
	}

	// CONTROL MESSAGES:

	public void sendMConnectTo(Node from, Node to) {
		// #1 mark nodes as currently used
		this.addRoute(from, this.node, to);
		// #2 send the message
		this._sendControlMessage(from, new MConnectTo(from, this.node, to));
	}

	public void receiveMConnectTo(MConnectTo m) {
		assert (m.from == this.node);

		if (m.mediator == null) {
			this.addNeighborTrySafeButIfNotFallbackToUnsafe(new MExchangeWith(m.from, m.to, 1)); // ugly
		} else {
			this.addNeighborUnsafe(m);
		}
	}

	public void sendMExchangeWith(Node dest, Integer qCounter) {
		SprayWithRouting other = ((WholePRCcast) dest.getProtocol(WholePRCcast.PID)).swr;
		assert (this.outview.contains(dest));
		assert (other.inview.contains(this.node));
		this.addRoute(dest, null, this.node);
		this._sendControlMessage(dest, new MExchangeWith(dest, this.node, qCounter));
	}

	public void receiveMExchangeWith(MExchangeWith m) {
		assert (m.from == this.node);
		assert (this.inview.contains(m.to));
		this.onPeriodicCall(m.to, m);
	}

	public void addRoute(Node from, Node mediator, Node to) {
		if (mediator == null && this.node == from) {
			// #A this --> to
			SprayWithRouting other = ((WholePRCcast) to.getProtocol(WholePRCcast.PID)).swr;
			other.inview.add(this.node);
		} else if (mediator == null && this.node == to) {
			// #B from --> this
			SprayWithRouting other = ((WholePRCcast) from.getProtocol(WholePRCcast.PID)).swr;
			other.inview.add(this.node);

		} else if (this.node == mediator) {
			// #C from --> this --> to
			SprayWithRouting otherF = ((WholePRCcast) from.getProtocol(WholePRCcast.PID)).swr;
			otherF.inview.add(this.node);
			SprayWithRouting otherT = ((WholePRCcast) to.getProtocol(WholePRCcast.PID)).swr;
			otherT.inview.add(this.node);
		} else if (mediator != null) {
			// #D this -> mediator -> to || from -> mediator -> this
			SprayWithRouting other = ((WholePRCcast) mediator.getProtocol(WholePRCcast.PID)).swr;
			other.inview.add(this.node);
		}
		this.routes.addRoute(from, mediator, to);
	}

	// from: process A; to: process B; A -> alpha -> B
	public void sendAlpha(MConnectTo m) {
		this._sendControlMessage(m.to, new MAlpha(m.from, m.mediator, m.to));
	}

	// from: process A; to: process B; B -> beta -> A
	public void sendBeta(MAlpha m) {
		this._sendControlMessage(m.from, new MBeta(m.from, m.mediator, m.to));
	}

	// from: process A; to: process B; A -> pi -> B
	public void sendPi(MBeta m) {
		this._sendControlMessage(m.to, new MPi(m.from, m.mediator, m.to));
	}

	// from: process A; to: process B; B -> rho -> A
	public void sendRho(MPi m) {
		this._sendControlMessage(m.from, new MRho(m.from, m.mediator, m.to));
	}

	public void _sendControlMessage(Node dest, IMControlMessage m) {
		++this.numberOfControlMessagesSentSinceLastCheck;

		if (m.getMediator() != null && m.getMediator() != this.node) {
			assert (this.routes.inUse().contains(m.getMediator()));
			this._send(m.getMediator(), m);
		} else if (m.getMediator() != null && m.getMediator() == this.node) {
			assert (this.routes.inUse().contains(m.getReceiver()));
			this._send(m.getReceiver(), m);
		} else if (m.getMediator() == null) {
			assert (this.routes.inUse().contains(dest));
			this._send(dest, m);
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
		assert (this.prcb.canSend(to) || this.routes.inUse().contains(to));

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
			// System.out.println("@" + this.node.getID() + "       --> " + n.getID() + " " + m.toString());
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
			if (this.prcb.canSend(n))
				result.add(n);
		}
		for (Node n : this.inview) {
			if (this.prcb.canSend(n))
				result.add(n);
		}
		for (Node n : this.routes.inUse()) {
			assert (this.prcb.canSend(n));
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
			if (n.isUp() && this.prcb.canSend(n)) // (TODO) inUse included?
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

	////// FOR PEERSIM OBSERVER

	public Integer getNumberOfControlMessagesSentSinceLastCheck() {
		Integer result = this.numberOfControlMessagesSentSinceLastCheck;
		this.numberOfControlMessagesSentSinceLastCheck = 0;
		return result;
	}

}
