package descent.broadcast.causal.prcbroadcast.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.collections4.bag.HashBag;

import descent.broadcast.causal.prcbroadcast.MBuffer;
import descent.broadcast.causal.prcbroadcast.PreventiveReliableCausalBroadcast;
import descent.broadcast.reliable.MReliableBroadcast;
import descent.rps.APeerSampling;
import descent.rps.IMessage;
import descent.rps.IPeerSampling;
import descent.spray.SprayPartialView;
import peersim.config.FastConfig;
import peersim.core.Node;
import peersim.transport.Transport;

/**
 * Spray with a light form of routing capabilities : the mediator knows both
 * peers and routes control messages back and forth until new links become safe.
 */
public class SprayWithRouting extends APeerSampling implements IRoutingService {

	public static int pid; // (TODO) rework that

	public PreventiveReliableCausalBroadcast parent; // (TODO) Interface CB + PS

	public HashMap<FromTo, Node> paths;

	public SprayPartialView outview;
	public HashBag<Node> inview;

	public HashSet<Node> unsafe;

	public SprayWithRouting(PreventiveReliableCausalBroadcast parent) {
		this.parent = parent;

		this.paths = new HashMap<FromTo, Node>();

		this.outview = new SprayPartialView();
		this.inview = new HashBag<Node>();

		this.unsafe = new HashSet<Node>();
	}

	// PEER-SAMPLING:

	public void periodicCall() {
		// TODO Auto-generated method stub

	}

	public IMessage onPeriodicCall(Node origin, IMessage message) {
		// TODO Auto-generated method stub
		return null;
	}

	public void join(Node joiner, Node contact) {
		this._clear();
		this._setNode(joiner);
		if (contact != null) {
			// #1 the very first connection is safe
			this.outview.addNeighbor(contact);
			// #2 subsequent ones might not be
			SprayWithRouting swr = (SprayWithRouting) contact.getProtocol(SprayWithRouting.pid);
			swr.onSubscription(joiner);
		}
		this.isUp = true;
	}

	public void onSubscription(Node origin) {
		this.inview.add(origin);
		// TODO Auto-generated method stub

	}

	public void leave() {
		// #0 Goes down.
		this.isUp = false;
		// #1 Immediately remove in the in-views.
		for (Node neighbor : this.outview.getPeers()) {
			SprayWithRouting swr = (SprayWithRouting) neighbor.getProtocol(SprayWithRouting.pid);
			swr._closeI(this.node);
		}
	}

	/**
	 * A neighbor in our in-view just left. Remove the occurences in our local
	 * structure.
	 * 
	 * @param leaver
	 *            The leaver identity.
	 */
	private void _closeI(Node leaver) {
		this.inview.remove(leaver);
		this.parent.closeI(leaver);
		this.unsafe.remove(leaver);
		this._removeAllRoutes(leaver);
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

	public void receiveMConnectTo(Node from, Node to, Node mediator) {
		// (TODO)
	}

	public void sendAlpha(Node from, Node to) {
		// TODO Auto-generated method stub

	}

	public void sendBeta(Node from, Node to) {
		// TODO Auto-generated method stub

	}

	public void sendPi(Node from, Node to) {
		// TODO Auto-generated method stub

	}

	public void sendRho(Node from, Node to) {
		// TODO Auto-generated method stub

	}

	public void sendBuffer(Node from, Node to, ArrayList<MReliableBroadcast> buffer) {
		// #0 bidirectional, Process "to" also sends a buffer
		Node sender = from;
		Node receiver = to;
		if (this.node == to) {
			sender = to;
			receiver = from;
		}
		// #1 check if there is an issue with algo
		if (!this.unsafe.contains(receiver)) {
			System.out.println("Send buffer but seems safe already");
			return;
		}
		// #2 send the buffer
		MBuffer m = new MBuffer(from, to, buffer);
		((Transport) this.node.getProtocol(FastConfig.getTransport(SprayWithRouting.pid))).send(sender, receiver, m,
				SprayWithRouting.pid);
	}

	/**
	 * Send a message using safe channels
	 * 
	 * @param to
	 */
	private void _send(Node to) {

	}

	public void sendToOutview(MReliableBroadcast m) {
		for (Node n : this.getOutview()) {
			((Transport) this.node.getProtocol(FastConfig.getTransport(SprayWithRouting.pid))).send(this.node, n, m,
					SprayWithRouting.pid);
		}
	}

	public HashSet<Node> getOutview() {
		// since bidirectionnal, outview includes inview
		HashSet<Node> result = new HashSet<Node>();
		for (Node n : this.outview.getPeers()) {
			if (!this.unsafe.contains(n))
				result.add(n);
		}
		for (Node n : this.inview) {
			if (!this.unsafe.contains(n))
				result.add(n);
		}
		return result;
	}

	public void setNeighborSafe(Node n) {
		this.unsafe.remove(n);
	}

	public void setNeighborUnsafe(Node n) {
		this.unsafe.add(n);
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
	public IPeerSampling clone() {
		return new SprayWithRouting(this.parent);
	}

	@Override
	protected boolean pFail(List<Node> path) {
		return false;
	}

	/**
	 * Reset the internal structures. Not very clean but garbage collecting will
	 * do the job.
	 */
	private void _clear() {
		this.paths = new HashMap<FromTo, Node>();
		this.outview = new SprayPartialView();
		this.inview = new HashBag<Node>();
		this.unsafe = new HashSet<Node>();
	}

}
