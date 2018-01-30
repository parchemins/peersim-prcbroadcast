package descent.broadcast.causal.prcbroadcast;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.collections4.IteratorUtils;

import descent.bidirectionnal.MClose;
import descent.bidirectionnal.MOpen;
import descent.broadcast.causal.pcbroadcast.MForward;
import descent.broadcast.causal.pcbroadcast.MLockedBroadcast;
import descent.broadcast.causal.pcbroadcast.MRegularBroadcast;
import descent.broadcast.causal.pcbroadcast.MUnlockBroadcast;
import descent.broadcast.reliable.MReliableBroadcast;
import descent.rps.APeerSampling;
import descent.rps.IMessage;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

/**
 * Preventive reliable causal broadcast. It sends control messages to remove the
 * need of monotonically increasing structures. It does *not* rely on vectors
 * the size of which increases with the number of people that ever broadcast a
 * message. Message overhead is constant (same principle of pcbroadcast). Local
 * structure scales with network dynamicity and traffic (they vary over time).
 */
public class PreventiveReliableCausalBroadcast implements EDProtocol, CDProtocol {

	// #1 protocol
	private final static String PAR_PID = "pid";
	public static Integer pid;

	IRoutingService irs; // (TODO) init correctly

	// #2 reliability structure
	private Integer counter = 0;
	protected Node node;
	public HashMap<Node, HashSet<MReliableBroadcast>> expected;

	// #3 safety structures
	public HashMap<Node, HashSet<MReliableBroadcast>> buffersAlpha;
	public HashMap<Node, ArrayList<MReliableBroadcast>> buffersBeta;
	public HashMap<Node, HashSet<MReliableBroadcast>> buffersPi;
	public HashMap<Node, Boolean> receiptsOfPi;

	/////////

	public PreventiveReliableCausalBroadcast(String prefix) {
		PreventiveReliableCausalBroadcast.pid = Configuration
				.getPid(prefix + "." + PreventiveReliableCausalBroadcast.PAR_PID);

		this.buffersAlpha = new HashMap<Node, HashSet<MReliableBroadcast>>();
		this.buffersBeta = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.buffersPi = new HashMap<Node, HashSet<MReliableBroadcast>>();
		this.receiptsOfPi = new HashMap<Node, Boolean>();
	}

	public PreventiveReliableCausalBroadcast() {
		this.buffersAlpha = new HashMap<Node, HashSet<MReliableBroadcast>>();
		this.buffersBeta = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.buffersPi = new HashMap<Node, HashSet<MReliableBroadcast>>();
		this.receiptsOfPi = new HashMap<Node, Boolean>();
	}

	// SAFETY:

	/**
	 * This process wants to add another process in its out-view. This starts
	 * the safety check of the new link.
	 * 
	 * @param n
	 *            The new neighbor.
	 */
	public void openO(Node n) {
		// (TODO) remove n from link to use. n is not safe.
		irs.sendAlpha(this.node, n);
	}

	/**
	 * Another process want to add this process in its out-view. The link is not
	 * safe yet.
	 * 
	 * @param n
	 *            The neighbor that adds this process in its out-view.
	 * 
	 */
	public void openI(Node n) {
		// #0 (TODO) remove n from safe in-view links. Any messages related to
		// causal broadcast and received from this link should be ignored
		// altogether.
	}

	/**
	 * Just received the first control message of a safety protocol.
	 * 
	 * @param from
	 *            The node that started the safety protocol.
	 * @param to
	 *            to = this.node
	 */
	public void receiveAlpha(Node from, Node to) {
		this.buffersAlpha.put(from, new HashSet<MReliableBroadcast>());
		this.receiptsOfPi.put(from, false);
		this.irs.sendBeta(from, to);
	}

	/**
	 * Just received the second control message of safety protocol.
	 * 
	 * @param from
	 *            from = this.node
	 * @param to
	 *            The node we add in our out-view.
	 */
	public void receiveBeta(Node from, Node to) {
		this.buffersBeta.put(to, new ArrayList<MReliableBroadcast>());
		this.irs.sendPi(from, to);
	}

	/**
	 * Just received the third control message of safety protocol.
	 * 
	 * @param from
	 *            The node that add this process in its out-view.
	 * @param to
	 *            to = this.node
	 */
	public void receivePi(Node from, Node to) {
		this.buffersPi.put(from, new HashSet<MReliableBroadcast>());
		this.receiptsOfPi.put(from, true);
		this.irs.sendRho(from, to);
	}

	/**
	 * Just received the fourth and last control message of safety protocol.
	 * 
	 * @param from
	 *            from = this.node
	 * @param to
	 *            The node that we add as neighbor.
	 */
	public void receiveRho(Node from, Node to) {
		this.irs.sendBuffer(from, to, this.buffersBeta.get(to));
		this.buffersBeta.remove(to);
		// (TODO) add the neighbor to safe links
	}

	/**
	 * Just received the buffer of messages. Must ignore messages already
	 * delivered; must deliver messages never delivered; must expect messages
	 * already received but not by the sender.
	 * 
	 * @param from
	 *            The process that added this process in its out-view
	 * @param to
	 *            to = this.node
	 * @param buffer
	 *            The buffer of messages that makes the link safe.
	 */
	public void receiveBuffer(Node from, Node to, ArrayList<MReliableBroadcast> buffer) {
		// #1 filter useless messages of buffer
		buffer.removeAll(this.buffersAlpha.get(from)); // potential messages
		// #2 deliver messages that were not delivered (normal receive procedure
		// including forward)
		for (int i = 0; i < buffer.size(); ++i) {
			if (!this.buffersPi.get(from).contains(buffer.get(i))) {
				// (TODO) RECEIVE NOT CDELIVER
				this.cDeliver((MRegularBroadcast) buffer.get(i).getPayload());
			}
		}
		// #3 add expected messages to the link
		HashSet<MReliableBroadcast> toExpect = new HashSet<MReliableBroadcast>(buffersPi.get(from));
		toExpect.removeAll(buffer);
		this.expected.put(from, toExpect);
		// (TODO) adds the neighbor in our in-view
	}

	/**
	 * Clean the structure when a link is closed in our out-view.
	 * 
	 * @param neighbor
	 *            The process removed from our neighborhood.
	 */
	public void closeO(Node neighbor) {
		this.buffersBeta.remove(neighbor);
	}

	/**
	 * Clean the structure when a link is closed in our in-view.
	 * 
	 * @param neighbor
	 *            The process removed from our neighborhood.
	 */
	public void closeI(Node neighbor) {
		this.buffersAlpha.remove(neighbor);
		this.buffersPi.remove(neighbor);
		this.receiptsOfPi.remove(neighbor);
	}

	// DISSEMINATION:

	/**
	 * Broadcast the message with the guarantee that the delivery follows the
	 * happen before relationship.
	 * 
	 * @param message
	 *            The message to broadcast.
	 */
	public void cbroadcast(IMessage message) {
		MReliableBroadcast m = this.rbroadcast(message);
		this.buffering(m);
	}

	public MReliableBroadcast rbroadcast(IMessage message) {
		return null;
	}

	/**
	 * Deliver exactly once. In this class, messages arrive causally ready, no
	 * need to check.
	 * 
	 * @param m
	 *            The message delivered.
	 */
	public void rDeliver(MReliableBroadcast m) {
		this.buffering(m);
		this.cDeliver((MRegularBroadcast) m.getPayload());
	}

	/**
	 * Deliver the message. It follows causal order.
	 * 
	 * @param m
	 *            The message delivered.
	 */
	public void cDeliver(MRegularBroadcast m) {
		// nothing
	}

	/**
	 * 
	 * @param m
	 */
	private void buffering(MReliableBroadcast m) {
		for (Node n : this.buffersBeta.keySet()) {
			this.buffersBeta.get(n).add(m);
		}
		for (Node n : this.receiptsOfPi.keySet()) {
			if (this.receiptsOfPi.get(n)) {
				this.buffersPi.get(n).add(m);
			} else {
				this.buffersAlpha.get(n).add(m);
			}
		}
	}

	// PEERSIM-RELATED:

	public void processEvent(Node node, int protocolId, Object message) {
		this._setNode(node);
	}

	public void nextCycle(Node node, int protocolId) {
		this._setNode(node);
	}

	/**
	 * Lazy loading the node.
	 * 
	 * @param n
	 *            The node hosting this protocol.
	 */
	protected void _setNode(Node n) {
		if (this.node == null) {
			this.node = n;
		}
	}

	@Override
	public Object clone() {
		return new PreventiveReliableCausalBroadcast();
	}

}
