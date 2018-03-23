package descent.causalbroadcast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import descent.causalbroadcast.messages.MAlpha;
import descent.causalbroadcast.messages.MBeta;
import descent.causalbroadcast.messages.MPi;
import descent.causalbroadcast.messages.MRegularBroadcast;
import descent.causalbroadcast.messages.MReliableBroadcast;
import descent.causalbroadcast.messages.MRho;
import descent.causalbroadcast.routingbispray.IRoutingService;
import descent.causalbroadcast.routingbispray.MConnectTo;
import descent.rps.IMessage;
import peersim.core.Node;

/**
 * Preventive reliable causal broadcast. It sends control messages to remove the
 * need of monotonically increasing structures. It does *not* rely on vectors
 * the size of which increases with the number of people that ever broadcast a
 * message. Message overhead is constant (same principle of pcbroadcast). Local
 * structure scales with network dynamicity and traffic (they vary over time).
 */
public class PRCBcast implements IPRCB {

	IRoutingService irs;

	// #1 check safety directional or bidirectional
	private static EArcType TYPE = EArcType.BIDIRECTIONAL;

	// #2 reliability structure
	private Integer counter = 0;
	protected Node node;
	public HashMap<Node, HashSet<MReliableBroadcast>> expected;

	// #3 safety structures
	public HashMap<Node, ArrayList<MReliableBroadcast>> buffersAlpha;
	public HashMap<Node, ArrayList<MReliableBroadcast>> buffersPi;
	public HashMap<Node, Boolean> receiptsOfPi;

	public HashSet<Node> unsafe;
	public HashSet<Node> safe;

	////////////////

	public PRCBcast() {
		this.expected = new HashMap<Node, HashSet<MReliableBroadcast>>();

		this.buffersAlpha = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.buffersPi = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.receiptsOfPi = new HashMap<Node, Boolean>();

		this.unsafe = new HashSet<Node>();
		this.safe = new HashSet<Node>();
	}

	// SAFETY:

	/**
	 * This process wants to add another process in its out-view. This starts
	 * the safety check of the new link.
	 * 
	 * @param to
	 *            The new neighbor.
	 */
	public void open(MConnectTo m, boolean bypassSafety) {
		Node other = (m.from == this.node) ? m.to : m.from;
		assert (!this.isStillChecking(other));
		assert (!this.safe.contains(other));
		assert (!this.unsafe.contains(other));
		if (bypassSafety) {
			assert (m.mediator == null);
			// #A peer-sampling knows the neighbor is safe by design
			this.safe.add(other);
		} else {
			// #B otherwise, safety check starts
			assert (this.node == m.from);
			this.unsafe.add(m.to);
			this.irs.sendAlpha(m);
		}
	}

	public void close(Node to) {
		assert (this.isSafe(to) || this.isYetToBeSafe(to));
		this.clean(to);
		this.safe.remove(to);
	}

	/**
	 * Just received the first control message of a safety protocol.
	 * 
	 * @param from
	 *            The node that started the safety protocol.
	 * @param to
	 *            to = this.node
	 */
	public void receiveAlpha(MAlpha m) {
		assert (!this.safe.contains(m.from) && !this.unsafe.contains(m.from));

		this.unsafe.add(m.from);

		this.buffersAlpha.put(m.from, new ArrayList<MReliableBroadcast>());
		this.buffersPi.put(m.from, new ArrayList<MReliableBroadcast>());
		this.receiptsOfPi.put(m.from, false);

		this.irs.sendBeta(m);
	}

	/**
	 * Just received the second control message of safety protocol.
	 * 
	 * @param from
	 *            from = this.node
	 * @param to
	 *            The node we add in our out-view.
	 */
	public void receiveBeta(MBeta m) {
		assert (!this.safe.contains(m.to) && this.unsafe.contains(m.to));

		this.buffersAlpha.put(m.to, new ArrayList<MReliableBroadcast>());
		this.buffersPi.put(m.to, new ArrayList<MReliableBroadcast>());
		this.receiptsOfPi.put(m.to, false);
		this.irs.sendPi(m);
	}

	/**
	 * Just received the third control message of safety protocol.
	 * 
	 * @param from
	 *            The node that add this process in its out-view.
	 * @param to
	 *            to = this.node
	 */
	public void receivePi(MPi m) {
		assert (!this.safe.contains(m.from) && this.unsafe.contains(m.from));

		this.receiptsOfPi.put(m.from, true);
		this.irs.sendRho(m);
	}

	/**
	 * Just received the fourth and last control message of safety protocol.
	 * 
	 * @param from
	 *            from = this.node
	 * @param to
	 *            The node that we add as neighbor.
	 */
	public void receiveRho(MRho m) {
		assert (!this.safe.contains(m.to) && this.unsafe.contains(m.to));

		if (PRCBcast.TYPE == EArcType.BIDIRECTIONAL) {
			// #A continue protocol
			this.receiptsOfPi.put(m.to, true);
			this.irs.sendBuffer(m.to, m.from, m.to, this.buffersAlpha.get(m.to));
			// safe "from"->"to", not safe on receipt : no clean yet
			this.unsafe.remove(m.to);
			this.safe.add(m.to);
		} else {
			// #B it 's enough for directional
			this.clean(m.to);
		}
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
	 * @param bufferBeta
	 *            The buffer of messages that makes the link safe.
	 */
	public void receiveBuffer(Node from, Node to, ArrayList<MReliableBroadcast> bufferBeta) {
		if (this.node == from) {
			this.receiveBufferAtFrom(to, bufferBeta);
			this.receiveBufferCommon(to, bufferBeta);
		} else {
			this.receiveBufferAtTo(from, bufferBeta);
			this.receiveBufferCommon(from, bufferBeta);
		}
	}

	public void receiveBufferAtFrom(Node origin, ArrayList<MReliableBroadcast> bufferBeta) {
		assert (this.safe.contains(origin));
		assert (!this.unsafe.contains(origin));
	}

	public void receiveBufferAtTo(Node origin, ArrayList<MReliableBroadcast> bufferBeta) {
		assert (!this.safe.contains(origin) && this.unsafe.contains(origin));
		// last exchange for bidirectional safety
		if (PRCBcast.TYPE == EArcType.BIDIRECTIONAL) {
			this.irs.sendBuffer(origin, origin, this.node, this.buffersPi.get(origin));
			this.unsafe.remove(origin);
			this.safe.add(origin);
		}
	}

	public void receiveBufferCommon(Node origin, ArrayList<MReliableBroadcast> bufferBeta) {
		// #1 filter useless messages of buffer
		bufferBeta.removeAll(this.buffersAlpha.get(origin)); // potential
		// #2 deliver messages that were not delivered (normal receive procedure
		// including forward)
		for (int i = 0; i < bufferBeta.size(); ++i) {
			if (!this.buffersPi.get(origin).contains(bufferBeta.get(i))) {
				this.receive(bufferBeta.get(i), origin);
			}
		}
		// #3 add expected messages to the link
		HashSet<MReliableBroadcast> toExpect = new HashSet<MReliableBroadcast>(this.buffersPi.get(origin));
		toExpect.removeAll(bufferBeta);
		this.expected.put(origin, toExpect);

		this.clean(origin);
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

	/**
	 * Reliable broadcast communication primitive.
	 * 
	 * @param message
	 *            The message to broadcast.
	 * @return {MReliableBroadcast} The message actually sent including its
	 *         control information.
	 */
	public MReliableBroadcast rbroadcast(IMessage message) {
		MReliableBroadcast m = new MReliableBroadcast(this.node.getID(), ++this.counter, message);
		this.receive(m, null);
		return m;
	}

	/**
	 * The broadcast protocol just received a broadcast message.
	 * 
	 * @param m
	 *            The message just received.
	 * @param from
	 *            From whom we just received the message.
	 */
	public void receive(MReliableBroadcast m, Node from) {
		if (!received(m, from)) {
			this.irs.sendToOutview(m);
			this.rDeliver(m);
		}
	}

	/**
	 * Checks in the local structure if the message has already been received.
	 * If not, it adds it to the local structure. If it has, it purges the
	 * structure from this specific entry.
	 * 
	 * @param m
	 *            The message to check.
	 * @param from
	 *            The node that just sent the message
	 * @return True if the message was already received, false otherwise.
	 */
	private boolean received(MReliableBroadcast m, Node from) {
		boolean hasReceived = false;
		Iterator<Node> keys = this.expected.keySet().iterator();
		// #1 check if the message has been received previously
		while (!hasReceived && keys.hasNext()) {
			Node n = keys.next();
			if (this.expected.get(n).contains(m)) {
				hasReceived = true;
			}
		}
		// #2 if it has, we purge the specific entry
		if (hasReceived) {
			this.expected.get(from).remove(from);
			if (this.expected.get(from).size() == 0) {
				this.expected.remove(from);
			}
		} else {
			// #3 if not, we expect to receive the message from all other
			// neighbors
			for (Node n : this.irs.getOutview()) {
				if (!this.expected.containsKey(n)) {
					this.expected.put(n, new HashSet<MReliableBroadcast>());
				}
				this.expected.get(n).add(m);
			}
			if (from != null) { // from is null when its the original sender
				this.expected.get(from).remove(from);
			}
		}
		return hasReceived;
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
		for (Node n : this.receiptsOfPi.keySet()) {
			if (this.receiptsOfPi.get(n)) {
				this.buffersPi.get(n).add(m);
			} else {
				this.buffersAlpha.get(n).add(m);
			}
		}
	}

	private void clean(Node neighbor) {
		this.unsafe.remove(neighbor);
		this.buffersAlpha.remove(neighbor);
		this.buffersPi.remove(neighbor);
		this.receiptsOfPi.remove(neighbor);
	}

	// PEERSIM-RELATED:

	/**
	 * Lazy loading the node.
	 * 
	 * @param n
	 *            The node hosting this protocol.
	 */
	public void setNode(Node n) {
		if (this.node == null)
			this.node = n;
	}

	public void setIRS(IRoutingService irs) {
		this.irs = irs;
	}

	public boolean isSafe(Node neighbor) {
		return this.safe.contains(neighbor);
	}

	public boolean isYetToBeSafe(Node neighbor) {
		return this.unsafe.contains(neighbor);
	}

	public boolean isNotSafe(Node neighbor) {
		return !this.safe.contains(neighbor);
	}

	/**
	 * Utility function that checks that at least one of involved processes is
	 * still performing the safety checking. It goes from sendingAlpha to
	 * receive the second buffer.
	 * 
	 * @param neighbor
	 * @return
	 */
	public boolean isStillChecking(Node neighbor) {
		PRCBcast other = ((WholePRCcast) neighbor.getProtocol(WholePRCcast.PID)).prcb;
		return this.isYetToBeSafe(neighbor) || this.buffersAlpha.containsKey(neighbor) || other.isYetToBeSafe(this.node)
				|| other.buffersAlpha.containsKey(this.node);
	}
}
