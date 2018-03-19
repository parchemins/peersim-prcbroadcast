package descent.causalbroadcast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import descent.causalbroadcast.messages.MRegularBroadcast;
import descent.causalbroadcast.messages.MReliableBroadcast;
import descent.causalbroadcast.routingbispray.IRoutingService;
import descent.causalbroadcast.routingbispray.SprayWithRouting;
import descent.rps.IMessage;
import peersim.cdsim.CDProtocol;
import peersim.core.Node;
import peersim.edsim.EDProtocol;

/**
 * Preventive reliable causal broadcast. It sends control messages to remove the
 * need of monotonically increasing structures. It does *not* rely on vectors
 * the size of which increases with the number of people that ever broadcast a
 * message. Message overhead is constant (same principle of pcbroadcast). Local
 * structure scales with network dynamicity and traffic (they vary over time).
 */
public class PreventiveReliableCausalBroadcast implements EDProtocol, CDProtocol {

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

	/////////

	public PreventiveReliableCausalBroadcast(String prefix) {
		this.expected = new HashMap<Node, HashSet<MReliableBroadcast>>();

		this.buffersAlpha = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.buffersPi = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.receiptsOfPi = new HashMap<Node, Boolean>();

		this.unsafe = new HashSet<Node>();
	}

	public PreventiveReliableCausalBroadcast() {
		this.expected = new HashMap<Node, HashSet<MReliableBroadcast>>();

		this.buffersAlpha = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.buffersPi = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.receiptsOfPi = new HashMap<Node, Boolean>();

		this.unsafe = new HashSet<Node>();
	}

	// SAFETY:

	/**
	 * This process wants to add another process in its out-view. This starts
	 * the safety check of the new link.
	 * 
	 * @param to
	 *            The new neighbor.
	 */
	public boolean openO(Node to) {
		boolean alreadySafe = this.irs.isSafe(to);
		boolean isNew = this.irs.addToOutView(to);
		// not (already safe or being safety checked)
		if (isNew && !alreadySafe) {
			// brand new link
			this.unsafe.add(to);

			assert (!this.irs.isSafe(to));

			// start safety check communication pattern
			this.irs.sendAlpha(this.node, to);
		} else if (isNew && alreadySafe) {
			// already in inview and safe
			SprayWithRouting other = (SprayWithRouting) ((WholePRCcast) to.getProtocol(WholePRCcast.PID)).swr;
			assert (other.isSafe(this.node));
			other.addToInView(this.node);
			this.irs.sendMRemoveRoute(to);
		} else if (!isNew && !alreadySafe) {
			// already being safety checked
			assert (((SprayWithRouting) ((WholePRCcast) this.node.getProtocol(WholePRCcast.PID)).swr).outview
					.contains(to));
			this.irs.sendMRemoveRoute(to);
		}
		return isNew || alreadySafe || (!alreadySafe && !isNew);
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
		assert (!this.irs.isSafe(from));

		this.unsafe.add(from);

		this.buffersAlpha.put(from, new ArrayList<MReliableBroadcast>());
		this.buffersPi.put(from, new ArrayList<MReliableBroadcast>());
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
		assert (!this.irs.isSafe(to));

		this.buffersAlpha.put(to, new ArrayList<MReliableBroadcast>());
		this.buffersPi.put(to, new ArrayList<MReliableBroadcast>());
		this.receiptsOfPi.put(to, false);
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
		assert (!this.irs.isSafe(from));

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
		assert (!this.irs.isSafe(to));

		if (PreventiveReliableCausalBroadcast.TYPE == EArcType.BIDIRECTIONAL) {
			// #A continue protocol
			this.receiptsOfPi.put(to, true);
			this.irs.sendBuffer(to, from, to, this.buffersAlpha.get(to));
			// safe "from"->"to", not safe on receipt : no clean yet
			this.unsafe.remove(to);
		} else {
			// #B it 's enough for directional
			this.clean(to);
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
		Node origin = to;
		// #0 send a buffer back
		if (this.node == to) {
			origin = from;
			if (PreventiveReliableCausalBroadcast.TYPE == EArcType.BIDIRECTIONAL) {
				// last exchange for bidirectional safety
				this.irs.sendBuffer(from, from, to, this.buffersPi.get(from));
			}
		}

		// from -> to is already safe when rho is received
		// to -> from becomes safe here
		assert ((this.node == from && this.irs.isSafe(to)) || (this.node == to && !this.irs.isSafe(origin)));

		// #1 filter useless messages of buffer
		bufferBeta.removeAll(this.buffersAlpha.get(origin)); // potential
		// #2 deliver messages that were not delivered (normal receive procedure
		// including forward)
		for (int i = 0; i < bufferBeta.size(); ++i) {
			if (!this.buffersPi.get(from).contains(bufferBeta.get(i))) {
				this.receive(bufferBeta.get(i), origin);
			}
		}
		// #3 add expected messages to the link
		HashSet<MReliableBroadcast> toExpect = new HashSet<MReliableBroadcast>(this.buffersPi.get(origin));
		toExpect.removeAll(bufferBeta);
		this.expected.put(origin, toExpect);

		this.clean(origin);

		// #4 add to inview
		if (this.node == to && PreventiveReliableCausalBroadcast.TYPE == EArcType.BIDIRECTIONAL) {
			this.irs.addToInView(origin);
		}
	}

	/**
	 * Clean the structure when a link is closed in our out-view.
	 * 
	 * @param neighbor
	 *            The process removed from our neighborhood.
	 */
	public void closeO(Node neighbor) {
		this.clean(neighbor);
	}

	/**
	 * Clean the structure when a link is closed in our in-view.
	 * 
	 * @param neighbor
	 *            The process removed from our neighborhood.
	 */
	public void closeI(Node neighbor) {
		this.clean(neighbor);
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
	public void _setNode(Node n) {
		if (this.node == null) {
			this.node = n;
		}
	}

	public void setIRS(IRoutingService irs) {
		this.irs = irs;
	}

	public boolean isUnsafe(Node neighbor) {
		return this.unsafe.contains(neighbor);
	}

	@Override
	public Object clone() {
		return new PreventiveReliableCausalBroadcast();
	}

}
