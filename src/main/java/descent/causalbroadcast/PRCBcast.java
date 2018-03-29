package descent.causalbroadcast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import descent.causalbroadcast.messages.MAlpha;
import descent.causalbroadcast.messages.MBeta;
import descent.causalbroadcast.messages.MPi;
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

	// #4 just a consistency checker
	private static boolean VECTOR_CLOCK_CHECK = true;
	public HashMap<Node, Integer> vectorClock;

	////////////////////////////////////////////////////////////////////////////

	public PRCBcast() {
		this.expected = new HashMap<Node, HashSet<MReliableBroadcast>>();

		this.buffersAlpha = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.buffersPi = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.receiptsOfPi = new HashMap<Node, Boolean>();

		this.unsafe = new HashSet<Node>();
		this.safe = new HashSet<Node>();

		this.vectorClock = new HashMap<Node, Integer>();
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
		assert (!this.canReceive(other));
		assert (!this.canSend(other));

		if (bypassSafety) {
			assert (m.mediator == null);
			// #A peer-sampling knows the neighbor is safe by design
			this.safe.add(other);
			this.expected.put(other, new HashSet<MReliableBroadcast>());
		} else {
			// #B otherwise, safety check starts
			assert (this.node == m.from);
			this.unsafe.add(m.to);
			this.irs.sendAlpha(m);
		}
	}

	public void close(Node to) {
		assert (this.node != to);
		// System.out.println("@" + this.node.getID() + " CLOSE " + to.getID());
		assert (this.isSafe(to) || this.isYetToBeSafe(to));

		this.cleanSafetyChecking(to);
		this.expected.remove(to);
		this.safe.remove(to);

		assert (!this.canSend(to));
		assert (!this.canReceive(to));
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
		assert (!this.safe.contains(m.from));
		assert (!this.unsafe.contains(m.from));
		assert (!this.canReceive(m.from));
		assert (!this.canSend(m.from));

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
		assert (!this.safe.contains(m.to));
		assert (this.unsafe.contains(m.to));
		assert (!this.canReceive(m.to));
		assert (!this.canSend(m.to));

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
		assert (!this.safe.contains(m.from));
		assert (this.unsafe.contains(m.from));
		assert (!this.canReceive(m.from));
		assert (!this.canSend(m.from));

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
		assert (!this.safe.contains(m.to));
		assert (this.unsafe.contains(m.to));
		assert (!this.canReceive(m.to));
		assert (!this.canSend(m.to));

		this.unsafe.remove(m.to);
		this.safe.add(m.to); // (here in case latency is 0)
		// safe "from"->"to", not safe on receipt : no clean yet
		if (PRCBcast.TYPE == EArcType.BIDIRECTIONAL) {
			this.receiptsOfPi.put(m.to, true);
		}

		// System.out.println("@" + this.node.getID() + " ====== >" +
		// m.to.getID());
		this.irs.sendBuffer(m.to, m.from, m.to, this.buffersAlpha.get(m.to));

		if (PRCBcast.TYPE == EArcType.DIRECTIONAL) {
			// #B it 's enough for directional
			this.cleanSafetyChecking(m.to);
		}

		assert (this.canSend(m.to));
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
			assert (this.safe.contains(to));
			assert (!this.unsafe.contains(to));
			assert (this.canSend(to));
			assert (!this.canReceive(to));

			this.receiveBufferCommon(to, bufferBeta);

			assert (this.canReceive(to));
		} else {

			assert (!this.safe.contains(from));
			assert (this.unsafe.contains(from));
			assert (!this.canSend(from));
			assert (!this.canReceive(from));

			this.receiveBufferCommon(from, bufferBeta);

			if (PRCBcast.TYPE == EArcType.BIDIRECTIONAL) {
				// System.out.println("@" + this.node.getID() + " ~~~~~~> " +
				// from.getID());
				this.irs.sendBuffer(from, from, this.node, this.buffersPi.get(from));
			}

			this.cleanSafetyChecking(from);

			this.safe.add(from);
			this.unsafe.remove(from);

			assert (this.canSend(from));
			assert (this.canReceive(from));
		}
	}

	public void receiveBufferCommon(Node origin, ArrayList<MReliableBroadcast> bufferBeta) {

		/*
		 * if (this.node.getID() == 36 || this.node.getID() == 33) {
		 * System.out.println("ALPHA " +
		 * Arrays.toString(this.buffersAlpha.get(origin).toArray()));
		 * System.out.println("BETA " + Arrays.toString(bufferBeta.toArray()));
		 * System.out.println("PI " +
		 * Arrays.toString(this.buffersPi.get(origin).toArray())); }
		 */

		// #1 filter useless messages of buffer
		bufferBeta.removeAll(this.buffersAlpha.get(origin)); // potential
		// #2 deliver messages that were not delivered (normal receive procedure
		// including forward)
		this.expected.put(origin, new HashSet<MReliableBroadcast>());
		ArrayList<MReliableBroadcast> toDeliver = new ArrayList<MReliableBroadcast>();
		for (int i = 0; i < bufferBeta.size(); ++i) {
			if (!this.buffersPi.get(origin).contains(bufferBeta.get(i))) {
				toDeliver.add(bufferBeta.get(i).copy(origin));
			}
		}
		// #3 add expected messages to the link
		HashSet<MReliableBroadcast> toExpect = new HashSet<MReliableBroadcast>(this.buffersPi.get(origin));
		toExpect.removeAll(bufferBeta);

		this.expected.put(origin, toExpect);

		for (MReliableBroadcast m : toDeliver) {
			assert (!this.isAlreadyReceived(m));
			this.receive(m, m.sender);
		}

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
		// MReliableBroadcast m =
		this.rbroadcast(message);
		// this.buffering(m);
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
		MReliableBroadcast m = new MReliableBroadcast(this.node, this.node, ++this.counter, message);
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
		if (!this.received(m, from)) {
			this.irs.sendToOutview(m.copy(this.node));
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
		// #1 check if the message has been received previously
		boolean hasReceived = this.isAlreadyReceived(m);
		// #2 if it has, we purge the specific entry
		if (hasReceived) {
			assert (this.expected.get(from).remove(m)); // no double receipts
		} else {
			// #3 if not, we expect to receive the message from all other
			// neighbors
			for (Node n : this.expected.keySet()) {
				if (this.expected.containsKey(n)) {
					this.expected.get(n).add(m);
				}
			}
			// null when its the original sender
			if (from != null && this.expected.containsKey(from)) {
				assert (this.expected.get(from).remove(m));
			}
		}
		return hasReceived;

	}

	private boolean isAlreadyReceived(MReliableBroadcast m) {
		for (Node n : this.expected.keySet()) {
			if (this.expected.get(n).contains(m)) {
				return true;
			}
		}
		return false;
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
		this.cDeliver((IMessage) m.getPayload());

		if (PRCBcast.VECTOR_CLOCK_CHECK) {
			Node origin = m.origin;
			Integer counter = m.counter;
			if (!this.vectorClock.containsKey(m.origin)) {
				assert (counter == 1);
				this.vectorClock.put(origin, counter);
			} else {
				assert (counter == this.vectorClock.get(origin) + 1);
				this.vectorClock.put(origin, counter);
			}
		}
	}

	/**
	 * Deliver the message. It follows causal order.
	 * 
	 * @param m
	 *            The message delivered.
	 */
	public void cDeliver(IMessage m) {
		// nothing
	}

	/**
	 * 
	 * @param m
	 */
	private void buffering(MReliableBroadcast m) {
		for (Node n : this.receiptsOfPi.keySet()) {
			// if (this.buffersPi.get(n).contains(m)) {
			// System.out.println("MIAOU @" + this.node.getID() + " ==== " +
			// m.toString());
			// }
			assert (!this.buffersPi.get(n).contains(m));
			assert (!this.buffersAlpha.get(n).contains(m));

			if (this.receiptsOfPi.get(n)) {
				this.buffersPi.get(n).add(m);
			} else {
				this.buffersAlpha.get(n).add(m);
			}
		}
	}

	private void cleanSafetyChecking(Node neighbor) {
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

	public boolean canReceive(Node neighbor) {
		assert (!this.expected.containsKey(neighbor) || (this.expected.containsKey(neighbor)
				&& !this.unsafe.contains(neighbor) && this.safe.contains(neighbor)));
		return this.expected.containsKey(neighbor);
	}

	public boolean canSend(Node neighbor) {
		return this.safe.contains(neighbor);
	}

}
