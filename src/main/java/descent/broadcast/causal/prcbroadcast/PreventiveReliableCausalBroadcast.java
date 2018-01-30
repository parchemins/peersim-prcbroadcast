package descent.broadcast.causal.prcbroadcast;

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

	// #2 reliability structure
	private Integer counter = 0;
	protected Node node;
	public HashMap<Node, HashSet<MReliableBroadcast>> expected;

	// #3 safety structures
	public HashMap<Node, ArrayList<MReliableBroadcast>> buffersAlpha;
	public HashMap<Node, ArrayList<MReliableBroadcast>> buffersBeta;
	public HashMap<Node, ArrayList<MReliableBroadcast>> buffersPi;
	public HashMap<Node, Boolean> receiptsOfPi;

	/////////

	public PreventiveReliableCausalBroadcast(String prefix) {
		PreventiveReliableCausalBroadcast.pid = Configuration
				.getPid(prefix + "." + PreventiveReliableCausalBroadcast.PAR_PID);

		this.buffersAlpha = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.buffersBeta = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.buffersPi = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.receiptsOfPi = new HashMap<Node, Boolean>();
	}

	public PreventiveReliableCausalBroadcast() {
		this.buffersAlpha = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.buffersBeta = new HashMap<Node, ArrayList<MReliableBroadcast>>();
		this.buffersPi = new HashMap<Node, ArrayList<MReliableBroadcast>>();
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
		// #0 (TODO) remove n from safe links
		// #1 get the peer sampling service
		IRoutingService irs = null; // (TODO) init correctly
		// #2 send alpha
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
		
	}

	/**
	 * Just received a locked message. Must acknowledge it.
	 * 
	 * @param from
	 *            The node that sent the locked message.
	 * @param to
	 *            The node that must acknowledge the locked message.
	 */
	private void receiveLocked(Node from, Node to) {
		MUnlockBroadcast mu = new MUnlockBroadcast(from, to);
		Transport t = ((Transport) this.node
				.getProtocol(FastConfig.getTransport(PreventiveReliableCausalBroadcast.pid)));

		APeerSampling ps = (APeerSampling) this.node
				.getProtocol(FastConfig.getLinkable(PreventiveReliableCausalBroadcast.pid));
		List<Node> neighborhood = IteratorUtils.toList(ps.getAliveNeighbors().iterator());

		if (neighborhood.contains(from)) {
			t.send(this.node, from, mu, PreventiveReliableCausalBroadcast.pid);
		} else {
			// just to check if it cannot send message because it has no
			PreventiveReliableCausalBroadcast fcb = (PreventiveReliableCausalBroadcast) from
					.getProtocol(PreventiveReliableCausalBroadcast.pid);
			if (fcb.buffers.containsKey(to)) {
				System.out.println("NOT COOL");
			}
		}
	}

	/**
	 * Just received an acknowledged message. Must empty the corresponding
	 * buffer etc.
	 * 
	 * @param from
	 *            We are the origin.
	 * @param to
	 *            The node that acknowledged our locked message.
	 */
	private void receiveAck(Node from, Node to) {
		if (this.buffers.containsKey(to)) {
			Transport t = ((Transport) this.node
					.getProtocol(FastConfig.getTransport(PreventiveReliableCausalBroadcast.pid)));
			// #1 empty the buffer
			for (int i = 0; i < this.buffers.get(to).size(); ++i) {
				t.send(this.node, to, this.buffers.get(to).get(i), PreventiveReliableCausalBroadcast.pid);
			}
			// #2 remove the entry from the buffer
			this.buffers.remove(to);
		}
	}

	/**
	 * A peer is removed from neighborhoods. Clean buffers if need be.
	 * 
	 * @param n
	 *            The removed neighbor.
	 * 
	 */
	public void closed(Node n) {
		this.buffers.remove(n);
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
		++this.counter;
		MReliableBroadcast mrb = new MReliableBroadcast(this.node.getID(), this.counter,
				new MRegularBroadcast(message));
		this.received.add(mrb.id, mrb.counter);
		this._sendToAllNeighbors(mrb);
		this.rDeliver(mrb);
	}

	/**
	 * Deliver exactly once. In this class, messages arrive causally ready, no
	 * need to check.
	 * 
	 * @param m
	 *            The message delivered.
	 */
	public void rDeliver(MReliableBroadcast m) {
		// #1 buffers
		for (Node neigbhor : this.buffers.keySet()) {
			this.buffers.get(neigbhor).add(m);
		}
		// #2 deliver
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

	public void processEvent(Node node, int protocolId, Object message) {
		this._setNode(node);

		if (message instanceof MRegularBroadcast) {
			MReliableBroadcast mrb = (MReliableBroadcast) message;
			if (!this.received.contains(mrb.id, mrb.counter)) {
				this.received.add(mrb.id, mrb.counter);
				this._sendToAllNeighbors(mrb); // forward
				this.rDeliver(mrb);
			}
		} else if (message instanceof MOpen) {
			MOpen mo = (MOpen) message;
			this.opened(mo.to, mo.mediator);
		} else if (message instanceof MClose) {
			MClose mc = (MClose) message;
			this.closed(mc.to);
		} else if (message instanceof MForward) {
			MForward mf = (MForward) message;
			this.onForward(mf.to, mf.getPayload());
		} else if (message instanceof MLockedBroadcast) {
			MLockedBroadcast mlb = (MLockedBroadcast) message;
			this.receiveLocked(mlb.from, mlb.to);
		} else if (message instanceof MUnlockBroadcast) {
			MUnlockBroadcast mu = (MUnlockBroadcast) message;
			this.receiveAck(mu.from, mu.to);
		}
	}

	public void nextCycle(Node node, int protocolId) {
		this._setNode(node);

		if (CommonState.r.nextDouble() < PreventiveReliableCausalBroadcast.pmessage) {
			// (TODO)
		}
	}

	/**
	 * Send a locked message to a remote peer that we want to add in our
	 * neighborhood.
	 * 
	 * @param to
	 *            The peer to reach.
	 */
	private void _sendLocked(Node to, Node mediator) {
		MLockedBroadcast mlb = new MLockedBroadcast(this.node, to);

		Transport t = ((Transport) this.node
				.getProtocol(FastConfig.getTransport(PreventiveReliableCausalBroadcast.pid)));
		t.send(this.node, mediator, new MForward(to, mlb), PreventiveReliableCausalBroadcast.pid);
	}

	/**
	 * Forward a message to a remote peer.
	 * 
	 * @param to
	 *            The peer to forward the message to.
	 * @param message
	 *            The message to forward.
	 */
	private void onForward(Node to, IMessage message) {
		((Transport) this.node.getProtocol(FastConfig.getTransport(PreventiveReliableCausalBroadcast.pid)))
				.send(this.node, to, message, PreventiveReliableCausalBroadcast.pid);
	}

	/**
	 * Send the reliable broadcast message to all neighbors, excepts ones still
	 * buffering phase.
	 * 
	 * @param m
	 *            The message to send.
	 */
	protected void _sendToAllNeighbors(MReliableBroadcast m) {
		APeerSampling ps = (APeerSampling) this.node
				.getProtocol(FastConfig.getLinkable(PreventiveReliableCausalBroadcast.pid));

		for (Node q : ps.getAliveNeighbors()) {
			// (XXX) maybe remove q from peer-sampling, cause it may be
			// scrambled too quick.
			// Or maybe put
			// EDProtocol even for cycles. So all scrambles do not happen at a
			// same time.
			if (!this.buffers.containsKey(q)) {
				((Transport) this.node.getProtocol(FastConfig.getTransport(PreventiveReliableCausalBroadcast.pid)))
						.send(this.node, q, m, PreventiveReliableCausalBroadcast.pid);
			}
		}
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
