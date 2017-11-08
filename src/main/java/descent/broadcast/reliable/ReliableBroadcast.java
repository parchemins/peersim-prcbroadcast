package descent.broadcast.reliable;

import descent.rps.APeerSampling;
import descent.rps.IMessage;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

/**
 * Broadcast protocol that reliably delivers a message to all network members
 * exactly once.
 */
public class ReliableBroadcast implements EDProtocol, CDProtocol {

	private static String PAR_PID = "pid";
	private static int PID;

	private VectorClock received;

	private Integer counter = 0;
	private Node node;

	public ReliableBroadcast(String prefix) {
		this.PID = Configuration.getPid(prefix + "." + ReliableBroadcast.PAR_PID);
		// this.Q = Configuration.getPid(prefix + "." + ReliableBroadcast.PAR_Q);
		this.received = new VectorClock();
	}

	public ReliableBroadcast() {
		this.received = new VectorClock();
	}

	/**
	 * Broadcast a message to all peers of the network.
	 * 
	 * @param m
	 *            The message to send.
	 */
	public void broadcast(IMessage m) { // b_p(m)
		++this.counter;
		MReliableBroadcast mrb = new MReliableBroadcast(this.node.getID(), this.counter, m);
		this.received.add(mrb.id, mrb.counter);
		this._sendToAllNeighbors(mrb);
		this.rDeliver(mrb.message);
	}

	/**
	 * Should deliver the message to all applications depending on this.
	 * 
	 * @param m
	 *            The delivered message.
	 */
	public void rDeliver(IMessage m) {
		// nothing (TODO)?
	}

	public void processEvent(Node node, int protocolId, Object message) {
		this._setNode(node);

		if (message instanceof MReliableBroadcast) { // r_p(m)
			MReliableBroadcast mrb = (MReliableBroadcast) message;
			if (!this.received.contains(mrb.id, mrb.counter)) {
				this.received.add(mrb.id, mrb.counter);
				this._sendToAllNeighbors(mrb);
				this.rDeliver(mrb.message);
			}
		}
	}

	/**
	 * Send the reliable broadcast message to all neighbors.
	 * 
	 * @param m
	 *            The message to send.
	 */
	private void _sendToAllNeighbors(MReliableBroadcast m) {
		APeerSampling ps = (APeerSampling) this.node.getProtocol(FastConfig.getLinkable(ReliableBroadcast.PID));

		for (Node q : ps.getAliveNeighbors()) {
			((Transport) node.getProtocol(FastConfig.getTransport(ReliableBroadcast.PID))).send(this.node, q, m,
					ReliableBroadcast.PID);

		}
	}

	@Override
	public Object clone() {
		return new ReliableBroadcast();
	}

	public void nextCycle(Node node, int protocolId) {
		this._setNode(node);
	}

	private void _setNode(Node n) {
		if (this.node == null) {
			this.node = n;
		}
	}

}