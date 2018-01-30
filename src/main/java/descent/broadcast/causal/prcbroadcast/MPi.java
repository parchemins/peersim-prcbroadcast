package descent.broadcast.causal.prcbroadcast;

import peersim.core.Node;

/**
 * When Process A adds a link to Process B, this is the third control message
 * that must reach Process B. On receipt, Process B stops the first buffer and
 * starts another one.
 */
public class MPi {

	public final Node from;
	public final Node to;

	public MPi(Node from, Node to) {
		this.from = from;
		this.to = to;
	}
}
