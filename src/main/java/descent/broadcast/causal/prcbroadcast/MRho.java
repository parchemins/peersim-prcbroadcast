package descent.broadcast.causal.prcbroadcast;

import peersim.core.Node;

/**
 * When Process A adds a link to Process B, this is the fourth control message.
 * On receipt, Process A can safely send its buffer to Process B and start using
 * the new link for causal broadcast.	
 */
public class MRho {

	public final Node from;
	public final Node to;

	public MRho(Node from, Node to) {
		this.from = from;
		this.to = to;
	}
}
