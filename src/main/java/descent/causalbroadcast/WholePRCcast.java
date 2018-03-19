package descent.causalbroadcast;

import descent.causalbroadcast.messages.IMControlMessage;
import descent.causalbroadcast.messages.MAlpha;
import descent.causalbroadcast.messages.MBeta;
import descent.causalbroadcast.messages.MBuffer;
import descent.causalbroadcast.messages.MPi;
import descent.causalbroadcast.messages.MRho;
import descent.causalbroadcast.routingbispray.MConnectTo;
import descent.causalbroadcast.routingbispray.MRemoveRoute;
import descent.causalbroadcast.routingbispray.SprayWithRouting;
import descent.controllers.IComposition;
import descent.rps.APeerSampling;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.core.Node;
import peersim.edsim.EDProtocol;

// Whole protocol at once because peersim does not handle inheritance very well.
// In particular, static values such as pid are not consistent.
public class WholePRCcast implements IComposition, EDProtocol, CDProtocol {

	public static String PAR_PID = "pid";
	public static Integer PID;

	public PreventiveReliableCausalBroadcast prcb;
	public SprayWithRouting swr;

	public WholePRCcast(String prefix) {
		WholePRCcast.PID = Configuration.getPid(prefix + "." + PAR_PID);

		this.prcb = new PreventiveReliableCausalBroadcast(prefix);
		this.swr = new SprayWithRouting(this.prcb);
		this.prcb.setIRS(this.swr);
	}

	public WholePRCcast() {
		this.prcb = new PreventiveReliableCausalBroadcast();
		this.swr = new SprayWithRouting(this.prcb);
		this.prcb.setIRS(this.swr);
	}

	public void nextCycle(Node node, int protocolId) {
		this.swr.periodicCall();
	}

	public void processEvent(Node node, int protocolId, Object message) {
		this.prcb._setNode(node);

		// Give the message to the proper sub-protocol
		if (message instanceof MConnectTo) {
			MConnectTo m = (MConnectTo) message;
			System.out.println("this . from " + m.from.getID() + "  + +  this" + this.prcb.node.getID());
			assert (m.from == this.prcb.node);

			if (m.mediator == null) {
				// direct safe link already exist and just need to be inverted
				assert (this.swr.isSafe(m.to));
				this.swr.outview.addNeighbor(m.to);

				SprayWithRouting other = ((WholePRCcast) node.getProtocol(WholePRCcast.PID)).swr;
				if (!other.outview.contains(this.swr.node)) {
					this.swr.inview.remove(m.to);
				}
			} else {
				this.swr.addRoute(m.mediator, m.to);
				assert (this.prcb.openO(m.to)); // (TODO) send MRemoveRoute if
												// already exists
			}

		} else if (message instanceof MRemoveRoute) {
			MRemoveRoute m = (MRemoveRoute) message;
			assert (m.mediator == this.prcb.node);
			this.swr.removeRouteAsMediator(m.from, m.to);

		} else if (message instanceof IMControlMessage) {
			IMControlMessage imcm = (IMControlMessage) message;

			if (imcm.getReceiver() == this.prcb.node) {
				// receiver
				if (message instanceof MAlpha) {
					MAlpha m = (MAlpha) message;
					this.swr.addRoute(m.mediator, m.from);
					this.prcb.receiveAlpha(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MBeta) {
					this.prcb.receiveBeta(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MPi) {
					this.prcb.receivePi(imcm.getFrom(), imcm.getTo());
					this.swr.removeRouteAsEndProcess(this.swr.routes.getRoute(imcm.getFrom()), imcm.getFrom());
				} else if (message instanceof MRho) {
					this.prcb.receiveRho(imcm.getFrom(), imcm.getTo());
					this.swr.removeRouteAsEndProcess(this.swr.routes.getRoute(imcm.getTo()), imcm.getTo());
				} else if (message instanceof MBuffer) {
					this.prcb.receiveBuffer(imcm.getFrom(), imcm.getTo(), ((MBuffer) message).messages);
				}

			} else {
				// mediator
				if (message instanceof MAlpha) {
					this.swr.sendAlpha(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MBeta) {
					this.swr.sendBeta(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MPi) {
					this.swr.sendPi(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MRho) {
					this.swr.sendRho(imcm.getFrom(), imcm.getTo());
					this.swr.removeRouteAsMediator(imcm.getFrom(), imcm.getTo());
				}
			}

		}
	}

	@Override
	public Object clone() {
		// TODO maybe clone some configurations
		return new WholePRCcast();
	}

	public APeerSampling getPeerSampling() {
		return this.swr;
	}

}
