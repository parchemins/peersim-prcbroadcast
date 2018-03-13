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
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.core.Node;
import peersim.edsim.EDProtocol;

// Whole protocol at once because peersim does not handle inheritance very well.
// In particular, static values such as pid are not consistent.
public class WholePRCcast implements EDProtocol, CDProtocol {

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
		// Give the message to the proper sub-protocol
		if (message instanceof MConnectTo) {
			MConnectTo m = (MConnectTo) message;
			assert (m.from == this.prcb.node);
			this.swr.addRoute(m.mediator, m.to);
			this.prcb.openO(m.to);

		} else if (message instanceof MRemoveRoute) {
			MRemoveRoute m = (MRemoveRoute) message;
			assert (m.mediator == this.prcb.node);
			this.swr.removeRouteAsMediator(m.from, m.to);

		} else if (message instanceof IMControlMessage) {
			IMControlMessage imcm = (IMControlMessage) message;
			if (imcm.getReceiver() == this.prcb.node) {
				// receiver
				if (message instanceof MAlpha) {
					this.prcb.receiveAlpha(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MBeta) {
					this.prcb.receiveBeta(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MPi) {
					this.prcb.receivePi(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MRho) {
					this.prcb.receiveRho(imcm.getFrom(), imcm.getTo());
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
					this.swr.removeRouteAsMediator(imcm.getFrom(), imcm.getTo());
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

}
