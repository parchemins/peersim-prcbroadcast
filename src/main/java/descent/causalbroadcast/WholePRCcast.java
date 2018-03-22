package descent.causalbroadcast;

import descent.causalbroadcast.messages.IMControlMessage;
import descent.causalbroadcast.messages.MAlpha;
import descent.causalbroadcast.messages.MBeta;
import descent.causalbroadcast.messages.MBuffer;
import descent.causalbroadcast.messages.MPi;
import descent.causalbroadcast.messages.MRho;
import descent.causalbroadcast.routingbispray.MConnectTo;
import descent.causalbroadcast.routingbispray.MExchangeWith;
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

	public PRCBcast prcb;
	public SprayWithRouting swr;

	public WholePRCcast(String prefix) {
		WholePRCcast.PID = Configuration.getPid(prefix + "." + PAR_PID);

		this.prcb = new PRCBcast();
		this.swr = new SprayWithRouting(this.prcb);
		this.prcb.setIRS(this.swr);
	}

	public WholePRCcast() {
		this.prcb = new PRCBcast();
		this.swr = new SprayWithRouting(this.prcb);
		this.prcb.setIRS(this.swr);
	}

	public void nextCycle(Node node, int protocolId) {
		this.swr.periodicCall();
	}

	public void processEvent(Node node, int protocolId, Object message) {
		this.prcb._setNode(node);

		// Give the message to the proper sub-protocol
		if (message instanceof MExchangeWith) {
			MExchangeWith m = (MExchangeWith) message;
			assert (m.from == this.prcb.node);
			this.swr.receiveMExchangeWith(m.to, m);

		} else if (message instanceof MConnectTo) {
			MConnectTo m = (MConnectTo) message;
			assert (m.from == this.prcb.node);
			this.swr.receiveMConnectTo(m.from, m.mediator, m.to);

		} else if (message instanceof IMControlMessage) {
			IMControlMessage imcm = (IMControlMessage) message;

			if (imcm.getReceiver() == this.prcb.node) {
				// receiver
				if (message instanceof MAlpha) {
					MAlpha m = (MAlpha) message;
					this.swr.receiveMConnectFrom(m.from, m.mediator, m.to);
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
				IMControlMessage m = (IMControlMessage) message;
				// refresh the path (XXX) why is that needed
				this.swr.addRoute(m.getFrom(), this.prcb.node, m.getTo());

				if (message instanceof MAlpha) {
					this.swr.sendAlpha(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MBeta) {
					this.swr.sendBeta(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MPi) {
					this.swr.sendPi(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MRho) {
					this.swr.sendRho(imcm.getFrom(), imcm.getTo());
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
