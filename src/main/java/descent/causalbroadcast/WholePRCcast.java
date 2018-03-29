package descent.causalbroadcast;

import descent.causalbroadcast.messages.IMControlMessage;
import descent.causalbroadcast.messages.MAlpha;
import descent.causalbroadcast.messages.MBeta;
import descent.causalbroadcast.messages.MBuffer;
import descent.causalbroadcast.messages.MPi;
import descent.causalbroadcast.messages.MReliableBroadcast;
import descent.causalbroadcast.messages.MRho;
import descent.causalbroadcast.routingbispray.MConnectTo;
import descent.causalbroadcast.routingbispray.MExchangeWith;
import descent.causalbroadcast.routingbispray.SprayWithRouting;
import descent.controllers.IComposition;
import descent.rps.APeerSampling;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.edsim.EDProtocol;

// Whole protocol at once because peersim does not handle inheritance very well.
// In particular, static values such as pid are not consistent.
public class WholePRCcast implements IComposition, EDProtocol, CDProtocol {

	public static String PAR_PID = "pid";
	public static Integer PID;

	public static String PAR_STOP = "stop";
	public static Integer STOP;

	public PRCBcast prcb;
	public SprayWithRouting swr;

	////////////////////////////////////////////////////////////////////////////

	public WholePRCcast(String prefix) {
		WholePRCcast.PID = Configuration.getPid(prefix + "." + WholePRCcast.PAR_PID);
		WholePRCcast.STOP = Configuration.getInt(prefix + "." + WholePRCcast.PAR_STOP);

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
		if (CommonState.getIntTime() < WholePRCcast.STOP)
			this.swr.periodicCall();
		this.swr.routes.setNode(node);
		this.swr.routes.upKeep();
	}

	public void processEvent(Node node, int protocolId, Object message) {
		this.swr.routes.upKeep();

		// Give the message to the proper sub-protocol
		if (message instanceof MReliableBroadcast) {
			// must check because message sent still travel
			MReliableBroadcast m = (MReliableBroadcast) message;
			if (this.prcb.canReceive(m.sender)) {
				//System.out.println(
				//		"@" + this.prcb.node.getID() + " <--       " + m.sender.getID() + " " + m.toString());
				this.prcb.receive(m, m.sender);
			} else {
				// System.out.println();
				// System.out.println("TO DOUBLE CHECK @" + this.prcb.node.getID() + " FROM " + m.sender.getID()
				//		+ " MESSAGE " + m.toString());
			}

		} else if (message instanceof MExchangeWith) {
			MExchangeWith m = (MExchangeWith) message;
			this.swr.addRoute(m.from, null, m.to);
			this.swr.receiveMExchangeWith(m);

		} else if (message instanceof MConnectTo) {
			MConnectTo m = (MConnectTo) message;
			this.swr.addRoute(m.from, m.mediator, m.to);
			this.swr.receiveMConnectTo(m);

		} else if (message instanceof IMControlMessage) {
			IMControlMessage imcm = (IMControlMessage) message;

			if (imcm.getReceiver() == this.prcb.node) {
				// receiver
				if (message instanceof MAlpha) {
					MAlpha m = (MAlpha) message;
					this.swr.addRoute(m.from, m.mediator, m.to);
					this.prcb.receiveAlpha(m);
				} else if (message instanceof MBeta) {
					MBeta m = (MBeta) message;
					this.prcb.receiveBeta(m);
				} else if (message instanceof MPi) {
					MPi m = (MPi) message;
					this.prcb.receivePi(m);
				} else if (message instanceof MRho) {
					MRho m = (MRho) message;
					this.prcb.receiveRho(m);
				} else if (message instanceof MBuffer) {
					// System.out.println("@" + this.prcb.node.getID() + " RCV BUFF " + imcm.getSender().getID());
					this.prcb.receiveBuffer(imcm.getFrom(), imcm.getTo(), ((MBuffer) message).messages);
				}

			} else {
				// mediator forwards
				IMControlMessage m = (IMControlMessage) message;
				// refreshing route is not necessary
				// this.swr.addRoute(m.getFrom(), this.prcb.node, m.getTo());
				this.swr._sendControlMessage(m.getReceiver(), m);
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
