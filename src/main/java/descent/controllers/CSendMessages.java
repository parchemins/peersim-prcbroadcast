package descent.controllers;

import descent.causalbroadcast.WholePRCcast;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.util.ExtendedRandom;

public class CSendMessages implements Control {

	public static String PAR_MESSAGES = "messages";
	public static Integer MESSAGES;

	private static ExtendedRandom erng;

	public CSendMessages(String prefix) {
		CSendMessages.MESSAGES = Configuration.getInt(prefix + "." + CSendMessages.PAR_MESSAGES, 0);
		CSendMessages.erng = new ExtendedRandom(CommonState.r.getLastSeed());
	}

	public boolean execute() {
		for (int i = 0; i < CSendMessages.MESSAGES; ++i) {
			Integer index = CSendMessages.erng.nextInt(CDynamicNetwork.networks.get(0).size());
			WholePRCcast wprc = (WholePRCcast) CDynamicNetwork.networks.get(0).get(index).getProtocol(WholePRCcast.PID);
			// System.out.println();
			//System.out.println("@"+wprc.swr.node.getID() + " INIT BROADCAST.");
			wprc.prcb.cbroadcast(null);
		}

		return false;
	}

}
