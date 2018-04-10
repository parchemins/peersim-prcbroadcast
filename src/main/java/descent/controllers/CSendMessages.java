package descent.controllers;

import descent.causalbroadcast.WholePRCcast;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.util.ExtendedRandom;

public class CSendMessages implements Control {

	public static String PAR_MESSAGES = "messages";
	public static Integer MESSAGES;

	public static String PAR_STOP = "stop";
	public static long STOP;

	private static ExtendedRandom erng;

	public CSendMessages(String prefix) {
		CSendMessages.MESSAGES = Configuration.getInt(prefix + "." + CSendMessages.PAR_MESSAGES, 0);
		CSendMessages.erng = new ExtendedRandom(CommonState.r.getLastSeed());

		CSendMessages.STOP = Configuration.getLong(prefix + "." + CSendMessages.PAR_STOP, 0);
	}

	public boolean execute() {
		if (CommonState.getTime() > CSendMessages.STOP){
			return false;
		}

		for (int i = 0; i < CSendMessages.MESSAGES; ++i) {
			Integer index = CSendMessages.erng.nextInt(CDynamicNetwork.networks.get(0).size());
			WholePRCcast wprc = (WholePRCcast) CDynamicNetwork.networks.get(0).get(index).getProtocol(WholePRCcast.PID);
			wprc.prcb.cbroadcast(null);
		}

		/*
		 * for (int i = 0; i < CSendMessages.MESSAGES; ++i) { Integer index =
		 * 100; if (CDynamicNetwork.networks.get(0).size() > index &&
		 * CDynamicNetwork.networks.get(0).get(index).isUp()) { WholePRCcast
		 * wprc = (WholePRCcast) CDynamicNetwork.networks.get(0).get(index)
		 * .getProtocol(WholePRCcast.PID); wprc.prcb.cbroadcast(null); } }
		 */

		return false;
	}

}
