package descent.observers;

import java.util.ArrayList;

import descent.causalbroadcast.WholePRCcast;
import descent.controllers.CDynamicNetwork;
import descent.observers.structure.DictGraph;
import descent.observers.structure.IObserverProgram;
import descent.observers.structure.Stats;
import peersim.core.CommonState;
import peersim.core.Node;

public class PMessages implements IObserverProgram {

	public PMessages() {

	}

	public void tick(long currentTick, DictGraph observer) {
		ArrayList<Double> controlMessages = new ArrayList<Double>();

		for (Node n : CDynamicNetwork.networks.get(0)) {
			WholePRCcast wprc = (WholePRCcast) n.getProtocol(WholePRCcast.PID);

			controlMessages.add((double) wprc.swr.getNumberOfControlMessagesSentSinceLastCheck());
		}

		Stats sControlMessages = Stats.getFromSmall(controlMessages);

		System.out.println("PM.  " + CommonState.getTime() + " " + sControlMessages.toString());

	}

	public void onLastTick(DictGraph observer) {
		// TODO Auto-generated method stub

	}

}
