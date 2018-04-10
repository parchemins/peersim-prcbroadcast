package descent.observers;

import java.util.ArrayList;

import descent.causalbroadcast.PRCBcast;
import descent.causalbroadcast.WholePRCcast;
import descent.causalbroadcast.routingbispray.SprayWithRouting;
import descent.controllers.CDynamicNetwork;
import descent.observers.structure.DictGraph;
import descent.observers.structure.IObserverProgram;
import descent.observers.structure.Stats;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.transport.Transport;

public class PLocalSpace implements IObserverProgram {

	public PLocalSpace() {

	}

	public void tick(long currentTick, DictGraph observer) {
		ArrayList<Double> expectedMessages = new ArrayList<Double>();
		ArrayList<Double> buffersMessages = new ArrayList<Double>();
		ArrayList<Double> sizeVector = new ArrayList<Double>();

		for (Node n : CDynamicNetwork.networks.get(0)) {
			PRCBcast prcb = ((WholePRCcast) n.getProtocol(WholePRCcast.PID)).prcb;

			Integer sumExpectedMessages = 0;
			for (Node m : prcb.expected.keySet()) {
				sumExpectedMessages += prcb.expected.get(m).size();
			}

			Integer sumBuffersMessages = 0;
			for (Node m : prcb.buffersAlpha.keySet()) {
				sumBuffersMessages += prcb.buffersAlpha.get(m).size();
				sumBuffersMessages += prcb.buffersPi.get(m).size();
			}

			if (PRCBcast.VECTOR_CLOCK_CHECK) {
				sizeVector.add((double) prcb.vectorClock.size());
			}

			expectedMessages.add((double) sumExpectedMessages);
			buffersMessages.add((double) sumBuffersMessages);
		}

		Stats sExpectedMessages = Stats.getFromSmall(expectedMessages);
		Stats sBuffersMessages = Stats.getFromSmall(buffersMessages);
		Stats sVectorClock = Stats.getFromSmall(sizeVector);

		System.out.println("PLS. " + CommonState.getTime() + " " + sExpectedMessages.mean + " " + sBuffersMessages.mean
				+ " " + sVectorClock.mean);
	}

	public void onLastTick(DictGraph observer) {
	}

}
