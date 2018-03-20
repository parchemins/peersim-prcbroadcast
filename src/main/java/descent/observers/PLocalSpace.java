package descent.observers;

import java.util.ArrayList;

import descent.causalbroadcast.PRCBcast;
import descent.causalbroadcast.WholePRCcast;
import descent.causalbroadcast.routingbispray.SprayWithRouting;
import descent.controllers.CDynamicNetwork;
import descent.observers.structure.DictGraph;
import descent.observers.structure.IObserverProgram;
import descent.observers.structure.Stats;
import peersim.config.Configuration;
import peersim.core.Node;

public class PLocalSpace implements IObserverProgram {

	public PLocalSpace() {

	}

	public void tick(long currentTick, DictGraph observer) {
		ArrayList<Double> unsafe = new ArrayList<Double>();
		ArrayList<Double> safe  = new ArrayList<Double>();

		ArrayList<Double> routes = new ArrayList<Double>();

		for (Node n : CDynamicNetwork.networks.get(0)) {
			PRCBcast prcb = ((WholePRCcast) n.getProtocol(WholePRCcast.PID)).prcb;
			SprayWithRouting swr = ((WholePRCcast) n.getProtocol(WholePRCcast.PID)).swr;

			unsafe.add((double) prcb.unsafe.size());
			safe.add((double) prcb.safe.size());
			
			routes.add((double) swr.routes.inUse().size());
		}

		Stats sUnsafe = Stats.getFromSmall(unsafe);
		Stats sRoutes = Stats.getFromSmall(routes);
		Stats sSafe = Stats.getFromSmall(safe);
		System.out.println("PLS. " + sUnsafe.mean + "  "+ sSafe.mean + "  " + sRoutes.mean);
	}

	public void onLastTick(DictGraph observer) {
	}

}
