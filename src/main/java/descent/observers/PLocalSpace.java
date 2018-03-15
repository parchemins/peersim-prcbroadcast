package descent.observers;

import java.util.ArrayList;

import descent.causalbroadcast.PreventiveReliableCausalBroadcast;
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

		ArrayList<Double> routes = new ArrayList<Double>();
		ArrayList<Double> inUse = new ArrayList<Double>();

		for (Node n : CDynamicNetwork.networks.get(0)) {
			PreventiveReliableCausalBroadcast prcb = ((WholePRCcast) n.getProtocol(WholePRCcast.PID)).prcb;
			SprayWithRouting swr = ((WholePRCcast) n.getProtocol(WholePRCcast.PID)).swr;

			unsafe.add((double) prcb.unsafe.size());
			routes.add((double) swr.routes.toVia.size());
			inUse.add((double) swr.inUse.size());
		}

		Stats sUnsafe = Stats.getFromSmall(unsafe);
		Stats sRoutes = Stats.getFromSmall(routes);
		Stats sInUse = Stats.getFromSmall(inUse);

		System.out.println("PLS. " + sUnsafe.mean + " " + sRoutes.mean + " " + sInUse.mean);
	}

	public void onLastTick(DictGraph observer) {
	}

}
