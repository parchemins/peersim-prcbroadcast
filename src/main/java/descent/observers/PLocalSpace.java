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
import peersim.core.Node;
import peersim.transport.Transport;

public class PLocalSpace implements IObserverProgram {

	public PLocalSpace() {

	}

	public void tick(long currentTick, DictGraph observer) {
		ArrayList<Double> outview = new ArrayList<Double>();
		ArrayList<Double> inview = new ArrayList<Double>();

		ArrayList<Double> unsafe = new ArrayList<Double>();
		ArrayList<Double> safe = new ArrayList<Double>();

		ArrayList<Double> routes = new ArrayList<Double>();

		for (Node n : CDynamicNetwork.networks.get(0)) {
			PRCBcast prcb = ((WholePRCcast) n.getProtocol(WholePRCcast.PID)).prcb;
			SprayWithRouting swr = ((WholePRCcast) n.getProtocol(WholePRCcast.PID)).swr;

			outview.add((double) swr.outview.partialView.size());
			inview.add((double) swr.inview.size());

			unsafe.add((double) prcb.unsafe.size());
			safe.add((double) prcb.safe.size());

			routes.add((double) swr.routes.inUse().size());
		}

		Node theOne = CDynamicNetwork.networks.get(0).get(0);

		Long latency = ((Transport) theOne.getProtocol(FastConfig.getTransport(WholePRCcast.PID))).getLatency(null,
				null);

		Stats sOutview = Stats.getFromSmall(outview);
		Stats sInview = Stats.getFromSmall(inview);

		Stats sUnsafe = Stats.getFromSmall(unsafe);
		Stats sRoutes = Stats.getFromSmall(routes);
		Stats sSafe = Stats.getFromSmall(safe);
		System.out.println("PLS. " + latency + " (" + sOutview.mean + "; " + sInview.mean + ") (" + sUnsafe.mean + "  "
				+ sSafe.mean + ")  " + sRoutes.mean);
	}

	public void onLastTick(DictGraph observer) {
	}

}
