package io.github.retz.scheduler;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;


public class RetzLeaderSelectorListener extends LeaderSelectorListenerAdapter {

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
	CuratorClient.runForMaster();
    }
}
