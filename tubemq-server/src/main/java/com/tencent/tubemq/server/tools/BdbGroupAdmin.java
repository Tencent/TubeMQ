/*
 * Tencent is pleased to support the open source community by making TubeMQ available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.tubemq.server.tools;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 *  Utility class for script #{bin/groupAdmin.sh} to remove node from BDB-JE replication group.
 *  Note the following rules when using.
 *
 *  1. For best results, shutdown the node before removing it.
 *
 *  2. You use the node's name (not the host/port pair) to identify the node you want to remove from the group.
 *     If the node name that you specify is unknown to the replication group, a MemberNotFoundException is thrown.
 *     If it names a secondary node, an IllegalArgumentException is thrown.
 *
 *  3. Once removed, the electable node can no longer connect to the Master, nor can it  participate in elections.
 *     If you want to reconnect the node to the Master (that is, you want to add it back  to the replication group),
 *     you will have to do so using a different node name than the node was using when it was removed from the group.
 *
 *  4. An active Master cannot be removed from the group. To remove the active Master, either shut it down or wait
 *     until it transitions to the Replica state. If you attempt to remove an active Master, a MasterStateException
 *     is thrown.
 */

public class BdbGroupAdmin {

    public static void main(final String[] args) throws Exception {

        if (args == null || args.length != 3) {
            System.out.println("Parameter error. Need 3 params : \n" +
                    "1) replication group name,\n" +
                    "2) replication group helperHost(ip:port),\n" +
                    "3) node name in replication group to be removed.");
            return;
        }

        Set<InetSocketAddress> helpers = new HashSet<InetSocketAddress>();
        String group = args[0];
        String[] hostAndPort = args[1].split(":");
        if (hostAndPort.length != 2) {
            System.out.println("replication group helperHost(ip:port) format error.");
            return;
        }
        String nodeName2Remove = args[2];
        InetSocketAddress helper =
                new InetSocketAddress(hostAndPort[0], Integer.valueOf(hostAndPort[1]));
        helpers.add(helper);
        ReplicationGroupAdmin rga = new ReplicationGroupAdmin(group, helpers);

        try {
            //print node information before remove
            Set<String> nodeNames = new HashSet<>();
            for (ReplicationNode repNode  : rga.getGroup().getNodes()) {
                nodeNames.add(repNode.getName());
            }
            System.out.println("Before remove, " + group +
                    " has " + nodeNames.size() + " nodes :" + nodeNames);

            //do remove node
            rga.removeMember(nodeName2Remove);
            System.out.print("Remove " + nodeName2Remove + "successfully.");

            //print node information after remove
            nodeNames.clear();
            for (ReplicationNode repNode  : rga.getGroup().getNodes()) {
                nodeNames.add(repNode.getName());
            }
            System.out.println("After remove, " + group +
                    " has " + nodeNames.size() + " nodes :" + nodeNames);

        } catch (UnknownMasterException e) {
            System.out.println("Can't find active master node.");
        } catch (MemberNotFoundException e) {
            System.out.println("Node name to remove is unknown to the replication group," +
                    " please use the correct node name.");
        } catch (MasterStateException e) {
            System.out.println("Master node is not allowed to remove. To remove the active Master, " +
                    "either shut it down or wait until it transitions to the Replica state.");
        } catch (EnvironmentFailureException e) {
            System.out.println("Replication group environment failed.");
        }
    }

}
