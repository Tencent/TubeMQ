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
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;


public class BdbGroupAdmin {

    public static void main(final String[] args) throws Exception {

        if (args == null || args.length != 3) {
            System.out.println("please input 3 params , groupName helperHost(ip:port) nodeName2Remove..");
            return;
        }
        Set<InetSocketAddress> helpers = new HashSet<InetSocketAddress>();
        String group = args[0];
        String[] hostAndPort = args[1].split(":");
        if (hostAndPort.length != 2) {
            System.out.println("helperHost(ip:port) format error..");
            return;
        }
        String nodeName2Remove = args[2];
        InetSocketAddress helper =
                new InetSocketAddress(hostAndPort[0], Integer.valueOf(hostAndPort[1]));
        helpers.add(helper);
        ReplicationGroupAdmin rga = new ReplicationGroupAdmin(group, helpers);
        try {
            rga.removeMember(nodeName2Remove);
            System.out.print("Remove " + nodeName2Remove + "successfully...");
        } catch (UnknownMasterException e) {
            e.printStackTrace();
        } catch (MemberNotFoundException e) {
            e.printStackTrace();
        } catch (MasterStateException e) {
            e.printStackTrace();
        } catch (EnvironmentFailureException e) {
            e.printStackTrace();
        }
    }

}
