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

package com.tencent.tubemq.corebase.cluster;

import com.tencent.tubemq.corebase.TokenConstants;
import com.tencent.tubemq.corebase.utils.TStringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MasterInfo {

    private final Map<String/** ip:port */, NodeAddrInfo> addrMap4Failover =
            new HashMap<String, NodeAddrInfo>();
    private List<String> nodeHostPortList;
    private NodeAddrInfo firstNodeAddr = null;
    private String masterClusterStr;

    /**
     * masterAddrInfo: "ip1:port,ip2:port"
     */
    public MasterInfo(final String masterAddrInfo) throws Exception {
        if (TStringUtils.isBlank(masterAddrInfo)) {
            throw new Exception("Illegal parameter: masterAddrInfo is Blank!");
        }
        if (!masterAddrInfo.contains(TokenConstants.ATTR_SEP)) {
            throw new Exception(
                    "Illegal parameter: masterAddrInfo's value must like \"ip1:port,ip2:port\"!");
        }
        String[] hostAndPortArray =
                masterAddrInfo.split(TokenConstants.ARRAY_SEP);
        for (String addr : hostAndPortArray) {
            if (TStringUtils.isBlank(addr)) {
                throw new Exception(
                        "Illegal parameter: masterAddrInfo's value must "
                                + "like \"ip1:port,ip2:port\" and ip:port not Blank!");
            }
            String[] hostPortItem = addr.split(TokenConstants.ATTR_SEP);
            if (hostPortItem.length != 2) {
                throw new Exception(
                        "Illegal parameter: masterAddrInfo's value must like \"ip1:port,ip2:port\"!");
            }
            String hostName = hostPortItem[0].trim();
            if (TStringUtils.isBlank(hostName)) {
                throw new Exception(
                        "Illegal parameter: masterAddrInfo's value must "
                                + "like \"ip1:port,ip2:port\" and ip's value not Blank!");
            }
            if (TStringUtils.isBlank(hostPortItem[1])) {
                throw new Exception(
                        "Illegal parameter: masterAddrInfo's value must"
                                + " like \"ip1:port,ip2:port\" and port's value not Blank!");
            }
            int port = Integer.parseInt(hostPortItem[1].trim());
            NodeAddrInfo tmpNodeAddrInfo = new NodeAddrInfo(hostName, port);
            if (addrMap4Failover.get(tmpNodeAddrInfo.getHostPortStr()) == null) {
                addrMap4Failover.put(tmpNodeAddrInfo.getHostPortStr(), tmpNodeAddrInfo);
            }
            if (this.firstNodeAddr == null) {
                this.firstNodeAddr = tmpNodeAddrInfo;
            }
        }
        nodeHostPortList = new ArrayList<String>(addrMap4Failover.size());
        for (String hostPortStr : addrMap4Failover.keySet()) {
            nodeHostPortList.add(hostPortStr);
        }
        int count = 0;
        Collections.sort(nodeHostPortList);
        StringBuilder builder = new StringBuilder(256);
        for (String nodeStr : nodeHostPortList) {
            if (count++ > 0) {
                builder.append(TokenConstants.ARRAY_SEP);
            }
            builder.append(nodeStr);
        }
        this.masterClusterStr = builder.toString();
    }

    private MasterInfo(Map<String, NodeAddrInfo> addressMap4Failover,
                       NodeAddrInfo firstNodeAddr, String masterClusterStr) {
        for (Map.Entry<String, NodeAddrInfo> entry : addressMap4Failover.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey()) || entry.getValue() == null) {
                continue;
            }
            this.addrMap4Failover.put(entry.getKey(), entry.getValue());
        }
        this.firstNodeAddr = firstNodeAddr;
        this.masterClusterStr = masterClusterStr;
    }

    public Map<String, NodeAddrInfo> getAddrMap4failover() {
        return addrMap4Failover;
    }

    public String getMasterClusterStr() {
        return masterClusterStr;
    }

    public List<String> getNodeHostPortList() {
        return nodeHostPortList;
    }

    @Override
    public String toString() {
        return firstNodeAddr.getHostPortStr();
    }

    @Override
    public int hashCode() {
        return this.masterClusterStr.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (!(o instanceof MasterInfo)) {
            return false;
        }
        MasterInfo other = (MasterInfo) o;
        if (this.addrMap4Failover.size() != other.addrMap4Failover.size()) {
            return false;
        }
        for (String address : this.addrMap4Failover.keySet()) {
            if (other.addrMap4Failover.get(address) == null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public MasterInfo clone() {
        return new MasterInfo(this.addrMap4Failover, this.firstNodeAddr, this.masterClusterStr);
    }
}
