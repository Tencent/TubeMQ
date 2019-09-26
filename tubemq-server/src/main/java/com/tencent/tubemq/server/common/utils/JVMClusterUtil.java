/**
 * Tencent is pleased to support the open source community by making TubeMQ available.
 * <p>
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tubemq.server.common.utils;

import com.tencent.tubemq.corebase.utils.ThreadUtils;
import com.tencent.tubemq.server.broker.BrokerConfig;
import com.tencent.tubemq.server.broker.TubeBroker;
import com.tencent.tubemq.server.master.MasterConfig;
import com.tencent.tubemq.server.master.TMaster;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility used running a cluster all in the one JVM.
 */
public class JVMClusterUtil {
    private static final Logger logger = LoggerFactory.getLogger(JVMClusterUtil.class);

    public static JVMClusterUtil.BrokerThread createTubeBrokerThread(final BrokerConfig config,
                                                                     final Class<? extends TubeBroker> tbsc,
                                                                     final int index) throws IOException {
        TubeBroker server;
        try {
            server = tbsc.getConstructor(BrokerConfig.class).newInstance(config);
        } catch (InvocationTargetException ite) {
            Throwable target = ite.getTargetException();
            throw new RuntimeException("Failed construction of TubeBroker: " + tbsc.toString()
                    + ((target.getCause() != null) ? target.getCause().getMessage() : ""), target);
        } catch (Exception e) {
            IOException ioe = new IOException();
            ioe.initCause(e);
            throw ioe;
        }
        return new JVMClusterUtil.BrokerThread(server, index);
    }

    public static JVMClusterUtil.MasterThread createMasterThread(final MasterConfig masterConfig,
                                                                 final Class<? extends TMaster> tmc,
                                                                 final int index) throws IOException {
        TMaster server;
        try {
            server = tmc.getConstructor(MasterConfig.class).newInstance(masterConfig);
        } catch (InvocationTargetException ite) {
            Throwable target = ite.getTargetException();
            throw new RuntimeException("Failed construction of Master: " + tmc.toString()
                    + ((target.getCause() != null) ? target.getCause().getMessage() : ""), target);
        } catch (Exception e) {
            IOException ioe = new IOException();
            ioe.initCause(e);
            throw ioe;
        }
        return new JVMClusterUtil.MasterThread(server, index);
    }

    /**
     * Start the cluster. Waits until there is a primary master initialized and returns its
     * address.
     */
    public static void startup(final List<MasterThread> masters,
                               final List<BrokerThread> regionservers) throws IOException {

        if (masters == null || masters.isEmpty()) {
            return;
        }

        for (JVMClusterUtil.MasterThread t : masters) {
            t.start();
        }

        if (regionservers != null) {
            for (JVMClusterUtil.BrokerThread t : regionservers) {
                t.start();
            }
        }
    }

    /**
     * @param masters
     * @param brokerServers
     */
    public static void shutdown(final List<MasterThread> masters,
                                final List<BrokerThread> brokerServers) {
        if (logger.isDebugEnabled()) {
            logger.debug("Shutting down Tube Cluster");
        }
        // regionServerThreads can never be null because they are initialized when
        // the class is constructed.
        if (brokerServers != null) {
            for (BrokerThread t : brokerServers) {
                if (t.isAlive()) {
                    try {
                        t.getTubeBroker().stop("Shutdown requested");
                        t.join();
                    } catch (InterruptedException e) {
                        // continue
                    }
                }
            }
        }
        if (masters != null) {
            // Do backups first.
            for (JVMClusterUtil.MasterThread t : masters) {
                t.master.stop();
            }
        }
        if (masters != null) {
            for (JVMClusterUtil.MasterThread t : masters) {
                while (t.master.isAlive()) {
                    try {
                        // The below has been replaced to debug sometime hangs on end of
                        // tests.
                        // this.master.join():
                        ThreadUtils.threadDumpingIsAlive(t.master.getThread());
                    } catch (InterruptedException e) {
                        // continue
                    }
                }
            }
        }
        logger.info("Shutdown of " + ((masters != null) ? masters.size() : "0") + " master(s) and "
                + ((brokerServers != null) ? brokerServers.size() : "0") + " broker(s) complete");
    }

    /**
     * Data structure to hold TubeBroker Thread and TubeBroker instance
     */
    public static class BrokerThread extends Thread {
        private final TubeBroker tubeBroker;

        public BrokerThread(final TubeBroker t, final int index) {
            super(t, "TubeBroker:" + index);
            this.tubeBroker = t;
        }

        /**
         * @return the tube broker
         */
        public TubeBroker getTubeBroker() {
            return this.tubeBroker;
        }

        /**
         * Block until the tube broker has come online, indicating it is ready to be used.
         */
        public void waitForServerOnline() {
            tubeBroker.waitForServerOnline();
        }
    }

    /**
     * Data structure to hold Master Thread and Master instance
     */
    public static class MasterThread extends Thread {
        private final TMaster master;

        public MasterThread(final TMaster m, final int index) {
            super(m, "Master:" + index);
            this.master = m;
        }

        /**
         * @return the master
         */
        public TMaster getMaster() {
            return this.master;
        }
    }
}
