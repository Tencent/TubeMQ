/*
 * Tencent is pleased to support the open source community by making TubeMQ available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tubemq.corerpc.netty;

import static org.jboss.netty.channel.Channels.fireExceptionCaught;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.LifeCycleAwareChannelHandler;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.util.ExternalResourceReleasable;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;


public class ReadTimeoutHandler extends SimpleChannelUpstreamHandler implements
        LifeCycleAwareChannelHandler, ExternalResourceReleasable {

    private final Timer timer;
    private final long timeoutMillis;

    /**
     * Creates a new instance.
     *
     * @param timer          the {@link Timer} that is used to trigger the scheduled event. The
     *                       recommended {@link Timer} implementation is {@link
     *                       org.jboss.netty.util.HashedWheelTimer}.
     * @param timeoutSeconds read timeout in seconds
     */
    public ReadTimeoutHandler(Timer timer, int timeoutSeconds) {
        this(timer, timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Creates a new instance.
     *
     * @param timer   the {@link Timer} that is used to trigger the scheduled event. The recommended
     *                {@link Timer} implementation is {@link org.jboss.netty.util.HashedWheelTimer}.
     * @param timeout read timeout
     * @param unit    the {@link TimeUnit} of {@code timeout}
     */
    public ReadTimeoutHandler(Timer timer, long timeout, TimeUnit unit) {
        if (timer == null) {
            throw new NullPointerException("timer");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        this.timer = timer;
        if (timeout <= 0) {
            timeoutMillis = 0;
        } else {
            timeoutMillis = Math.max(unit.toMillis(timeout), 1);
        }
    }

    private static void destroy(ChannelHandlerContext ctx) {
        State state = state(ctx);
        synchronized (state) {
            if (state.state != 1) {
                return;
            }
            state.state = 2;
        }

        if (state.timeout != null) {
            state.timeout.cancel();
            state.timeout = null;
        }
    }

    private static State state(ChannelHandlerContext ctx) {
        State state;
        synchronized (ctx) {
            // TODO: It could have been better if there is setAttachmentIfAbsent().
            state = (State) ctx.getAttachment();
            if (state != null) {
                return state;
            }
            state = new State();
            ctx.setAttachment(state);
        }
        return state;
    }

    /**
     * Stops the {@link Timer} which was specified in the constructor of this handler. You should
     * not call this method if the {@link Timer} is in use by other objects.
     */
    public void releaseExternalResources() {
        if (timer != null) {
            timer.stop();
        }
    }

    public void beforeAdd(ChannelHandlerContext ctx) throws Exception {
        if (ctx.getPipeline().isAttached()) {
            // channelOpen event has been fired already, which means
            // this.channelOpen() will not be invoked.
            // We have to initialize here instead.
            initialize(ctx);
        } else {
            // channelOpen event has not been fired yet.
            // this.channelOpen() will be invoked and initialization will occur there.
        }
    }

    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        // NOOP
    }

    public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
        destroy(ctx);
    }

    public void afterRemove(ChannelHandlerContext ctx) throws Exception {
        // NOOP
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // This method will be invoked only if this handler was added
        // before channelOpen event is fired. If a user adds this handler
        // after the channelOpen event, initialize() will be called by beforeAdd().
        initialize(ctx);
        ctx.sendUpstream(e);
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        destroy(ctx);
        ctx.sendUpstream(e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        State state = (State) ctx.getAttachment();
        state.lastReadTime = System.currentTimeMillis();
        ctx.sendUpstream(e);
    }

    private void initialize(ChannelHandlerContext ctx) {
        State state = state(ctx);

        // Avoid the case where destroy() is called before scheduling timeouts.
        // See: https://github.com/netty/netty/issues/143
        synchronized (state) {
            switch (state.state) {
                case 1:
                case 2:
                    return;
            }
            state.state = 1;
        }

        if (timeoutMillis > 0) {
            state.timeout =
                    timer.newTimeout(new ReadTimeoutTask(ctx), timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    protected void readTimedOut(ChannelHandlerContext ctx) throws Exception {
        fireExceptionCaught(ctx, new ReadTimeoutException());
    }

    private static final class State {
        // 0 - none, 1 - initialized, 2 - destroyed
        int state;
        volatile Timeout timeout;
        volatile long lastReadTime = System.currentTimeMillis();

        State() {
        }
    }

    private final class ReadTimeoutTask implements TimerTask {

        private final ChannelHandlerContext ctx;

        ReadTimeoutTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }

            if (!ctx.getChannel().isOpen()) {
                return;
            }

            State state = (State) ctx.getAttachment();
            long currentTime = System.currentTimeMillis();
            long nextDelay = timeoutMillis - (currentTime - state.lastReadTime);
            if (nextDelay <= 0) {
                // Read timed out - set a new timeout and notify the callback.
                state.timeout = timer.newTimeout(this, timeoutMillis, TimeUnit.MILLISECONDS);
                fireReadTimedOut(ctx);
            } else {
                // Read occurred before the timeout - set a new timeout with shorter delay.
                state.timeout = timer.newTimeout(this, nextDelay, TimeUnit.MILLISECONDS);
            }
        }

        private void fireReadTimedOut(final ChannelHandlerContext ctx) throws Exception {
            ctx.getPipeline().execute(new Runnable() {

                public void run() {
                    try {
                        readTimedOut(ctx);
                    } catch (Throwable t) {
                        fireExceptionCaught(ctx, t);
                    }
                }
            });
        }
    }
}
