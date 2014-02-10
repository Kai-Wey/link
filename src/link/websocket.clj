(ns link.websocket
  (:refer-clojure :exclude [send])
  (:use [link tcp util])
  (:import [io.netty.buffer ByteBuf Unpooled])
  (:import [io.netty.handler.codec.http
            HttpResponseEncoder
            HttpRequestDecoder
            HttpObjectAggregator])
  (:import [io.netty.handler.codec.http.websocketx
            WebSocketServerProtocolHandler
            TextWebSocketFrame
            BinaryWebSocketFrame
            PingWebSocketFrame
            PongWebSocketFrame])
  (:import [io.netty.channel
            ChannelHandlerContext
            SimpleChannelInboundHandler]))

(defn websocket-codecs
  ([path] (websocket-codecs path nil))
  ([path subprotocols]
     ;; web socket handler is of course stateful
     [(fn [] (HttpRequestDecoder.))
      (fn [] (HttpObjectAggregator. 65536))
      (fn [] (HttpResponseEncoder.))
      (fn [] (WebSocketServerProtocolHandler. path subprotocols))]))

(defn text [^String s]
  (TextWebSocketFrame. s))

(defn binary [^ByteBuf bytes]
  (BinaryWebSocketFrame. bytes))

(defn ping
  ([] (ping (Unpooled/buffer 0)))
  ([^ByteBuf payload] (PingWebSocketFrame. payload)))

(defn pong
  ([] (pong (Unpooled/buffer 0)))
  ([^ByteBuf payload] (PongWebSocketFrame. payload)))

;; make message receive handler
(make-handler-macro text)
(make-handler-macro binary)
(make-handler-macro ping)
(make-handler-macro pong)
(make-handler-macro error)
(make-handler-macro open)
(make-handler-macro close)

(defmacro create-handler1 [sharable & body]
  `(let [handlers# (merge ~@body)]
     (proxy [SimpleChannelInboundHandler] []
       (isSharable [] ~sharable)
       (channelActive [^ChannelHandlerContext ctx#]
         (when-let [handler# (:on-open handlers#)]
           (handler# (.channel ctx#)))
         (.fireChannelActive ctx#))

       (channelInactive [^ChannelHandlerContext ctx#]
         (when-let [handler# (:on-close handlers#)]
           (handler# (.channel ctx#)))
         (.fireChannelInactive ctx#))

       (exceptionCaught [^ChannelHandlerContext ctx#
                         ^Throwable e#]
         (if-let [handler# (:on-error handlers#)]
           (handler# (.channel ctx#) e#)
           (.fireExceptionCaught  ctx# e#)))

       (channelRead0 [^ChannelHandlerContext ctx# msg#]
         (let [ch# (.channel ctx#)]
           (cond
            (and (instance? TextWebSocketFrame msg#) (:on-text handlers#))
            ((:on-text handlers#) ch# (.text ^TextWebSocketFrame msg#))

            (and (instance? BinaryWebSocketFrame msg#) (:on-binary handlers#))
            ((:on-binary handlers#) ch# (.content ^BinaryWebSocketFrame msg#))

            (and (instance? PingWebSocketFrame msg#) (:on-ping handlers#))
            ((:on-ping handlers#) ch# (.content ^PingWebSocketFrame msg#))

            (and (instance? PongWebSocketFrame msg#) (:on-pong handlers#))
            ((:on-pong handlers#) ch# (.content ^PongWebSocketFrame msg#))

            :else (.fireChannelRead ctx# msg#)))))))

(defmacro create-websocket-handler [& body]
  `(create-handler1 true ~@body))

(defmacro create-stateful-websocket-handler [& body]
  `(fn [] (create-handler1 false ~@body)))
