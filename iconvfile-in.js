module.exports = function (RED) {
    "use strict";
    var fs = require("fs-extra");
    var os = require("os");
    var path = require("path");
    var iconv = require('iconv-lite');

    function IconvfileInNode(n) {
        RED.nodes.createNode(this, n);
        this.filename = n.filename;
        this.format = n.format;
        this.chunk = false;
        if (n.sendError === undefined) {
            this.sendError = true;
        } else {
            this.sendError = n.sendError;
        }
        if (this.format === "lines") {
            this.chunk = true;
        }
        if (this.format === "stream") {
            this.chunk = true;
        }
        if (n.charset === '') {
            this.charset = 'utf8';
        } else {
            this.charset = n.charset;
        }
        var node = this;

        this.on("input", function (msg) {
            var filename = node.filename || msg.filename || "";
            if (!node.filename) {
                node.status({
                    fill: "grey",
                    shape: "dot",
                    text: filename
                });
            }
            if (filename === "") {
                node.warn(RED._("iconvfile-in.errors.nofilename"));
            } else if (!iconv.encodingExists(node.charset)) {
                node.error(RED._("iconvfile-in.errors.nocharset") + node.charset, msg);
            }
            else {
                msg.filename = filename;
                var lines = Buffer.from([]);
                var spare = "";
                var count = 0;
                var type = "buffer";
                var ch = "";
                if (node.format === "lines") {
                    ch = "\n";
                    type = "string";
                }
                var hwm;
                var getout = false;

                var rs = fs.createReadStream(filename)
                    .on('readable', function () {
                        var chunk;
                        var hwm = rs._readableState.highWaterMark;
                        while (null !== (chunk = rs.read())) {
                            if (node.chunk === true) {
                                getout = true;
                                if (node.format === "lines") {
                                    spare += iconv.decode(chunk, node.charset);
                                    var bits = spare.split("\n");
                                    for (var i = 0; i < bits.length - 1; i++) {
                                        var m = {
                                            payload: bits[i],
                                            topic: msg.topic,
                                            filename: msg.filename,
                                            parts: {
                                                index: count,
                                                ch: ch,
                                                type: type,
                                                id: msg._msgid
                                            }
                                        }
                                        count += 1;
                                        node.send(m);
                                    }
                                    spare = bits[i];
                                }
                                if (node.format === "stream") {
                                    var m = {
                                        payload: chunk,
                                        topic: msg.topic,
                                        filename: msg.filename,
                                        parts: {
                                            index: count,
                                            ch: ch,
                                            type: type,
                                            id: msg._msgid
                                        }
                                    }
                                    count += 1;
                                    if (chunk.length < hwm) { // last chunk is smaller that high water mark = eof
                                        getout = false;
                                        m.parts.count = count;
                                    }
                                    node.send(m);
                                }
                            } else {
                                lines = Buffer.concat([lines, chunk]);
                            }
                        }
                    })
                    .on('error', function (err) {
                        node.error(err, msg);
                        if (node.sendError) {
                            var sendMessage = RED.util.cloneMessage(msg);
                            delete sendMessage.payload;
                            sendMessage.error = err;
                            node.send(sendMessage);
                        }
                    })
                    .on('end', function () {
                        if (node.chunk === false) {
                            if (node.format === "string") {
                                msg.payload = iconv.decode(lines, node.charset);
                            } else {
                                msg.payload = lines;
                            }
                            node.send(msg);
                        } else if (node.format === "lines") {
                            var m = {
                                payload: spare,
                                parts: {
                                    index: count,
                                    count: count + 1,
                                    ch: ch,
                                    type: type,
                                    id: msg._msgid
                                }
                            };
                            node.send(m);
                        } else if (getout) { // last chunk same size as high water mark - have to send empty extra packet.
                            var m = {
                                parts: {
                                    index: count,
                                    count: count,
                                    ch: ch,
                                    type: type,
                                    id: msg._msgid
                                }
                            };
                            node.send(m);
                        }
                    });
            }
        });
        this.on('close', function () {
            node.status({});
        });
    }
    RED.nodes.registerType("iconvfile-in", IconvfileInNode);
}
