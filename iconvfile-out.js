module.exports = function (RED) {
    "use strict";
    var fs = require("fs-extra");
    var os = require("os");
    var path = require("path");
    var iconv = require('iconv-lite');

    function IconvfileOutNode(n) {
        RED.nodes.createNode(this, n);
        this.filename = n.filename;
        this.appendNewline = n.appendNewline;
        this.overwriteFile = n.overwriteFile.toString();
        this.createDir = n.createDir || false;
        var node = this;
        node.wstream = null;
        node.msgQueue = [];
        node.closing = false;
        node.closeCallback = null;
        node.charset = n.charset;

        function processMsg(msg, done) {
            var filename = node.filename || msg.filename || "";
            if ((!node.filename) && (!node.tout)) {
                node.tout = setTimeout(function () {
                    node.status({
                        fill: "grey",
                        shape: "dot",
                        text: filename
                    });
                    clearTimeout(node.tout);
                    node.tout = null;
                }, 333);
            }
            if (filename === "") {
                node.warn(RED._("iconvfile-out.errors.nofilename"));
                done();
            } else if (node.overwriteFile === "delete") {
                fs.unlink(filename, function (err) {
                    if (err) {
                        node.error(RED._("iconvfile-out.errors.deletefail", {
                            error: err.toString()
                        }), msg);
                    } else {
                        if (RED.settings.verbose) {
                            node.log(RED._("iconvfile-out.status.deletedfile", {
                                file: filename
                            }));
                        }
                        node.send(msg);
                    }
                    done();
                });
            } else if (msg.hasOwnProperty("payload") && (typeof msg.payload !== "undefined")) {
                if (!iconv.encodingExists(node.charset)) {
                    node.error(RED._("iconvfile-out.errors.nocharset") + node.charset, msg);
                    done();
                    return;
                }
                var dir = path.dirname(filename);
                if (node.createDir) {
                    try {
                        fs.ensureDirSync(dir);
                    } catch (err) {
                        node.error(RED._("iconvfile-out.errors.createfail", {
                            error: err.toString()
                        }), msg);
                        done();
                        return;
                    }
                }

                var data = msg.payload;
                if ((typeof data === "object") && (!Buffer.isBuffer(data))) {
                    data = JSON.stringify(data);
                }
                if (typeof data === "boolean") {
                    data = data.toString();
                }
                if (typeof data === "number") {
                    data = data.toString();
                }
                if ((node.appendNewline) && (!Buffer.isBuffer(data))) {
                    data += os.EOL;
                }
                if (typeof data === 'string' && (!Buffer.isBuffer(data))) {
                    data = iconv.encode(data, node.charset);
                }
                if (node.overwriteFile === "true") {
                    var wstream = fs.createWriteStream(filename, {
                        encoding: 'binary',
                        flags: 'w',
                        autoClose: true
                    });
                    node.wstream = wstream;
                    wstream.on("error", function (err) {
                        node.error(RED._("iconvfile-out.errors.writefail", {
                            error: err.toString()
                        }), msg);
                        done();
                    });
                    wstream.on("open", function () {
                        wstream.end(data, function () {
                            node.send(msg);
                            done();
                        });
                    })
                    return;
                } else {
                    // Append mode
                    var recreateStream = !node.wstream || !node.filename;
                    if (node.wstream && node.wstreamIno) {
                        // There is already a stream open and we have the inode
                        // of the file. Check the file hasn't been deleted
                        // or deleted and recreated.
                        try {
                            var stat = fs.statSync(filename);
                            // File exists - check the inode matches
                            if (stat.ino !== node.wstreamIno) {
                                // The file has been recreated. Close the current
                                // stream and recreate it
                                recreateStream = true;
                                node.wstream.end();
                                delete node.wstream;
                                delete node.wstreamIno;
                            }
                        } catch (err) {
                            // File does not exist
                            recreateStream = true;
                            node.wstream.end();
                            delete node.wstream;
                            delete node.wstreamIno;
                        }
                    }
                    if (recreateStream) {
                        node.wstream = fs.createWriteStream(filename, {
                            encoding: 'binary',
                            flags: 'a',
                            autoClose: true
                        });
                        node.wstream.on("open", function (fd) {
                            try {
                                var stat = fs.statSync(filename);
                                node.wstreamIno = stat.ino;
                            } catch (err) {}
                        });
                        node.wstream.on("error", function (err) {
                            node.error(RED._("iconvfile-out.errors.appendfail", {
                                error: err.toString()
                            }), msg);
                            done();
                        });
                    }
                    if (node.filename) {
                        // Static filename - write and reuse the stream next time
                        node.wstream.write(data, function () {
                            node.send(msg);
                            done();
                        });
                    } else {
                        // Dynamic filename - write and close the stream
                        node.wstream.end(data, function () {
                            node.send(msg);
                            delete node.wstream;
                            delete node.wstreamIno;
                            done();
                        });
                    }
                }
            } else {
                done();
            }
        }

        function processQ(queue) {
            var msg = queue[0];
            processMsg(msg, function () {
                queue.shift();
                if (queue.length > 0) {
                    processQ(queue);
                } else if (node.closing) {
                    closeNode();
                }
            });
        }

        this.on("input", function (msg) {
            var msgQueue = node.msgQueue;
            if (msgQueue.push(msg) > 1) {
                // pending write exists
                return;
            }
            try {
                processQ(msgQueue);
            } catch (e) {
                node.msgQueue = [];
                if (node.closing) {
                    closeNode();
                }
                throw e;
            }
        });

        function closeNode() {
            if (node.wstream) {
                node.wstream.end();
            }
            if (node.tout) {
                clearTimeout(node.tout);
            }
            node.status({});
            var cb = node.closeCallback;
            node.closeCallback = null;
            node.closing = false;
            if (cb) {
                cb();
            }
        }

        this.on('close', function (done) {
            if (node.closing) {
                // already closing
                return;
            }
            node.closing = true;
            if (done) {
                node.closeCallback = done;
            }
            if (node.msgQueue.length > 0) {
                // close after queue processed
                return;
            } else {
                closeNode();
            }
        });
    }
    RED.nodes.registerType("iconvfile-out", IconvfileOutNode);
}
